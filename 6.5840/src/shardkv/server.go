package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd      string //Get Put Append Reconf Handoff HandoffDone
	ClientId int64
	Seq      int64

	// for Put/Append/AddShard/HandoffDone
	Shard int
	// Get/Put/Append
	Key string
	Val string
	// Reconf
	Config shardctrler.Config
	// for both AddShard and HandoffDone
	Num int
	// AddShard
	Data   map[string]string
	MaxSeq map[int64]int64
	// HandoffDone
}

type OpResult struct {
	// Get
	val string
}

type KeyValue map[string]string

type DB struct {
	State map[int]KeyValue // shardId -> key-value pairs
}

func (db *DB) get(key string) string {
	return db.State[key2shard(key)][key]
}

func (db *DB) put(key string, val string) {
	db.State[key2shard(key)][key] = val
}

func (db *DB) append(key string, val string) {
	db.State[key2shard(key)][key] += val
}

type ShardState int

const (
	Idle ShardState = iota
	Push
	Pull
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead      int32 // set by Kill()
	sm        *shardctrler.Clerk
	persister *raft.Persister
	db        DB
	config    shardctrler.Config

	// persist
	curShards [shardctrler.NShards]ShardState      // shard->state
	maxSeq    [shardctrler.NShards]map[int64]int64 // shard -> clientId -> seq
	// volatile
	seqCh map[int64]map[int]chan OpResult // clientId -> index -> chan
	//seqCh4Server map[int]map[int]chan OpResult   // configNum -> shard -> chan
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	// wrong group
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		//fmt.Printf("gid %d: key(%s) shard(%d) Get wrong group \n", kv.gid, args.Key, key2shard(args.Key))
		return
	}
	// shard not ready
	if kv.curShards[key2shard(args.Key)] != Idle {
		kv.mu.Unlock()
		reply.Err = ErrShardNotReady
		//fmt.Printf("Get shard not ready \n")
		return
	}
	kv.mu.Unlock()

	index, _, isleader := kv.rf.Start(Op{
		Cmd:      "Get",
		ClientId: args.ClientId,
		Seq:      args.Seq,

		Shard: key2shard(args.Key),
		Key:   args.Key,
	})
	// if !isleader {
	// 	reply.Err = ErrWrongLeader
	// 	fmt.Printf("Get wrong leader \n")
	// 	return
	// }
	//fmt.Printf("gid %d server %d: clientId(%d) seq(%d) cmd(Get) key(%s)\n", kv.gid, kv.me, args.ClientId, args.Seq, args.Key)

	ch := kv.getNotifyChan(args.ClientId, index)
	select {
	case res := <-ch:
		reply.Err = OK
		reply.Value = res.val
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
		go func() {
			<-ch
		}()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		//fmt.Printf("wrong leader \n")
		return
	}
	// wrong group
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		//fmt.Printf("wrong group \n")
		return
	}
	// shard not ready
	if kv.curShards[key2shard(args.Key)] != Idle {
		kv.mu.Unlock()
		reply.Err = ErrShardNotReady
		//fmt.Printf("Get shard not ready \n")
		return
	}
	// if key not exist, maxseq=0
	if args.Seq <= kv.maxSeq[key2shard(args.Key)][args.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		//fmt.Printf("ok \n")
		return
	}
	kv.mu.Unlock()

	index, _, isleader := kv.rf.Start(Op{
		Cmd:      args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,

		Shard: key2shard(args.Key),
		Key:   args.Key,
		Val:   args.Value,
	})
	// if !isleader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	//fmt.Printf("gid %d server %d putAppend: clientId(%d) seq(%d) cmd(%s) key(%s) val(%s)\n", kv.gid, kv.me, args.ClientId, args.Seq, args.Op, args.Key, args.Value)

	ch := kv.getNotifyChan(args.ClientId, index)
	//fmt.Printf("p1")
	select {
	case <-ch:
		reply.Err = OK
		//fmt.Printf("p2")
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
		//fmt.Printf("p3")
		go func() {
			<-ch
		}()
	}
	//fmt.Printf("p4\n")
}

func (kv *ShardKV) AddShard(args *AddShardArgs, reply *AddShardReply) {
	//fmt.Printf("gid %d server %d: config(%d) shard(%d) cmd(AddShard)\n", kv.gid, kv.me, args.Num, args.Shard)
	// receive shards from other groups
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	// wrong group
	if kv.config.Shards[args.Shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	// config unmatched
	if args.Num != kv.config.Num {
		kv.mu.Unlock()
		reply.Err = ErrConfigUnmatched
		return
	}
	// already receive
	if kv.curShards[args.Shard] != Pull {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	cloneData := make(map[string]string)
	for k, v := range args.Data {
		cloneData[k] = v
	}
	cloneMaxSeq := make(map[int64]int64)
	for k, v := range args.MaxSeq {
		cloneMaxSeq[k] = v
	}

	index, _, isleader := kv.rf.Start(Op{
		Cmd: "AddShard",

		Num:    args.Num,
		Shard:  args.Shard,
		Data:   cloneData,
		MaxSeq: cloneMaxSeq,
	})
	// if !isleader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	ch := kv.getNotifyChan(int64(args.Num), index)
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
		go func() {
			<-ch
		}()
	}
}

func (kv *ShardKV) getNotifyChan(clientId int64, index int) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// create map for each client
	if _, exist := kv.seqCh[clientId]; !exist {
		kv.seqCh[clientId] = make(map[int]chan OpResult)
	}
	// create channel for each index
	if _, exist := kv.seqCh[clientId][index]; !exist {
		kv.seqCh[clientId][index] = make(chan OpResult)
	}

	return kv.seqCh[clientId][index]
}

func (kv *ShardKV) sendShards(gid int, args *AddShardArgs) {
	if servers, ok := kv.config.Groups[gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply AddShardReply
			//fmt.Printf("gid %d server %d push: gid(%d) server(%d) shard(%d)\n", kv.gid, kv.me, gid, si, args.Shard)
			ok := srv.Call("ShardKV.AddShard", args, &reply)
			if ok && reply.Err == OK {
				//fmt.Printf("gid %d server %d handoff done: gid(%d) server(%d) shard(%d)\n", kv.gid, kv.me, gid, si, args.Shard)
				kv.rf.Start(Op{
					Cmd:   "HandoffDone",
					Num:   args.Num,
					Shard: args.Shard,
				})
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				return
			}
			// ... not ok, or ErrWrongLeader
		}
	}
	// ... not ok, or timeout
}

func (kv *ShardKV) saveSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db.State)
	e.Encode(kv.config)
	e.Encode(kv.curShards)
	e.Encode(kv.maxSeq)
	snapshot := w.Bytes()

	kv.rf.Snapshot(index, snapshot)
	// fmt.Printf("gid %d server %d snapshot: index(%d)\n", kv.gid, kv.me, index)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	// fmt.Printf("gid %d server %d: apply snapshot\n", kv.gid, kv.me)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var state map[int]KeyValue
	var config shardctrler.Config
	var curShards [shardctrler.NShards]ShardState
	var maxSeq [shardctrler.NShards]map[int64]int64

	if d.Decode(&state) != nil || d.Decode(&config) != nil || d.Decode(&curShards) != nil || d.Decode(&maxSeq) != nil {
		return
	}

	kv.db.State = state
	kv.config = config
	kv.curShards = curShards
	kv.maxSeq = maxSeq
}

// leader
func (kv *ShardKV) startNoOp() {
	for kv.killed() == false {
		term, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		if kv.rf.GetLastTerm() < term {
			kv.rf.Start(Op{Cmd: "NoOp"})
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// leader
func (kv *ShardKV) startSendShards() {
	// as a client, send shards to other group
	// only push shards to other groups, at the same time receive AddShards rpc
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		for shard, state := range kv.curShards {
			if state == Push {
				cloneData := make(map[string]string)
				for k, v := range kv.db.State[shard] {
					cloneData[k] = v
				}
				cloneMaxSeq := make(map[int64]int64)
				for k, v := range kv.maxSeq[shard] {
					cloneMaxSeq[k] = v
				}
				args := AddShardArgs{
					Num:    kv.config.Num,
					Shard:  shard,
					Data:   cloneData,
					MaxSeq: cloneMaxSeq,
				}
				go kv.sendShards(kv.config.Shards[shard], &args)
			}
		}
		kv.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

// leader
func (kv *ShardKV) updateConfiguartion() {
	// reconf
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		flag := false
		for _, state := range kv.curShards {
			if state != Idle {
				flag = true
				break
			}
		}
		kv.mu.Unlock()
		if !flag {
			newConf := kv.sm.Query(kv.config.Num + 1)
			if newConf.Num == kv.config.Num+1 {
				//fmt.Printf("gid %d server %d get new config: num(%d) group(%v) shard(%v)\n", kv.gid, kv.me, newConf.Num, newConf.Groups, newConf.Shards)
				kv.rf.Start(Op{
					Cmd:    "Reconf",
					Config: newConf,
				})
			}
		}

		time.Sleep(80 * time.Millisecond)
	}
}

// for all
func (kv *ShardKV) apply() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			opRes := OpResult{}
			op := applyMsg.Command.(Op)
			//fmt.Printf("apply op: cmd(%s) index(%d)\n", op.Cmd, applyMsg.CommandIndex)

			if op.Cmd == "Get" || op.Cmd == "Put" || op.Cmd == "Append" {
				if op.Cmd == "Get" {
					opRes.val = kv.db.get(op.Key)
				} else if op.Seq > kv.maxSeq[op.Shard][op.ClientId] {
					switch op.Cmd {
					case "Put":
						kv.db.put(op.Key, op.Val)
					case "Append":
						kv.db.append(op.Key, op.Val)
					default:
					}
					kv.mu.Lock()
					kv.maxSeq[op.Shard][op.ClientId] = op.Seq
					kv.mu.Unlock()
				}

				if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
					continue
				}

				ch := kv.getNotifyChan(op.ClientId, applyMsg.CommandIndex)
				ch <- opRes
			} else {
				switch op.Cmd {
				case "Reconf":
					// if not all idle, continue
					if op.Config.Num == kv.config.Num+1 {
						prevConfig, newConfig := kv.config, op.Config
						kv.mu.Lock()
						for shard := range newConfig.Shards {
							if prevConfig.Shards[shard] == 0 || newConfig.Shards[shard] == 0 {
								// idle
								kv.db.State[shard] = make(KeyValue)
								kv.maxSeq[shard] = make(map[int64]int64)
							} else if prevConfig.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
								// pull
								kv.curShards[shard] = Pull
							} else if prevConfig.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
								// push
								kv.curShards[shard] = Push
							}
						}
						kv.config = newConfig
						kv.mu.Unlock()

						//fmt.Printf("gid %d server %d prev conf: num(%d) group(%v) shard(%v)\n", kv.gid, kv.me, prevConfig.Num, prevConfig.Groups, prevConfig.Shards)
						//fmt.Printf("gid %d server %d cur conf: num(%d) group(%v) shard(%v)\n", kv.gid, kv.me, op.Config.Num, op.Config.Groups, op.Config.Shards)
					}
				case "AddShard":
					//fmt.Printf("gid %d server %d add shard: num(%d) shard(%d)\n", kv.gid, kv.me, op.Num, op.Shard)
					kv.mu.Lock()
					if op.Num == kv.config.Num && kv.curShards[op.Shard] == Pull {
						kv.db.State[op.Shard] = op.Data
						kv.maxSeq[op.Shard] = op.MaxSeq
						kv.curShards[op.Shard] = Idle
					}
					kv.mu.Unlock()

					if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
						continue
					}

					ch := kv.getNotifyChan(int64(op.Num), applyMsg.CommandIndex)
					ch <- OpResult{}
				case "HandoffDone":
					//fmt.Printf("gid %d server %d handoff done: num(%d) shard(%d)\n", kv.gid, kv.me, op.Num, op.Shard)
					kv.mu.Lock()
					if op.Num == kv.config.Num && kv.curShards[op.Shard] == Push {
						delete(kv.db.State, op.Shard)
						kv.curShards[op.Shard] = Idle
					}
					kv.mu.Unlock()
				default:
				}
			}

			if kv.maxraftstate != -1 && float64(kv.persister.RaftStateSize()) > float64(kv.maxraftstate)*0.9 {
				kv.saveSnapshot(applyMsg.CommandIndex)
			}
		}

		if applyMsg.SnapshotValid {
			kv.readSnapshot(applyMsg.Snapshot)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.db = DB{State: make(map[int]KeyValue)}
	kv.config = shardctrler.Config{}

	kv.maxSeq = [shardctrler.NShards]map[int64]int64{}
	kv.curShards = [shardctrler.NShards]ShardState{}

	kv.seqCh = make(map[int64]map[int]chan OpResult)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(kv.persister.ReadSnapshot())

	//fmt.Printf("add server: gid(%d)-server(%d)\n", gid, me)

	go kv.apply()
	go kv.updateConfiguartion()
	go kv.startSendShards()
	go kv.startNoOp()

	return kv
}
