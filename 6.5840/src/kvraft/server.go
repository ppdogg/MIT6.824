package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd string // Put/Get/Append
	Key string
	Val string

	ClientId int64
	Seq      int64
}

type OpResult struct {
	res string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state  map[string]string               // store data
	seqCh  map[int64]map[int]chan OpResult // clientId -> index -> chan
	maxSeq map[int64]int64                 // clientId -> seq

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	// if key not exist, maxseq=0
	if _, exist := kv.seqCh[args.ClientId]; !exist {
		kv.seqCh[args.ClientId] = make(map[int]chan OpResult)
	}
	kv.mu.Unlock()

	index, _, isleader := kv.rf.Start(Op{
		Cmd: "Get",
		Key: args.Key,

		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyChan(args.ClientId, index)
	select {
	case opRes := <-ch:
		reply.Err = OK
		reply.Value = opRes.res
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
		reply.Value = ""
		go func() {
			<-ch
			kv.delNotifyChan(args.ClientId, index)
		}()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	// if key not exist, maxseq=0
	if args.Seq <= kv.maxSeq[args.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// create map for each client
	if _, exist := kv.seqCh[args.ClientId]; !exist {
		kv.seqCh[args.ClientId] = make(map[int]chan OpResult)
	}
	kv.mu.Unlock()

	index, _, isleader := kv.rf.Start(Op{
		Cmd: args.Op,
		Key: args.Key,
		Val: args.Value,

		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyChan(args.ClientId, index)
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
		go func() {
			<-ch
			kv.delNotifyChan(args.ClientId, index)
		}()
	}
}

func (kv *KVServer) getNotifyChan(clientId int64, index int) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// create channel for each index
	if _, exist := kv.seqCh[clientId][index]; !exist {
		kv.seqCh[clientId][index] = make(chan OpResult)
	}

	return kv.seqCh[clientId][index]
}

func (kv *KVServer) delNotifyChan(clientId int64, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.seqCh[clientId][index]; exist {
		close(kv.seqCh[clientId][index])
		delete(kv.seqCh[clientId], index)
	}
}

func (kv *KVServer) saveSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.maxSeq)
	snapshot := w.Bytes()

	kv.rf.Snapshot(index, snapshot)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var state map[string]string
	var maxSeq map[int64]int64

	if d.Decode(&state) != nil || d.Decode(&maxSeq) != nil {
		return
	}

	kv.state = state
	kv.maxSeq = maxSeq
}

func (kv *KVServer) apply() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			opRes := OpResult{res: ""}
			op := applyMsg.Command.(Op)

			if op.Cmd == "Get" {
				opRes.res = kv.state[op.Key]
			} else if op.Seq > kv.maxSeq[op.ClientId] {
				switch op.Cmd {
				case "Put":
					kv.state[op.Key] = op.Val
				case "Append":
					kv.state[op.Key] = kv.state[op.Key] + op.Val
				default:
				}
				kv.mu.Lock()
				kv.maxSeq[op.ClientId] = op.Seq
				kv.mu.Unlock()
			}

			if kv.maxraftstate != -1 && float64(kv.persister.RaftStateSize()) > float64(kv.maxraftstate)*0.9 {
				kv.saveSnapshot(applyMsg.CommandIndex)
			}

			if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
				continue
			}

			ch := kv.getNotifyChan(op.ClientId, applyMsg.CommandIndex)
			ch <- opRes
			// to-do: delete last index's channel?
		} else if applyMsg.SnapshotValid {
			kv.readSnapshot(applyMsg.Snapshot)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.seqCh = make(map[int64]map[int]chan OpResult)
	kv.maxSeq = make(map[int64]int64)

	kv.persister = persister

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.apply()

	return kv
}
