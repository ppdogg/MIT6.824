package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	seqCh  map[int64]map[int]chan OpResult // clientId -> index -> chan
	maxSeq map[int64]int64                 // clientId -> seq

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Cmd      string
	ClientId int64
	Seq      int64

	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
}

type OpResult struct {
	// Query
	config Config
}

func copyConfig(config Config) Config {
	newConfig := Config{
		Num:    config.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	copy(newConfig.Shards[:], config.Shards[:])
	for gid, group := range config.Groups {
		newConfig.Groups[gid] = group
	}

	return newConfig
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	// if key not exist, maxseq=0
	if args.Seq <= sc.maxSeq[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	// create map for each client
	if _, exist := sc.seqCh[args.ClientId]; !exist {
		sc.seqCh[args.ClientId] = make(map[int]chan OpResult)
	}
	sc.mu.Unlock()

	index, _, isleader := sc.rf.Start(Op{
		Cmd:     "Join",
		Servers: args.Servers,

		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !isleader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	ch := sc.getNotifyChan(args.ClientId, index)
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(40 * time.Millisecond):
		reply.Err = Timeout
		go func() {
			<-ch
		}()
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	// if key not exist, maxseq=0
	if args.Seq <= sc.maxSeq[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	// create map for each client
	if _, exist := sc.seqCh[args.ClientId]; !exist {
		sc.seqCh[args.ClientId] = make(map[int]chan OpResult)
	}
	sc.mu.Unlock()

	index, _, isleader := sc.rf.Start(Op{
		Cmd:  "Leave",
		GIDs: args.GIDs,

		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !isleader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	ch := sc.getNotifyChan(args.ClientId, index)
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(40 * time.Millisecond):
		reply.Err = Timeout
		go func() {
			<-ch
		}()
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	// if key not exist, maxseq=0
	if args.Seq <= sc.maxSeq[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	// create map for each client
	if _, exist := sc.seqCh[args.ClientId]; !exist {
		sc.seqCh[args.ClientId] = make(map[int]chan OpResult)
	}
	sc.mu.Unlock()

	index, _, isleader := sc.rf.Start(Op{
		Cmd:   "Move",
		Shard: args.Shard,
		GID:   args.GID,

		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !isleader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	ch := sc.getNotifyChan(args.ClientId, index)
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(40 * time.Millisecond):
		reply.Err = Timeout
		go func() {
			<-ch
		}()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	// create map for each client
	if _, exist := sc.seqCh[args.ClientId]; !exist {
		sc.seqCh[args.ClientId] = make(map[int]chan OpResult)
	}
	sc.mu.Unlock()

	index, _, isleader := sc.rf.Start(Op{
		Cmd: "Query",
		Num: args.Num,

		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !isleader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	ch := sc.getNotifyChan(args.ClientId, index)
	select {
	case res := <-ch:
		reply.Err = OK
		reply.Config = res.config
	case <-time.After(40 * time.Millisecond):
		reply.Err = Timeout
		go func() {
			<-ch
		}()
	}
}

func (sc *ShardCtrler) getNotifyChan(clientId int64, index int) chan OpResult {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// create channel for each index
	if _, exist := sc.seqCh[clientId][index]; !exist {
		sc.seqCh[clientId][index] = make(chan OpResult)
	}

	return sc.seqCh[clientId][index]
}

func (sc *ShardCtrler) apply() {
	for sc.killed() == false {
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid {
			opRes := OpResult{}
			op := applyMsg.Command.(Op)

			if op.Cmd == "Query" {
				// the shardctrler should reply with the latest configuration.
				if op.Num == -1 || op.Num >= len(sc.configs) {
					opRes.config = sc.configs[len(sc.configs)-1]
				} else {
					opRes.config = sc.configs[op.Num]
				}
			} else if op.Seq > sc.maxSeq[op.ClientId] {
				newConfig := copyConfig(sc.configs[len(sc.configs)-1])
				switch op.Cmd {
				case "Join":
					for gid, servers := range op.Servers {
						newConfig.Groups[gid] = servers
					}

					if len(newConfig.Groups) == 0 {
						break
					}

					gids := sort.IntSlice{}
					for gid := range newConfig.Groups {
						gids = append(gids, gid)
					}
					sort.Sort(gids)

					if len(sc.configs[len(sc.configs)-1].Groups) == 0 {
						for i := range newConfig.Shards {
							newConfig.Shards[i] = gids[0]
						}
					}

					count := make(map[int][]int) // gid -> nums of shard
					for i := range newConfig.Shards {
						gid := newConfig.Shards[i]
						count[gid] = append(count[gid], i)
					}

					for {
						gid1, gid2 := gids[0], gids[0]
						maxCount, minCount := len(count[gids[0]]), len(count[gids[0]])
						for _, gid := range gids {
							num := len(count[gid])
							if num > maxCount {
								maxCount = num
								gid1 = gid
							}
							if num < minCount {
								minCount = num
								gid2 = gid
							}
						}

						if maxCount-minCount <= 1 {
							break
						}

						movedShard := count[gid1][len(count[gid1])-1]
						count[gid1] = count[gid1][:len(count[gid1])-1]

						newConfig.Shards[movedShard] = gid2
						count[gid2] = append(count[gid2], movedShard)
					}
				case "Leave":
					for _, gid := range op.GIDs {
						delete(newConfig.Groups, gid)
					}

					gids := sort.IntSlice{}
					for gid := range newConfig.Groups {
						gids = append(gids, gid)
					}
					if len(gids) == 0 {
						newConfig.Shards = [NShards]int{}
						break
					}
					sort.Sort(gids)

					count := make(map[int][]int) // gid -> nums of shard
					idleShard := make([]int, 0)
					for i := range newConfig.Shards {
						gid := newConfig.Shards[i]
						if _, exist := newConfig.Groups[gid]; !exist {
							idleShard = append(idleShard, i)
						} else {
							count[gid] = append(count[gid], i)
						}
					}

					for i := range idleShard {
						shard := idleShard[i]

						gid := gids[0]
						minCount := len(count[gids[0]])
						for _, g := range gids {
							num := len(count[g])
							if num < minCount {
								minCount = num
								gid = g
							}
						}

						newConfig.Shards[shard] = gid
						count[gid] = append(count[gid], shard)
					}
				case "Move":
					newConfig.Shards[op.Shard] = op.GID
				default:
				}
				sc.configs = append(sc.configs, newConfig)
				sc.mu.Lock()
				sc.maxSeq[op.ClientId] = op.Seq
				sc.mu.Unlock()
			}

			if term, isLeader := sc.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
				continue
			}

			ch := sc.getNotifyChan(op.ClientId, applyMsg.CommandIndex)
			ch <- opRes
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	// The very first configuration should be numbered zero. It should contain no groups
	// all shards should be assigned to GID zero (an invalid GID)
	sc.seqCh = make(map[int64]map[int]chan OpResult)
	sc.maxSeq = make(map[int64]int64)

	go sc.apply()

	return sc
}
