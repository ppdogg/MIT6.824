package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seq      int64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) sendGetRequest(server int, args GetArgs, replyChan chan GetReply) {
	reply := GetReply{}
	if ok := ck.servers[server].Call("KVServer.Get", &args, &reply); ok {
		replyChan <- reply
	} else {
		replyChan <- GetReply{Err: ErrNotOK}
	}
}
func (ck *Clerk) sendPutAppendRequest(server int, args PutAppendArgs, replyChan chan PutAppendReply) {
	reply := PutAppendReply{}
	if ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply); ok {
		replyChan <- reply
	} else {
		replyChan <- PutAppendReply{Err: ErrNotOK}
	}
}
func (ck *Clerk) monitorGetReq(server int, args GetArgs, resChan chan GetReply) {
	replyChan := make(chan GetReply)
	go ck.sendGetRequest(server, args, replyChan)
	var reply GetReply
	select {
	case reply = <-replyChan:
	case <-time.After(50 * time.Millisecond):
		reply = GetReply{Err: ErrTimeout}
	}
	resChan <- reply
}
func (ck *Clerk) monitorPutAppendReq(server int, args PutAppendArgs, resChan chan PutAppendReply) {
	replyChan := make(chan PutAppendReply)
	go ck.sendPutAppendRequest(server, args, replyChan)
	var reply PutAppendReply
	select {
	case reply = <-replyChan:
	case <-time.After(50 * time.Millisecond):
		reply = PutAppendReply{Err: ErrTimeout}
	}
	resChan <- reply
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	seq := atomic.AddInt64(&ck.seq, 1)
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		Seq:      seq,
	}
	reply := GetReply{}
	server := ck.leaderId
	for {
		replyChan := make(chan GetReply)
		go ck.monitorGetReq(server, args, replyChan)
		reply = <-replyChan
		if reply.Err == OK {
			ck.leaderId = server
			break
		}
		server = (server + 1) % len(ck.servers)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := atomic.AddInt64(&ck.seq, 1)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		Seq:      seq,
	}
	server := ck.leaderId
	for {
		replyChan := make(chan PutAppendReply)
		go ck.monitorPutAppendReq(server, args, replyChan)
		reply := <-replyChan
		if reply.Err == OK {
			ck.leaderId = server
			break
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
