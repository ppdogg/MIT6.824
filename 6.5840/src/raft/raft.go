package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type raftState int32

const (
	follower raftState = iota
	candidate
	leader
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	voteFor     int
	log         []LogEntry
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state      raftState
	notifyChan chan struct{}

	applyCh     chan ApplyMsg
	condApplyCh chan []ApplyMsg

	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = rf.state == leader
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(currentTerm int, voteFor int, log []LogEntry, snapshotIndex int, snapshotTerm int, snapshot []byte) {
	// Your code here (2C).
	go func() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(currentTerm)
		e.Encode(voteFor)
		e.Encode(log)
		e.Encode(snapshotIndex)
		e.Encode(snapshotTerm)
		raftstate := w.Bytes()
		rf.persister.Save(raftstate, snapshot)
	}()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var log []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		return
	}

	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.log = log
	rf.snapshotIndex = snapshotIndex
	rf.commitIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
	rf.snapshot = rf.persister.ReadSnapshot()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex || index <= rf.snapshotIndex {
		return
	}

	rf.snapshot = snapshot
	rf.snapshotTerm = rf.log[rf.pos(index)].Term

	newLog := make([]LogEntry, 0)
	for i := rf.pos(index + 1); i <= rf.pos(rf.GetLastIndex()); i++ {
		newLog = append(newLog, rf.log[i])
	}
	rf.log = newLog

	rf.snapshotIndex = index

	rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int
	Data []byte
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}

	rf.log = make([]LogEntry, 0)
	rf.commitIndex = args.LastIncludedIndex

	rf.snapshot = args.Data
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshotIndex = args.LastIncludedIndex

	rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)

	go func() {
		rf.condApplyCh <- []ApplyMsg{{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  rf.snapshotTerm,
			SnapshotIndex: rf.snapshotIndex,
		}}
	}()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state == follower {
			rf.currentTerm = args.Term
			rf.voteFor = -1

			rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
		} else {
			rf.reset2Follower(args.Term)
		}
	}

	if rf.voteFor == -1 {
		// at least up to date
		if args.LastLogTerm > rf.GetLastTerm() || (args.LastLogTerm == rf.GetLastTerm() && args.LastLogIndex >= rf.GetLastIndex()) {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true

			rf.notifyChan <- struct{}{}
			rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
		}
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == follower {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.voteFor = -1

			rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
		}
	} else {
		rf.reset2Follower(args.Term)
	}

	if len(args.Entries) != 0 {
		// consistent match
		if (args.PrevLogIndex == 0 && args.PrevLogTerm == 0) || (rf.snapshotIndex == args.PrevLogIndex && rf.snapshotTerm == args.PrevLogTerm) {
			rf.log = args.Entries[:]
			reply.Success = true

			rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
		} else {
			// match entry(not the last one) with prevLogIndex and preLogTerm
			if rf.GetLastIndex() < args.PrevLogIndex {
				reply.ConflictIndex = rf.GetLastIndex()
				return
			}
			if rf.snapshotIndex >= args.PrevLogIndex {
				reply.ConflictIndex = rf.snapshotIndex + 1
				return
			}
			if rf.log[rf.pos(args.PrevLogIndex)].Index == args.PrevLogIndex && rf.log[rf.pos(args.PrevLogIndex)].Term != args.PrevLogTerm {
				reply.ConflictTerm = rf.log[rf.pos(args.PrevLogIndex)].Term
				// binary search
				pos := sort.Search(len(rf.log), func(i int) bool {
					return rf.log[i].Term >= reply.ConflictTerm
				})
				if rf.log[pos].Term == reply.ConflictTerm {
					reply.ConflictIndex = rf.log[pos].Index
				}
				return
			}

			// contain an entry at prevLogIndex whose term matches prevLogTerm
			matchedPos, entryPos := rf.pos(args.PrevLogIndex+1), 0
			for ; matchedPos < len(rf.log) && entryPos < len(args.Entries); matchedPos, entryPos = matchedPos+1, entryPos+1 {
				if rf.log[matchedPos].Index != args.Entries[entryPos].Index || rf.log[matchedPos].Term != args.Entries[entryPos].Term {
					// exisiting entry conflicts with new one
					rf.log = rf.log[:matchedPos]

					rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
					break
				}
			}
			// append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[entryPos:]...)
			reply.Success = true

			rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
		}
	} else {
		if rf.state == follower {
			// heartbeat from leader
			rf.notifyChan <- struct{}{}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log) < 1 {
			return
		}

		newCommitIndex := int(math.Min(float64(args.LeaderCommit), float64(rf.GetLastIndex())))
		if args.Term != rf.log[rf.pos(newCommitIndex)].Term {
			return
		}

		if newCommitIndex > rf.commitIndex {
			applyMsg := make([]ApplyMsg, 0)
			for pos := rf.pos(rf.commitIndex + 1); pos <= rf.pos(newCommitIndex); pos++ {
				applyMsg = append(applyMsg, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[pos].Command,
					CommandIndex: rf.log[pos].Index,
					CommandTerm:  rf.log[pos].Term,
				})
			}
			rf.commitIndex = newCommitIndex
			go func() {
				rf.condApplyCh <- applyMsg
			}()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, replyChan chan InstallSnapshotReply) {
	reply := &InstallSnapshotReply{}
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); ok {
		replyChan <- *reply
	} else {
		replyChan <- InstallSnapshotReply{Term: 0}
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyChan chan RequestVoteReply) {
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		replyChan <- *reply
	} else {
		replyChan <- RequestVoteReply{Term: 0, VoteGranted: false}
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, replyChan chan AppendEntriesReply) {
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		replyChan <- *reply
	} else {
		replyChan <- AppendEntriesReply{Term: 0, Success: false, ConflictIndex: -1, ConflictTerm: -1}
	}
}

func (rf *Raft) monitorInstallSnaphot(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.snapshot,
	}
	replyChan := make(chan InstallSnapshotReply)
	go rf.sendInstallSnapshot(server, &args, replyChan)
	var reply InstallSnapshotReply
	select {
	case reply = <-replyChan:
	case <-time.After(15 * time.Millisecond):
		// auto exit if timeout
		return
	}

	if rf.state != leader {
		return
	}

	// return to follower
	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.reset2Follower(reply.Term)
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	rf.nextIndex[server] = rf.snapshotIndex + 1
	rf.mu.Unlock()
}
func (rf *Raft) monitorRequestVote(server int, args RequestVoteArgs, voteChan chan RequestVoteReply) {
	replyChan := make(chan RequestVoteReply)
	go rf.sendRequestVote(server, &args, replyChan)
	var reply RequestVoteReply
	select {
	case reply = <-replyChan:
	case <-time.After(150 * time.Millisecond):
		// auto exit if timeout
		voteChan <- RequestVoteReply{Term: 0, VoteGranted: false}
		return
	}
	voteChan <- reply
}
func (rf *Raft) monitorAppendEntries(server int, args *AppendEntriesArgs, replicateChan chan struct{}) {
	replyChan := make(chan AppendEntriesReply)
	go rf.sendAppendEntries(server, args, replyChan)
	var reply AppendEntriesReply
	select {
	case reply = <-replyChan:
	case <-time.After(15 * time.Millisecond):
		// auto exit if timeout
		replicateChan <- struct{}{}
		return
	}

	if rf.state != leader {
		return
	}

	// return to follower
	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.reset2Follower(reply.Term)
		rf.mu.Unlock()

		replicateChan <- struct{}{}
		return
	}

	if reply.Success {
		// update matchIndex and nextIndex
		rf.mu.Lock()
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1 // plus the length of new entries
		rf.mu.Unlock()
	} else {
		// conflict
		matchedTerm := false
		if reply.ConflictTerm != -1 {
			// binary search
			rf.mu.Lock()
			pos := sort.Search(len(rf.log), func(i int) bool {
				return rf.log[i].Term > reply.ConflictTerm
			})
			if pos > 0 && rf.log[pos-1].Term == reply.ConflictTerm {
				matchedTerm = true
				rf.nextIndex[server] = rf.log[pos-1].Index
			}
			rf.mu.Unlock()
		}
		if !matchedTerm {
			rf.mu.Lock()
			xIndex := int(math.Max(1, float64(reply.ConflictIndex)))
			if xIndex <= rf.snapshotIndex {
				go rf.monitorInstallSnaphot(server)
			} else {
				rf.nextIndex[server] = xIndex
			}
			rf.mu.Unlock()
		}
	}

	replicateChan <- struct{}{}
}

func (rf *Raft) pos(index int) int {
	if index <= rf.snapshotIndex {
		// return rf.lastIncludeIndex
		return -1
	}
	return index - rf.snapshotIndex - 1
}
func (rf *Raft) GetLastIndex() int {
	if len(rf.log) == 0 {
		return rf.snapshotIndex
	}
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) GetLastTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}
func (rf *Raft) reset2Follower(term int) {
	rf.currentTerm = term
	rf.voteFor = -1
	rf.state = follower
	go rf.receiveNotify()

	rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
}

// follower
func (rf *Raft) receiveNotify() {
	for rf.killed() == false {
		electionTimeout := time.Duration(600 + (rand.Int63() % 600))
		select {
		case <-rf.notifyChan:
		case <-time.After(electionTimeout * time.Millisecond):
			rf.mu.Lock()
			// change state
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.state = candidate

			rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
			rf.mu.Unlock()
			// start election
			go rf.startElection()
			return
		}
	}
}

// leader
func (rf *Raft) notifyPeers(notifyFrequent time.Duration) {
	for rf.killed() == false {
		if rf.state != leader {
			return
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		heartbeatChan := make(chan AppendEntriesReply, len(rf.peers))
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendAppendEntries(i, &args, heartbeatChan)
			}
		}

		go func() {
			finish := 0
			for finish < len(rf.peers)-1 {
				finish++
				reply := <-heartbeatChan
				if rf.state != leader {
					continue
				}

				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.reset2Follower(reply.Term)
					rf.mu.Unlock()
				}
			}
		}()

		time.Sleep(notifyFrequent * time.Millisecond)
	}
}

// leader
func (rf *Raft) updateCommitIndex(notifyFrequent time.Duration) {
	for rf.killed() == false {
		if rf.state != leader {
			return
		}
		// update commitIndex
		rf.mu.Lock()
		sortIndex := sort.IntSlice{}
		for i := range rf.peers {
			if i != rf.me {
				sortIndex = append(sortIndex, rf.matchIndex[i])
			}
		}
		sort.Sort(sortIndex)
		newCommitIndex := sortIndex[len(rf.peers)/2]

		if newCommitIndex > rf.commitIndex {
			applyMsg := make([]ApplyMsg, 0)
			for pos := rf.pos(rf.commitIndex + 1); pos <= rf.pos(newCommitIndex); pos++ {
				applyMsg = append(applyMsg, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[pos].Command,
					CommandIndex: rf.log[pos].Index,
					CommandTerm:  rf.log[pos].Term,
				})
			}
			rf.commitIndex = newCommitIndex
			go func() {
				rf.condApplyCh <- applyMsg
			}()
		}
		rf.mu.Unlock()

		time.Sleep(notifyFrequent * time.Millisecond)
	}
}

// leader
func (rf *Raft) replicateEntries(notifyFrequent time.Duration) {
	for rf.killed() == false {
		if rf.state != leader {
			return
		}

		args := make([]AppendEntriesArgs, len(rf.peers))
		replicateChan := make(chan struct{}, len(rf.peers))
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.nextIndex[i] <= rf.snapshotIndex {
				go rf.monitorInstallSnaphot(i)
				replicateChan <- struct{}{}
				continue
			}

			rf.mu.Lock()
			// must satisfy: last log index >= nextIndex
			if rf.nextIndex[i] > rf.GetLastIndex() {
				rf.mu.Unlock()
				replicateChan <- struct{}{}
				continue
			}
			args[i] = AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      rf.log[rf.pos(rf.nextIndex[i]):],
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.snapshotIndex,
				PrevLogTerm:  rf.snapshotTerm,
			}
			if rf.nextIndex[i] >= rf.snapshotIndex+2 {
				args[i].PrevLogIndex = rf.log[rf.pos(rf.nextIndex[i]-1)].Index
				args[i].PrevLogTerm = rf.log[rf.pos(rf.nextIndex[i]-1)].Term
			}
			rf.mu.Unlock()

			go rf.monitorAppendEntries(i, &args[i], replicateChan)
		}
		// waiting for all tasks finished (maybe waitGroup?)
		finish := 0
		for finish < len(rf.peers)-1 {
			finish++
			<-replicateChan
		}

		time.Sleep(notifyFrequent * time.Millisecond)
	}
}

// candidate
func (rf *Raft) startElection() {
	electionTimeout := time.Duration(300 + (rand.Int63() % 300))
	localChan := make(chan struct{}, len(rf.peers))
	finishChan := make(chan struct{})

	go func() {
		rf.mu.Lock()
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.GetLastIndex(),
			LastLogTerm:  rf.GetLastTerm(),
		}
		rf.mu.Unlock()
		voteChan := make(chan RequestVoteReply, len(rf.peers)-1)
		for i := range rf.peers {
			if i != rf.me {
				go rf.monitorRequestVote(i, args, voteChan)
			}
		}

		count := 0
		finish := 0
		for finish < len(rf.peers)-1 {
			finish++
			reply := <-voteChan
			if rf.state != candidate {
				localChan <- struct{}{}
				continue
			}
			if reply.Term > rf.currentTerm {
				// return to follower
				rf.mu.Lock()
				rf.reset2Follower(reply.Term)
				rf.mu.Unlock()
				localChan <- struct{}{}
				continue
			}

			if reply.VoteGranted {
				count++
			}
			if count+1 > len(rf.peers)/2 {
				// become leader
				rf.mu.Lock()
				rf.state = leader
				// initialized to leader last log index+1
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.GetLastIndex() + 1
				}
				// initialized to 0
				rf.matchIndex = make([]int, len(rf.peers))
				rf.mu.Unlock()

				go rf.notifyPeers(time.Duration(100))
				go rf.replicateEntries(time.Duration(30))
				go rf.updateCommitIndex(time.Duration(15))
				localChan <- struct{}{}
			}
		}
		finishChan <- struct{}{}
	}()

	select {
	case <-localChan:
		<-finishChan
	case <-time.After(electionTimeout * time.Millisecond):
		// last election failed
		if rf.state != candidate {
			return
		}
		rf.mu.Lock()
		rf.currentTerm++
		rf.voteFor = rf.me

		rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)
		rf.mu.Unlock()
		// restart election
		go rf.startElection()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		isLeader = false
		return index, term, isLeader
	}

	index = rf.GetLastIndex() + 1 // index start from 1
	term = rf.currentTerm
	newEntry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)

	rf.persist(rf.currentTerm, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = follower
	rf.notifyChan = make(chan struct{})

	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.condApplyCh = make(chan []ApplyMsg)

	rf.snapshotIndex = 0
	rf.snapshotTerm = 0
	rf.snapshot = make([]byte, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// receive heartbeat from leader/candidate
	go rf.receiveNotify()
	// applyMsg
	go func() {
		for rf.killed() == false {
			applyMsg := <-rf.condApplyCh
			for _, msg := range applyMsg {
				rf.applyCh <- msg
			}
		}
	}()
	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}
