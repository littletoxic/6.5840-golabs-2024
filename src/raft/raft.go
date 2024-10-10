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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// leader volatile state
	nextIndex  []int
	matchIndex []int

	// other
	serverCount  int
	currentState ServerState
	receive      bool
	sendTasks    []*Shared
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Shared struct {
	mu    sync.Mutex
	cond  *sync.Cond
	ready bool
}

func NewShared() *Shared {
	s := &Shared{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Shared) Wait() {
	s.mu.Lock()
	for !s.ready {
		s.cond.Wait() // 等待条件满足
	}
	s.ready = false
	s.mu.Unlock()
}

func (s *Shared) Notify() {
	s.mu.Lock()
	s.ready = true
	s.cond.Signal() // 通知一个等待的 goroutine
	s.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type requestVoteRequest struct {
	args       *RequestVoteArgs
	notifyChan chan int
	reply      *RequestVoteReply
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	DPrintf("%v %v: get requestVote from %v\n", rf.me, rf.currentTerm, args.CandidateId)
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.VoteGranted = false
	} else {
		// 收到可能的候选人消息
		rf.receive = true

		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.currentState = Follower
		}

		lastLogIndex := len(rf.log)
		lastLogEntry := rf.log[lastLogIndex-1]
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogEntry.Term || args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogIndex-1) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			DPrintf("%v %v: agree requestVote from %v\n", rf.me, rf.currentTerm, args.CandidateId)

		} else {
			reply.VoteGranted = false
		}

	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRequest struct {
	args       *AppendEntriesArgs
	notifyChan chan int
	reply      *AppendEntriesReply
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Success = false
	} else {
		// 一个 term 只有一个 Leader
		rf.receive = true

		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			// 该 term 有 Leader 了，不需要下面一行
			// rf.votedFor = -1
			rf.currentState = Follower
		}

		// If AppendEntries RPC received from new leader: convert to
		// follower
		if rf.currentState == Candidate {
			rf.currentState = Follower
		}

		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Reply false if log doesn’t contain an entry at prevLogIndex
			// whose term matches prevLogTerm (§5.3)
			reply.Success = false
		} else {
			// If an existing entry conflicts with a new one (same index
			// but different terms), delete the existing entry and all that
			// follow it (§5.3)
			// Append any new entries not already in the log
			if len(args.Entries) != 0 {

				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

				// todo: persistence
			}
			// If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				// todo: commit
			}

		}

	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (3B).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.currentState == Leader
	rf.mu.Unlock()

	if isLeader {

	}

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

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		if !rf.killed() && rf.currentState != Leader && !rf.receive {
			rf.currentState = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			savedTerm := rf.currentTerm
			rf.receive = true

			go rf.sendRequestVoteToAll(savedTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 500 + (rand.Int63() % 250)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.log = []LogEntry{{}}

	rf.serverCount = len(peers)
	rf.currentState = Follower
	rf.receive = true

	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		rf.sendTasks[i] = NewShared()
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) sendAppendEntriesRoutine(cond *Shared, server int, forTerm int) {
	DPrintf("%v %v: sendAppendEntriesRoutine for %v \n", rf.me, forTerm, server)

	for rf.currentState == Leader && !rf.killed() && forTerm == rf.currentTerm {

		// Upon election: send initial empty AppendEntries RPCs
		// (heartbeat) to each server;
		prevLogIndex := rf.nextIndex[server] - 1
		// 复制了 Log，加锁
		rf.mu.Lock()
		args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, rf.log[prevLogIndex+1:], rf.commitIndex}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		DPrintf("%v %v: send AppendEntries to %v %v\n", rf.me, args.Term, server, ok)

		if ok {
			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentState = Follower
			}

			rf.mu.Unlock()
		}

		cond.Wait()
	}

}

func (rf *Raft) sendRequestVoteToAll(forTerm int) {

	voteCount := 1

	ch := make(chan *RequestVoteReply)

	rf.mu.Lock()
	logLen := len(rf.log)
	args := &RequestVoteArgs{forTerm, rf.me, logLen - 1, rf.log[logLen-1].Term}
	rf.mu.Unlock()
	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		// 并行发送
		go rf.sendRequestVoteToServer(ch, i, args)
	}

	// 处理所有 RequestVote 的回复
	for i := 0; i < rf.serverCount-1; i++ {
		reply := <-ch
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = Follower
		}
		rf.mu.Unlock()

		if reply.VoteGranted {
			voteCount++
			// If votes received from majority of servers: become leader
			if voteCount == (rf.serverCount/2)+1 {
				rf.mu.Lock()
				if forTerm == rf.currentTerm {
					DPrintf("%v %v: become leader\n", rf.me, rf.currentTerm)
					rf.currentState = Leader
					rf.initLeaderState()
				}
				rf.mu.Unlock()

			}
		}
	}

}

// 调用时已经加锁
func (rf *Raft) initLeaderState() {
	rf.nextIndex = make([]int, rf.serverCount)
	rf.matchIndex = make([]int, rf.serverCount)
	rf.sendTasks = make([]*Shared, rf.serverCount)
	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		// for each server, index of the next log entry
		// to send to that server (initialized to leader
		// last log index + 1)
		rf.nextIndex[i] = len(rf.log)
		// for each server, index of highest log entry
		// known to be replicated on server
		// (initialized to 0, increases monotonically)
		rf.matchIndex[i] = 0
		rf.sendTasks[i] = NewShared()
		go rf.sendAppendEntriesRoutine(rf.sendTasks[i], i, rf.currentTerm)
		// 一直发送空 appendEntries
		go rf.heartbeatTicker(rf.sendTasks[i], rf.currentTerm)
	}
}

func (rf *Raft) heartbeatTicker(cond *Shared, forTerm int) {
	for rf.currentState == Leader && rf.killed() == false && rf.currentTerm == forTerm {

		// repeat during idle periods to
		// prevent election timeouts (§5.2)

		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) sendRequestVoteToServer(ch chan *RequestVoteReply, server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	DPrintf("%v %v: RequestVote to %v %v %v\n", rf.me, args.Term, server, ok, reply.VoteGranted)
	if !ok {
		reply.Term = -1
		reply.VoteGranted = false
	}
	ch <- &reply

}

// 从 Leader 状态转换时调用，认为
func (rf *Raft) cleanup() {

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
