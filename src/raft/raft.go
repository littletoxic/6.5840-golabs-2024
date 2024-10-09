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
	serverCount       int
	currentState      ServerState
	receive           atomic.Bool
	requestVoteChan   chan *requestVoteRequest
	voteTimerChan     chan int
	AppendEntriesChan chan *AppendEntriesRequest
	taskChannels      []chan int
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

type AERChannelWithMutex struct {
	ch     chan *AppendEntriesReply
	closed bool
	mu     sync.Mutex
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
	notifyChan := make(chan int)
	rf.requestVoteChan <- &requestVoteRequest{args, notifyChan, reply}
	<-notifyChan
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
	notifyChan := make(chan int)
	rf.AppendEntriesChan <- &AppendEntriesRequest{args, notifyChan, reply}
	<-notifyChan
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
		if rf.currentState != Leader && !rf.receive.Swap(false) {
			rf.voteTimerChan <- 0
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 300 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	rf.voteTimerChan <- 0
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
	rf.receive.Store(true)
	rf.requestVoteChan = make(chan *requestVoteRequest)
	rf.voteTimerChan = make(chan int)
	rf.AppendEntriesChan = make(chan *AppendEntriesRequest)

	go rf.mainLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) mainLoop() {
	for rf.killed() == false {

		select {

		// Respond to RPCs from candidates and leaders
		case requestVoteRequest := <-rf.requestVoteChan:
			rf.requestVoteRequestHandler(requestVoteRequest)

		// Respond to RPCs from candidates and leaders
		case appendEntriesRequest := <-rf.AppendEntriesChan:
			rf.appendEntriesRequestHandler(appendEntriesRequest)

		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate
		case <-rf.voteTimerChan:
			// On conversion to candidate, start election:
			//	• Increment currentTerm
			//	• Vote for self
			//	• Reset election timer
			rf.mu.Lock()
			if !rf.killed() && rf.currentState == Candidate || rf.currentState == Follower {
				rf.currentState = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				savedTerm := rf.currentTerm
				rf.receive.Store(true)

				go rf.sendRequestVoteToAll(savedTerm)
			}
			rf.mu.Unlock()
		}

	}

	// clean up
	// 可能不完全，确保完全清理可以给 channel 加锁
	for {
		finish := false
		select {

		case requestVoteRequest := <-rf.requestVoteChan:
			requestVoteRequest.notifyChan <- 1

		case appendEntriesRequest := <-rf.AppendEntriesChan:
			appendEntriesRequest.notifyChan <- 1

		default:
			finish = true
		}

		if finish {
			break
		}
	}
}

func (rf *Raft) appendEntriesRequestHandler(appendEntriesRequest *AppendEntriesRequest) {
	appendEntriesArgs := appendEntriesRequest.args
	notifyChan := appendEntriesRequest.notifyChan
	appendEntriesReply := appendEntriesRequest.reply

	if appendEntriesArgs.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		appendEntriesReply.Success = false
	} else {
		// 一个 term 只有一个 Leader
		rf.receive.Store(true)
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.mu.Lock()
		if appendEntriesArgs.Term > rf.currentTerm {
			rf.currentTerm = appendEntriesArgs.Term
			// 该 term 有 Leader 了
			// rf.votedFor = -1
			rf.currentState = Follower
		}

		// If AppendEntries RPC received from new leader: convert to
		// follower
		if rf.currentState == Candidate {
			rf.currentState = Follower
		}

		if appendEntriesArgs.PrevLogIndex >= len(rf.log) || rf.log[appendEntriesArgs.PrevLogIndex].Term != appendEntriesArgs.PrevLogTerm {
			// Reply false if log doesn’t contain an entry at prevLogIndex
			// whose term matches prevLogTerm (§5.3)
			appendEntriesReply.Success = false
		} else {
			// If an existing entry conflicts with a new one (same index
			// but different terms), delete the existing entry and all that
			// follow it (§5.3)
			// Append any new entries not already in the log
			if len(appendEntriesArgs.Entries) != 0 {

				rf.log = append(rf.log[:appendEntriesArgs.PrevLogIndex+1], appendEntriesArgs.Entries...)

				// todo: persistence
			}
			// If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)
			if appendEntriesArgs.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(appendEntriesArgs.LeaderCommit, len(rf.log)-1)
				// todo: commit
			}

		}
		rf.mu.Unlock()
	}

	appendEntriesReply.Term = rf.currentTerm

	notifyChan <- 1
}

func (rf *Raft) requestVoteRequestHandler(requestVoteRequest *requestVoteRequest) {
	requestVoteArgs := requestVoteRequest.args
	notifyChan := requestVoteRequest.notifyChan
	requestVoteReply := requestVoteRequest.reply

	DPrintf("%v: get requestVote from %v\n", rf.me, requestVoteArgs.CandidateId)

	if requestVoteArgs.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		requestVoteReply.VoteGranted = false
	} else {
		// 收到可能的候选人消息
		rf.receive.Store(true)
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.mu.Lock()
		if requestVoteArgs.Term > rf.currentTerm {
			rf.currentTerm = requestVoteArgs.Term
			rf.votedFor = -1
			rf.currentState = Follower
		}

		lastLogIndex := len(rf.log)
		lastLogEntry := rf.log[lastLogIndex-1]
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if (rf.votedFor == -1 || rf.votedFor == requestVoteArgs.CandidateId) && (requestVoteArgs.LastLogTerm > lastLogEntry.Term || requestVoteArgs.LastLogTerm == lastLogEntry.Term && requestVoteArgs.LastLogIndex >= lastLogIndex-1) {
			requestVoteReply.VoteGranted = true
			rf.votedFor = requestVoteArgs.CandidateId
			DPrintf("%v: agree requestVote term: %v id: %v\n", rf.me, requestVoteArgs.Term, requestVoteArgs.CandidateId)

		} else {
			requestVoteReply.VoteGranted = false
		}
		rf.mu.Unlock()
	}

	requestVoteReply.Term = rf.currentTerm

	notifyChan <- 1
}

// 发送 AppendEntries
func (rf *Raft) leaderRoutine(taskChan chan int, server int, forTerm int) {
	DPrintf("%v: leaderRoutine for %v \n", rf.me, server)
	// 多处发送，用锁保护
	ch := &AERChannelWithMutex{ch: make(chan *AppendEntriesReply), closed: false}
	// 只接收不需要锁
	go rf.AppendEntriesReplyHandler(forTerm, ch.ch, server)

	for rf.currentState == Leader && !rf.killed() && forTerm == rf.currentTerm {

		// Upon election: send initial empty AppendEntries RPCs
		// (heartbeat) to each server;

		prevLogIndex := rf.nextIndex[server] - 1
		// 复制了 Log，加锁
		rf.mu.Lock()
		args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, rf.log[prevLogIndex+1:], rf.commitIndex}
		rf.mu.Unlock()
		// 需要检测 channel 是否关闭
		go rf.sendAppendEntriesToServer(ch, server, &args, forTerm)

		<-taskChan
	}
	ch.ch <- &AppendEntriesReply{}

	ch.mu.Lock()
	close(ch.ch)
	ch.closed = true
	ch.mu.Unlock()

	for _ = range taskChan {
	}
}

func (rf *Raft) AppendEntriesReplyHandler(forTerm int, ch chan *AppendEntriesReply, server int) {
	reply := <-ch

	for rf.currentState == Leader && !rf.killed() && forTerm == rf.currentTerm {

		DPrintf("%v: Heartbeat to %v %v\n", rf.me, server, reply.Term != -1)
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = Follower
		}

		rf.mu.Unlock()

		reply = <-ch
	}
	for _ = range ch {
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
		go rf.sendRequestVoteToServer(ch, i, args)
	}

	i := 0
	for ; i < rf.serverCount-1; i++ {
		reply := <-ch
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = Follower
		}
		rf.mu.Unlock()

		if rf.currentTerm > forTerm {
			i++
			break
		}

		if reply.VoteGranted {
			voteCount++
			// If votes received from majority of servers: become leader
			if voteCount > rf.serverCount/2 {
				rf.mu.Lock()
				if forTerm == rf.currentTerm {
					DPrintf("%v: become leader term: %v\n", rf.me, rf.currentTerm)
					rf.currentState = Leader
					rf.initLeaderState()
				}
				rf.mu.Unlock()
				i++
				break
			}
		}
	}
	for ; i < rf.serverCount-1; i++ {
		<-ch
	}
	DPrintf("%v: exit: sendRequestVoteToAll\n", rf.me)
}

// 调用时已经加锁
func (rf *Raft) initLeaderState() {
	rf.nextIndex = make([]int, rf.serverCount)
	rf.matchIndex = make([]int, rf.serverCount)
	rf.taskChannels = make([]chan int, rf.serverCount)
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
		rf.taskChannels[i] = make(chan int)
		go rf.leaderRoutine(rf.taskChannels[i], i, rf.currentTerm)
	}
	go rf.heartbeatTicker(rf.taskChannels, rf.currentTerm)
}

func (rf *Raft) heartbeatTicker(taskChannels []chan int, forTerm int) {
	time.Sleep(100 * time.Millisecond)
	for rf.currentState == Leader && rf.killed() == false && rf.currentTerm == forTerm {

		// repeat during idle periods to
		// prevent election timeouts (§5.2)
		for i := 0; i < rf.serverCount; i++ {
			if i == rf.me {
				continue
			}
			taskChannels[i] <- i
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 让前面创建的 routine 退出
	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		taskChannels[i] <- i
		close(taskChannels[i])
	}

}

func (rf *Raft) sendRequestVoteToServer(ch chan *RequestVoteReply, server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	DPrintf("%v: RequestVote to %v %v %v\n", rf.me, server, ok, reply.VoteGranted)
	if ok {
		ch <- &reply
	} else {
		ch <- &RequestVoteReply{-1, false}
	}
}

func (rf *Raft) sendAppendEntriesToServer(ch *AERChannelWithMutex, server int, args *AppendEntriesArgs, forTerm int) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	DPrintf("%v: AppendEntries to %v %v\n", rf.me, server, ok)
	if !ok {
		reply.Term = -1
		reply.Success = false
	}

	ch.mu.Lock()
	if !ch.closed {
		ch.ch <- &reply
	}
	ch.mu.Unlock()

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
