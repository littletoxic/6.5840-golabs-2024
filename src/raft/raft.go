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
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sort"
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
	serverCount      int
	currentState     ServerState
	receive          bool
	sendNotifiers    []*DelayNotifier
	commitChanged    *DelayNotifier
	applyCh          chan ApplyMsg
	snapshotNotifier *DelayNotifier
	routerCount      []int

	// snapshot persistent
	firstIndex       int // 不使用 lastIncludedIndex，现在认为计算更方便
	lastIncludedTerm int
	snapshot         []byte
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

type DelayNotifier struct {
	mu    sync.Mutex
	cond  *sync.Cond
	ready bool
}

func NewDelayNotifier() *DelayNotifier {
	s := &DelayNotifier{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *DelayNotifier) Wait() {
	s.mu.Lock()
	for !s.ready {
		s.cond.Wait() // 等待条件满足
	}
	s.ready = false
	s.mu.Unlock()
}

func (s *DelayNotifier) Notify() {
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

	DPrintf("%v %v: state %v\n", rf.me, rf.currentTerm, rf.log)

	// 假设调用时已经加锁
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.firstIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var firstIndex int
	var lastIncludedTerm int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&firstIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logEntries) != nil {
		DPrintf("readPersist fail %v\n", rf.me)
	} else {
		// 重启恢复时也要加锁
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.firstIndex = firstIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.log = logEntries

		if snapshot == nil || len(snapshot) < 1 {
			rf.snapshot = nil
		} else {
			rf.snapshot = snapshot
		}

		// 读取 snapshot 后要改变
		rf.lastApplied = rf.firstIndex
		DPrintf("%v %v: readPersist success\n", rf.me, currentTerm)
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// 保留位于 index 位置的条目，以简化逻辑
	// index 通常看作逻辑位置，对 rf.log 条目的访问用到物理位置
	// 逻辑位置 = 物理位置 + rf.firstIndex（物理位置 0 条目的逻辑位置）
	rf.mu.Lock()
	DPrintf("%v %v: Snapshot from index %v\n", rf.me, rf.currentTerm, index)

	// 新 snapshot 包含的 index 大于现有时更新
	// 可能出现在 leader 发送了 snapshot 之后
	if index > rf.firstIndex {
		rf.snapshot = snapshot
		rf.lastIncludedTerm = rf.log[index-rf.firstIndex].Term
		rf.log = rf.log[index-rf.firstIndex:]
		rf.firstIndex = index
		rf.persist()
	}

	rf.mu.Unlock()

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
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.VoteGranted = false
	} else {
		DPrintf("%v %v: RequestVote from %v\n", rf.me, rf.currentTerm, args.CandidateId)
		// 收到可能的候选人消息
		rf.receive = true

		rf.requestPreprocess(args.Term)

		lastLogIndex := len(rf.log) - 1
		lastLogEntry := rf.log[lastLogIndex]
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogEntry.Term || (args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogIndex)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
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

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 加速日志恢复
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Success = false
	} else {
		// 一个 term 只有一个 Leader
		rf.receive = true

		rf.requestPreprocess(args.Term)

		// If AppendEntries RPC received from new leader: convert to
		// follower
		if rf.currentState == Candidate {
			rf.currentState = Follower
		}

		// not heartbeat
		if len(args.Entries) != 0 {

			// Reply false if log doesn’t contain an entry at prevLogIndex
			// whose term matches prevLogTerm (§5.3)
			if args.PrevLogIndex >= len(rf.log)+rf.firstIndex || (args.PrevLogIndex-rf.firstIndex >= 0 && rf.log[args.PrevLogIndex-rf.firstIndex].Term != args.PrevLogTerm) {
				reply.Success = false

				// PrevLogIndex 位置没有 entry
				if args.PrevLogIndex >= len(rf.log)+rf.firstIndex {
					reply.XTerm = -1
					reply.XLen = args.PrevLogIndex - len(rf.log) + 1 - rf.firstIndex
				} else {
					// 一次回退 follower 的一整个 term
					pIndex := args.PrevLogIndex - rf.firstIndex
					term := rf.log[pIndex].Term
					reply.XTerm = rf.log[pIndex].Term
					for pIndex >= 0 && rf.log[pIndex].Term == term {
						pIndex--
					}
					reply.XIndex = pIndex + 1
				}

			} else {
				// If an existing entry conflicts with a new one (same index
				// but different terms), delete the existing entry and all that
				// follow it (§5.3)
				// 实现成本地 Log 更短直接 append，否则判断上面的规则
				DPrintf("%v %v: ae %v %v %v\n", rf.me, rf.currentTerm, args, rf.log, rf.firstIndex)
				if len(rf.log)+rf.firstIndex-(args.PrevLogIndex+1) > len(args.Entries) {
					for i := 0; i < len(args.Entries); i++ {
						if args.PrevLogIndex+1+i-rf.firstIndex >= 0 && rf.log[args.PrevLogIndex+1+i-rf.firstIndex].Term != args.Entries[i].Term {
							rf.log = append(rf.log[:args.PrevLogIndex+1-rf.firstIndex+i], args.Entries[i:]...)
							rf.persist()
							break
						}
					}
				} else {
					// Append any new entries not already in the log
					i := rf.firstIndex - args.PrevLogIndex - 1
					if i >= 0 {
						rf.log = append(rf.log[:args.PrevLogIndex+1-rf.firstIndex+i], args.Entries[i:]...)
					} else {
						rf.log = append(rf.log[:args.PrevLogIndex+1-rf.firstIndex], args.Entries...)
					}
					rf.persist()
				}
				reply.Success = true
			}

		}

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		// LeaderCommit 设置为 min(matchIndex[i], commitIndex)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+rf.firstIndex)

			rf.commitChanged.Notify()
		}

	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	// 3D
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// 注释未使用的字段
	// Offset            int
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.firstIndex {
		// Reply immediately if term < currentTerm
		// 防止多次安装相同 snapshot
	} else {
		rf.receive = true

		rf.requestPreprocess(args.Term)

		// Create new snapshot file if first chunk (offset is 0)
		// Write data into snapshot file at given offset
		// Reply and wait for more data chunks if done is false
		// 假设一次发完，也就是 Done 一定为 true
		// Save snapshot file, discard any existing or partial snapshot
		// with a smaller index
		// 在这里是 rf.persist()
		rf.snapshot = args.Data
		rf.lastIncludedTerm = args.LastIncludedTerm
		// If existing log entry has same index and term as snapshot’s
		// last included entry, retain log entries following it and reply
		if args.LastIncludedIndex <= len(rf.log)-1+rf.firstIndex && args.LastIncludedTerm == rf.log[args.LastIncludedIndex-rf.firstIndex].Term {
			rf.log = rf.log[args.LastIncludedIndex-rf.firstIndex:]
		} else {
			// Discard the entire log
			rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		}
		rf.firstIndex = args.LastIncludedIndex

		// Reset state machine using snapshot contents (and load
		// snapshot’s cluster configuration)
		rf.snapshotNotifier.Notify()
		rf.lastApplied = rf.firstIndex

		rf.persist()
		DPrintf("%v %v: receive InstallSnapshot to %v \n", rf.me, args.Term, args.LastIncludedIndex)

	}
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	if isLeader {
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine (§5.3)
		rf.log = append(rf.log, LogEntry{command, term})
		index = len(rf.log) - 1 + rf.firstIndex
		for i := 0; i < rf.serverCount; i++ {
			if i == rf.me {
				continue
			}
			rf.sendNotifiers[i].Notify()
		}
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++

		rf.persist()

		DPrintf("%v %v: command %v appear in %v\n", rf.me, rf.currentTerm, command, index)

	}
	rf.mu.Unlock()

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
	rf.cleanup()
	rf.commitChanged.Notify()
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

			DPrintf("%v %v: timeout\n", rf.me, rf.currentTerm)
			go rf.sendRequestVoteToAll(rf.currentTerm)

		}
		rf.receive = false
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 350 + (rand.Int63() % 350)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: -1}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.serverCount = len(peers)
	rf.currentState = Follower
	rf.receive = true
	rf.sendNotifiers = make([]*DelayNotifier, rf.serverCount)
	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		rf.sendNotifiers[i] = NewDelayNotifier()
	}
	rf.commitChanged = NewDelayNotifier()
	rf.applyCh = applyCh
	rf.snapshotNotifier = NewDelayNotifier()

	rf.firstIndex = 0
	rf.lastIncludedTerm = -1
	rf.snapshot = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyRoutine()

	return rf
}

func (rf *Raft) sendLogRoutine(cond *DelayNotifier, server int, forTerm int) {
	DPrintf("%v %v: sendLogRoutine for %v\n", rf.me, forTerm, server)

	cond.Wait()
	for !rf.killed() && forTerm == rf.currentTerm {

		if rf.routerCount[server] < 5 {
			go rf.sendLoop(forTerm, server)
			rf.routerCount[server]++
		}
		// 最多 10 毫秒循环一次
		time.Sleep(10 * time.Millisecond)
		cond.Wait()
	}

}

func (rf *Raft) sendLoop(forTerm int, server int) {
	success := false
	for !rf.killed() && !success && forTerm == rf.currentTerm {

		rf.mu.Lock()
		// 上一条消息已经 discard，发送 snapshot 代替
		// 获取锁后检查再次 term
		for rf.nextIndex[server]-1-rf.firstIndex < 0 && forTerm == rf.currentTerm {
			reply := InstallSnapshotReply{}
			args := InstallSnapshotArgs{forTerm, rf.me, rf.firstIndex, rf.lastIncludedTerm, rf.snapshot}
			rf.mu.Unlock()

			DPrintf("%v %v: send InstallSnapshot %v to %v\n", rf.me, args.Term, args.LastIncludedIndex, server)
			ok := rf.sendInstallSnapshot(server, &args, &reply)

			rf.mu.Lock()
			if ok {
				DPrintf("%v %v: send InstallSnapshot %v to %v success\n", rf.me, args.Term, args.LastIncludedIndex, server)
				rf.responsePreprocess(reply.Term)
				if reply.Term > rf.currentTerm || rf.currentTerm != forTerm {
					break
				}
				rf.successUpdateFollowerState(server, args.LastIncludedIndex)
			}

		}
		var args *AppendEntriesArgs
		var lastLog, nextIndex int
		reply := AppendEntriesReply{}
		if forTerm == rf.currentTerm {
			lastLog = len(rf.log) - 1 + rf.firstIndex
			nextIndex = rf.nextIndex[server]
			args = &AppendEntriesArgs{forTerm, rf.me, nextIndex - 1, rf.log[nextIndex-1-rf.firstIndex].Term, rf.log[nextIndex-rf.firstIndex:], min(rf.commitIndex, rf.matchIndex[server])}
		}
		rf.mu.Unlock()

		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		if args != nil && lastLog >= rf.nextIndex[server] && forTerm == rf.currentTerm {
			DPrintf("%v %v: send AppendEntries[%v - %v] to %v\n", rf.me, args.Term, nextIndex-1, lastLog, server)
			ok := rf.sendAppendEntries(server, args, &reply)

			if ok {

				rf.mu.Lock()
				rf.responsePreprocess(reply.Term)
				if reply.Term > rf.currentTerm || forTerm != rf.currentTerm {
					rf.mu.Unlock()
					break
				}
				DPrintf("%v %v: finish send AppendEntries[%v - %v] to %v %v\n", rf.me, args.Term, nextIndex-1, lastLog, server, reply.Success)

				// If successful: update nextIndex and matchIndex for
				// follower (§5.3)
				if reply.Success {
					success = true

					rf.successUpdateFollowerState(server, lastLog)

					rf.mu.Unlock()
					break
				} else {
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (§5.3)
					// 快速恢复
					// nextIndex 与之前记录相等时更新，否则可能已经被更新过
					if nextIndex == rf.nextIndex[server] {
						if reply.XTerm == -1 {
							rf.nextIndex[server] -= reply.XLen
						} else {
							// 大力出奇迹，一次回退 follower 的一整个 term
							rf.nextIndex[server] = reply.XIndex
						}
						DPrintf("%v %v: update nextIndex[%v] to %v\n", rf.me, args.Term, server, rf.nextIndex[server])
					}
				}

				rf.mu.Unlock()
			}
		} else {
			break
		}
	}

	rf.mu.Lock()
	if forTerm == rf.currentTerm {
		rf.routerCount[server]--
	}
	rf.mu.Unlock()
}

func (rf *Raft) successUpdateFollowerState(server int, lastLog int) {
	// 更大时更新，防止网络延迟
	rf.nextIndex[server] = max(lastLog+1, rf.nextIndex[server])
	rf.matchIndex[server] = max(lastLog, rf.matchIndex[server])

	DPrintf("%v %v: update nextIndex[%v] to %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server])

	// 是否有更高的 committed
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	n := median(rf.matchIndex)
	if n > rf.commitIndex && rf.log[n-rf.firstIndex].Term == rf.currentTerm {
		rf.commitIndex = n
		DPrintf("%v %v: commitChanged to %v \n", rf.me, rf.currentTerm, n)

		rf.commitChanged.Notify()
	}
}

func (rf *Raft) sendRequestVoteToAll(forTerm int) {

	voteCount := 1

	ch := make(chan *RequestVoteReply)

	rf.mu.Lock()
	if rf.currentTerm != forTerm {
		rf.mu.Unlock()
		return
	}
	// On conversion to candidate, start election:
	//	• Increment currentTerm
	//	• Vote for self
	//	• Reset election timer
	//	• Send RequestVote RPCs to all other servers
	rf.currentState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.receive = true
	forTerm = rf.currentTerm
	rf.persist()

	logLen := len(rf.log)
	args := &RequestVoteArgs{forTerm, rf.me, logLen - 1, rf.log[logLen-1].Term}
	rf.mu.Unlock()
	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		// 并行发送
		go rf.sendRequestVoteParallel(ch, i, args)
	}

	// 处理所有 RequestVote 的回复
	for i := 0; i < rf.serverCount-1; i++ {
		reply := <-ch

		rf.mu.Lock()
		rf.responsePreprocess(reply.Term)

		if reply.VoteGranted {
			voteCount++
			// If votes received from majority of servers: become leader
			if voteCount == (rf.serverCount/2)+1 {
				if forTerm == rf.currentTerm {
					DPrintf("%v %v: become leader\n", rf.me, rf.currentTerm)
					rf.currentState = Leader
					rf.initLeaderState()
				}

			}
		}
		rf.mu.Unlock()
	}

}

// 调用时已经加锁
func (rf *Raft) initLeaderState() {
	rf.nextIndex = make([]int, rf.serverCount)
	rf.matchIndex = make([]int, rf.serverCount)
	rf.routerCount = make([]int, rf.serverCount)
	rf.sendNotifiers = make([]*DelayNotifier, rf.serverCount)
	for i := 0; i < rf.serverCount; i++ {
		// for each server, index of the next log entry
		// to send to that server (initialized to leader
		// last log index + 1)
		rf.nextIndex[i] = len(rf.log) + rf.firstIndex
		// for each server, index of highest log entry
		// known to be replicated on server
		// (initialized to 0, increases monotonically)
		rf.matchIndex[i] = 0
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log) - 1 + rf.firstIndex
			continue
		}
		rf.sendNotifiers[i] = NewDelayNotifier()
		go rf.sendLogRoutine(rf.sendNotifiers[i], i, rf.currentTerm)
		// 一直发送空 appendEntries
		go rf.heartbeatTicker(rf.currentTerm, i)
	}
}

// repeat during idle periods to
// prevent election timeouts (§5.2)
func (rf *Raft) heartbeatTicker(forTerm int, server int) {
	for rf.killed() == false && rf.currentTerm == forTerm {
		// Upon election: send initial empty AppendEntries RPCs
		// (heartbeat) to each server;
		go rf.sendHeartbeat(server)
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	// 心跳时不关心别的字段
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: min(rf.commitIndex, rf.matchIndex[server])}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	if ok {

		rf.mu.Lock()
		rf.responsePreprocess(reply.Term)

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendRequestVoteParallel(ch chan *RequestVoteReply, server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		reply.Term = -1
		reply.VoteGranted = false
	}
	ch <- &reply

}

// 从 Leader 状态转换时调用，认为已经持有锁
func (rf *Raft) cleanup() {
	for i := 0; i < rf.serverCount; i++ {
		if i == rf.me {
			continue
		}
		rf.sendNotifiers[i].Notify()
	}

}

func (rf *Raft) applyRoutine() {
	rf.commitChanged.Wait()
	for rf.killed() == false {
		// If commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine (§5.3)

		for rf.commitIndex > rf.lastApplied && !rf.killed() {
			rf.mu.Lock()
			// 假设 leader 不用将 snapshot 发送到 applyCh
			// DPrintf("%v %v %v\n", rf.lastApplied, rf.firstIndex, rf.snapshot == nil)
			if rf.lastApplied-rf.firstIndex == 0 && rf.snapshot != nil {
				msg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.lastIncludedTerm, SnapshotIndex: rf.firstIndex}
				rf.mu.Unlock()
				DPrintf("%v %v: apply snapshot at %v\n", rf.me, rf.currentTerm, msg.SnapshotIndex)
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			rf.lastApplied++
			apply := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.firstIndex].Command, CommandIndex: rf.lastApplied}
			DPrintf("%v %v: apply %v at %v -- firstIndex: %v\n", rf.me, rf.currentTerm, apply.Command, apply.CommandIndex, rf.firstIndex)
			rf.mu.Unlock()
			rf.applyCh <- apply
		}

		rf.commitChanged.Wait()
	}
}

// 假定调用时持有锁
func (rf *Raft) requestPreprocess(argTerm int) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if argTerm > rf.currentTerm {
		if rf.currentState == Leader {
			rf.cleanup()
		}
		rf.currentTerm = argTerm
		rf.votedFor = -1
		rf.currentState = Follower
		rf.persist()
	}
}

// 假定调用时持有锁
func (rf *Raft) responsePreprocess(replyTerm int) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if replyTerm > rf.currentTerm {
		if rf.currentState == Leader {
			rf.cleanup()
		}
		rf.currentTerm = replyTerm
		rf.votedFor = -1
		rf.currentState = Follower
		rf.persist()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func median(nums []int) int {
	// 复制切片以保持原始切片不变
	copied := make([]int, len(nums))
	copy(copied, nums)

	// 排序复制的切片
	sort.Ints(copied)

	n := len(copied)
	if n%2 == 1 {
		// 如果切片长度是奇数，中位数是中间的元素
		return copied[n/2]
	} else {
		// 如果切片长度是偶数
		return copied[n/2-1]
	}
}
