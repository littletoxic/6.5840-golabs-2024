package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	CommandType CommandType
	Key         string
	Value       string
	CommiterId  int
	ClientId    int64
	SendCount   int64
}

type CommandType int

const (
	GET = iota
	PUT
	APPEND
)

type Result struct {
	commiterId int
	result     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data         map[string]string
	lastExecuted map[int64]int64
	notifyChs    map[int]chan Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		CommandType: GET,
		Key:         args.Key,
		Value:       "",
		CommiterId:  kv.me,
		ClientId:    args.Id,
		SendCount:   args.SendCount,
	}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		// 直接返回
		reply.Err = ErrWrongLeader
		return
	}
	// 等待提交结果
	ch := make(chan Result)
	kv.mu.Lock()
	kv.notifyChs[index] = ch
	kv.mu.Unlock()
	result := <-ch
	if result.commiterId == kv.me {
		reply.Err = OK
		reply.Value = result.result
	} else {
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutAppend(args, reply, PUT)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutAppend(args, reply, APPEND)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, commandType CommandType) {
	cmd := Op{
		CommandType: commandType,
		Key:         args.Key,
		Value:       args.Value,
		CommiterId:  kv.me,
		ClientId:    args.Id,
		SendCount:   args.SendCount,
	}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		// 直接返回
		reply.Err = ErrWrongLeader
		return
	}

	// 等待提交结果
	ch := make(chan Result)
	kv.mu.Lock()
	kv.notifyChs[index] = ch
	kv.mu.Unlock()
	result := <-ch
	if result.commiterId == kv.me {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.mu.Unlock()

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
	kv.data = make(map[string]string)
	kv.lastExecuted = make(map[int64]int64)
	kv.notifyChs = make(map[int]chan Result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyRoutine()
	go kv.heartbeatRoutine()

	return kv
}

func (kv *KVServer) applyRoutine() {
	for !kv.killed() {
		apply := <-kv.applyCh

		if apply.CommandValid {
			cmd := apply.Command.(Op)
			DPrintf("%v %v\n", cmd, apply.CommandIndex)
			result := Result{commiterId: cmd.CommiterId}
			kv.mu.Lock()
			ch, exist := kv.notifyChs[apply.CommandIndex]
			kv.mu.Unlock()
			switch cmd.CommandType {
			case GET:
				result.result = kv.data[cmd.Key]

			case PUT:
				if cmd.SendCount > kv.lastExecuted[cmd.ClientId] {
					kv.lastExecuted[cmd.ClientId] = cmd.SendCount
					kv.data[cmd.Key] = cmd.Value

				}

			case APPEND:
				if cmd.SendCount > kv.lastExecuted[cmd.ClientId] {
					kv.lastExecuted[cmd.ClientId] = cmd.SendCount
					kv.data[cmd.Key] = kv.data[cmd.Key] + cmd.Value
				}

			}

			if exist {
				ch <- result
			}

		}

		if apply.SnapshotValid {

		}
	}
}

// 应用层心跳，检查 Raft 状态是否改变
func (kv *KVServer) heartbeatRoutine() {
	// 最长可能选举时间
	time.Sleep(700 * time.Millisecond)
	for !kv.killed() {
		cmd := Op{
			CommandType: GET,
			Key:         "",
			Value:       "",
			CommiterId:  kv.me,
			ClientId:    -1,
			SendCount:   0,
		}
		kv.rf.Start(cmd)
		time.Sleep(700 * time.Millisecond)
	}
}
