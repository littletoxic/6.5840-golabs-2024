package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data       map[string]string
	lastPut    map[int64]int64
	lastAppend map[int64]AppendState
}

type AppendState struct {
	sendCount int64
	lastValue string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	reply.Value = kv.data[args.Key]

	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if args.SendCount > kv.lastPut[args.Id] {
		kv.lastPut[args.Id] = args.SendCount
		kv.data[args.Key] = args.Value
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	appendState := kv.lastAppend[args.Id]
	if args.SendCount > appendState.sendCount {
		reply.Value = kv.data[args.Key]
		kv.lastAppend[args.Id] = AppendState{args.SendCount, reply.Value}
		kv.data[args.Key] = reply.Value + args.Value
	} else if args.SendCount == appendState.sendCount {
		reply.Value = appendState.lastValue
	} else {
		log.Fatal("不支持的情况")
	}

	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastPut = make(map[int64]int64)
	kv.lastAppend = make(map[int64]AppendState)

	return kv
}
