package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastSuccess int
	serverCount int
	id          int64
	sendCount   int64
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
	ck.lastSuccess = 0
	ck.serverCount = len(servers)
	ck.id = nrand()
	ck.sendCount = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.sendCount++
	args := GetArgs{key, ck.id, ck.sendCount}
	reply := GetReply{}

	success := false
	success = ck.servers[ck.lastSuccess].Call("KVServer.Get", &args, &reply)
	for !success || reply.Err != OK {
		ck.lastSuccess = (ck.lastSuccess + 1) % ck.serverCount
		reply = GetReply{}
		success = ck.servers[ck.lastSuccess].Call("KVServer.Get", &args, &reply)
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
	ck.sendCount++
	args := PutAppendArgs{key, value, ck.id, ck.sendCount}
	reply := PutAppendReply{}

	success := false
	for !success || reply.Err != OK {
		ck.lastSuccess = (ck.lastSuccess + 1) % ck.serverCount
		reply = PutAppendReply{}
		success = ck.servers[ck.lastSuccess].Call("KVServer."+op, &args, &reply)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
