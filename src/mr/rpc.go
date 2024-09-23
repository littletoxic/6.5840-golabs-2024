package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args struct {
	// Map 和 WaitingMap 阶段使用
	file string

	// Reduce 阶段
	reduceCount int64
}

type Reply struct {
	// Map 和 WaitingMap 阶段使用
	file     string
	mapCount int64

	// Reduce 阶段
	// 从 1 开始，worker 创建文件时减一
	reduceCount int64

	// 全局
	nReduce int64
	state   int64
}

const (
	Map int64 = iota
	WaitingMap
	Reduce
	WaitingReduce
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
