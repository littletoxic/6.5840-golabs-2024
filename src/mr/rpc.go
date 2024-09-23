package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type Args struct {
	// Map 和 WaitingMap 阶段使用
	File string

	// Reduce 阶段
	ReduceCount int64
}

type Reply struct {
	// Map 和 WaitingMap 阶段使用
	File     string
	MapCount int64

	// Reduce 阶段
	// 从 1 开始，worker 创建文件时减一
	ReduceCount int64

	// 全局
	NReduce int64
	State   int64
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
