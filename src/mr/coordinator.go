package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "sync/atomic"

type Coordinator struct {
	// Your definitions here.
	// Map 和 WaitingMap 阶段
	files     []string
	mapStates map[string]MapState
	filesLock sync.Mutex
	mapCount  atomic.Int64

	// Reduce 阶段
	reduceStates map[int64]time.Time
	reduceCount  atomic.Int64

	// 全局
	nReduce int64
	// 每个阶段的 states 共用
	statesLock sync.Mutex
	state      atomic.Int64
}

type MapState struct {
	start    time.Time
	mapCount int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(args *Args, reply *Reply) error {
	valid := false
	reply.nReduce = c.nReduce

	for !valid {
		switch c.state.Load() {
		case Map:

			c.filesLock.Lock()
			length := len(c.files)
			// 任务发完
			if length == 0 {
				c.filesLock.Unlock()
				c.state.CompareAndSwap(Map, WaitingMap)
				break
			}
			// 下一个文件
			file := c.files[length-1]
			c.files = c.files[:length-1]
			c.filesLock.Unlock()

			mapCount := c.mapCount.Add(1) - 1

			c.statesLock.Lock()
			delete(c.mapStates, args.file)
			c.mapStates[reply.file] = MapState{time.Now(), mapCount}
			c.statesLock.Unlock()

			reply.mapCount = mapCount
			reply.file = file
			reply.state = Map
			valid = true

		case WaitingMap:

			c.statesLock.Lock()
			delete(c.mapStates, args.file)

			var file string
			var mapCount int64
			for k, v := range c.mapStates {
				// 超时
				if time.Now().After(v.start.Add(time.Second * 10)) {
					file = k
					mapCount := v.mapCount
					c.mapStates[k] = MapState{time.Now(), mapCount}
					break
				}
			}

			if len(c.mapStates) == 0 {
				c.state.CompareAndSwap(WaitingMap, Reduce)
			}
			c.statesLock.Unlock()

			reply.mapCount = mapCount
			// 空表示等待
			reply.file = file
			reply.state = WaitingMap
			valid = true

		case Reduce:

			reduceCount := c.reduceCount.Add(1)

			// 任务发完
			if reduceCount >= c.nReduce {
				c.state.CompareAndSwap(Reduce, WaitingReduce)
				break
			}

			c.statesLock.Lock()
			c.reduceStates[reduceCount] = time.Now()
			c.statesLock.Unlock()

			reply.reduceCount = reduceCount
			reply.state = Reduce
			valid = true

		case WaitingReduce:

			c.statesLock.Lock()
			delete(c.reduceStates, args.reduceCount)

			var reduceCount int64
			for k, v := range c.reduceStates {
				// 超时
				if time.Now().After(v.Add(time.Second * 10)) {
					reduceCount = k
					c.reduceStates[k] = time.Now()
					break
				}
			}

			if len(c.mapStates) == 0 {
				c.state.CompareAndSwap(WaitingReduce, Done)
			}
			c.statesLock.Unlock()

			// 0 表示等待
			reply.reduceCount = reduceCount
			reply.state = WaitingReduce
			valid = true

		case Done:
			reply.state = Done
			valid = true

		}

	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.state.Load() == Done {
		time.Sleep(3 * time.Second)
		return true
	}

	// Your code here.
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = int64(nReduce)

	c.server()
	return &c
}
