package mr

import (
	"log"
	"path/filepath"
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
	reply.NReduce = c.nReduce

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
			delete(c.mapStates, args.File)
			c.mapStates[file] = MapState{time.Now(), mapCount}
			c.statesLock.Unlock()

			reply.MapCount = mapCount
			reply.File = file
			reply.State = Map
			valid = true

		case WaitingMap:

			c.statesLock.Lock()
			delete(c.mapStates, args.File)

			var file string
			var mapCount int64
			for f, state := range c.mapStates {
				// 超时
				if time.Now().After(state.start.Add(time.Second * 10)) {
					file = f
					mapCount = state.mapCount
					c.mapStates[f] = MapState{time.Now(), mapCount}
					break
				}
			}

			if len(c.mapStates) == 0 {
				c.state.CompareAndSwap(WaitingMap, Reduce)
			}
			c.statesLock.Unlock()

			reply.MapCount = mapCount
			// 空表示等待
			reply.File = file
			reply.State = WaitingMap
			valid = true

		case Reduce:

			reduceCount := c.reduceCount.Add(1)

			// 任务发完
			if reduceCount > c.nReduce {
				c.state.CompareAndSwap(Reduce, WaitingReduce)
				break
			}

			c.statesLock.Lock()
			delete(c.reduceStates, args.ReduceCount)
			c.reduceStates[reduceCount] = time.Now()
			c.statesLock.Unlock()

			reply.ReduceCount = reduceCount
			reply.State = Reduce
			valid = true

		case WaitingReduce:

			c.statesLock.Lock()
			delete(c.reduceStates, args.ReduceCount)

			var reduceCount int64
			for i, t := range c.reduceStates {
				// 超时
				if time.Now().After(t.Add(time.Second * 10)) {
					reduceCount = i
					c.reduceStates[i] = time.Now()
					break
				}
			}

			if len(c.reduceStates) == 0 {
				c.state.CompareAndSwap(WaitingReduce, Done)
			}
			c.statesLock.Unlock()

			// 0 表示等待
			reply.ReduceCount = reduceCount
			reply.State = WaitingReduce
			valid = true

		case Done:
			reply.State = Done
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

		files, err := filepath.Glob("mr-*-inter")
		if err != nil {
			log.Printf("error finding files: %v", err)
		}

		tmp, err := filepath.Glob("tmp-mr-*")
		if err != nil {
			log.Printf("error finding files: %v", err)
		}

		files = append(files, tmp...)

		for _, file := range files {
			err := os.Remove(file)
			if err != nil {
				log.Printf("error removing file %s: %v", file, err)
			}
		}

		time.Sleep(3 * time.Second)
		return true
	}

	// Your code here.
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// Map 和 WaitingMap 阶段
	c.files = files
	c.mapStates = make(map[string]MapState)

	// Reduce 阶段
	c.reduceStates = make(map[int64]time.Time)

	// 全局
	c.nReduce = int64(nReduce)

	c.server()
	return &c
}
