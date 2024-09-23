package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := Args{}
	for {

		reply := Reply{}

		ok := call("Coordinator.Distribute", &args, &reply)
		if ok {

			switch reply.state {
			case Map, WaitingMap:

				filename := reply.file

				// 等待
				if filename == "" {
					time.Sleep(1 * time.Second)
					break
				}

				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))

				tmpFiles := make([]*os.File, reply.nReduce)
				encoders := make([]*json.Encoder, reply.nReduce)
				for i := 0; i < reply.nReduce; i++ {
					tmpFiles[i], err = os.CreateTemp(".", "tmp-mr-*")
					if err != nil {
						log.Fatalf("cannot create temp file: %v", err)
					}
					encoders[i] = json.NewEncoder(tmpFiles[i])
				}

				for _, kv := range kva {
					reduce := ihash(kv.Key) % reply.nReduce
					err := encoders[reduce].Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode kv pair: %v", err)
					}
				}

				for i := 0; i < reply.nReduce; i++ {
					tmpFiles[i].Close()
					newName := fmt.Sprintf("mr-%d-%d", reply.mapCount, reply.nReduce)
					err := os.Rename(tmpFiles[i].Name(), newName)
					if err != nil {
						log.Fatalf("cannot rename temp file: %v", err)
					}
				}

			}

		} else {
			fmt.Printf("call failed!\n")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
