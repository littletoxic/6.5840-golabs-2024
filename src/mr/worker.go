package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		nReduce := int(reply.NReduce)
		if ok {

			switch reply.State {
			case Map, WaitingMap:

				filename := reply.File
				mapCount := reply.MapCount

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

				tmpFiles := make([]*os.File, nReduce)
				encoders := make([]*json.Encoder, nReduce)
				for i := 0; i < nReduce; i++ {
					tmpFiles[i], err = os.CreateTemp(".", "tmp-mr-*")
					if err != nil {
						log.Fatalf("cannot create temp File: %v", err)
					}
					encoders[i] = json.NewEncoder(tmpFiles[i])
				}

				// 将 map 的结果按 NReduce 分片写入文件
				for _, kv := range kva {
					reduce := ihash(kv.Key) % nReduce
					err := encoders[reduce].Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode kv pair: %v", err)
					}
				}

				for i := 0; i < nReduce; i++ {
					tmpFiles[i].Close()
					newName := fmt.Sprintf("mr-%d-%d", mapCount, nReduce)
					err := os.Rename(tmpFiles[i].Name(), newName)
					if err != nil {
						log.Fatalf("cannot rename temp File: %v", err)
					}
				}

				// 处理 File 完成
				args.File = reply.File

			case Reduce, WaitingReduce:

				reduceCount := reply.ReduceCount

				// 等待
				if reduceCount == 0 {
					time.Sleep(1 * time.Second)
					break
				}

				pattern := fmt.Sprintf("mr-*-%d", reduceCount-1)
				files, err := filepath.Glob(pattern)
				if err != nil {
					log.Fatalf("error finding files: %v", err)
				}

				// 读取 ReduceCount 要处理的所有文件
				kva := []KeyValue{}
				for _, file := range files {
					f, err := os.Open(file)
					if err != nil {
						log.Fatalf("cannot open  %v", file)
					}

					decoder := json.NewDecoder(f)
					var kv KeyValue
					for decoder.More() {
						if err := decoder.Decode(&kv); err != nil {
							log.Fatalf("cannot decode kv pair: %v", err)
						}
						kva = append(kva, kv)
					}

					f.Close()
				}

				sort.Sort(ByKey(kva))

				oname := fmt.Sprintf("mr-out-%d", reduceCount-1)
				ofile, _ := os.Create(oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				ofile.Close()

			case Done:
				fmt.Printf("finish!\n")
				os.Exit(0)
			}

		} else {
			fmt.Printf("call failed!\n")
			os.Exit(1)
		}
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
