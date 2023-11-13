package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// defer sleep for 100ms to avoid busy waiting
		defer func() {
			time.Sleep(100 * time.Millisecond)
		}()

		mapTaskNum, mapTasksRemaining, mapTasksProcessing := CallMapTask(mapf)
		if mapTaskNum >= 0 {
			// let the coordinator know that the map task is done
			args := MapTaskDoneArgs{TaskNum: mapTaskNum}
			reply := MapTaskDoneReply{}
			ok := call("Coordinator.MapTaskDone", &args, &reply)
			if !ok {
				log.Printf("MapTaskDone call failed!\n")
			}
			continue
		}
		if mapTasksRemaining > 0 || mapTasksProcessing > 0 {
			continue
		}

		reduceTaskNum, reduceTasksRemaining, reduceTasksProcessing := CallReduceTask(reducef)
		if reduceTaskNum >= 0 {
			// let the coordinator know that the reduce task is done
			args := ReduceTaskDoneArgs{TaskNum: reduceTaskNum}
			reply := ReduceTaskDoneReply{}
			ok := call("Coordinator.ReduceTaskDone", &args, &reply)
			if !ok {
				log.Printf("ReduceTaskDone call failed!\n")
			}
			continue
		}
		if reduceTasksRemaining > 0 || reduceTasksProcessing > 0 {
			continue
		}

		break
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
func CallMapTask(mapf func(string, string) []KeyValue) (int, int, int) {
	args := MapTaskArgs{}
	reply := MapTaskReply{}
	ok := call("Coordinator.MapTask", &args, &reply)
	if !ok {
		log.Printf("MapTask call failed!\n")
		return -1, 0, 0
	}

	if reply.TaskNum < 0 {
		return reply.TaskNum, reply.TasksRemaining, reply.TasksProcessing
	}

	DoMap(mapf, reply.Filename, reply.TaskNum, reply.NReduce)
	return reply.TaskNum, reply.TasksRemaining, reply.TasksProcessing
}

func DoMap(mapf func(string, string) []KeyValue, filename string, taskNum int, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open map input '%v'", filename)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("cannot read map input '%v'", filename)
		return
	}
	file.Close()

	kva := mapf(filename, string(content))
	kvsplit := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % nReduce
		kvsplit[reduceNum] = append(kvsplit[reduceNum], kv)
	}

	for i := 0; i < nReduce; i++ {
		if kvsplit[i] == nil {
			continue
		}

		// create temporary map task output file for reduce task i from map task taskNum
		ofile, err := os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			log.Printf("cannot create map output '%v'", ofile.Name())
			continue
		}
		defer os.Remove(ofile.Name())

		enc := json.NewEncoder(ofile)
		for _, kv := range kvsplit[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("cannot encode key-value pair '%v'", kv)
				continue
			}
		}

		ofile.Close()

		// rename the temporary file to the final 'mr-X-Y', where X is the map task number, and Y is the reduce task number.
		oname := fmt.Sprintf("mr-%d-%d", taskNum, i)
		err = os.Rename(ofile.Name(), oname)
		if err != nil {
			log.Printf("cannot rename map output '%v'", oname)
			continue
		}
	}
}

func CallReduceTask(reducef func(string, []string) string) (int, int, int) {
	args := ReduceTaskArgs{}
	reply := ReduceTaskReply{}
	ok := call("Coordinator.ReduceTask", &args, &reply)
	if !ok {
		log.Printf("ReduceTask call failed!\n")
		return -1, 0, 0
	}

	if reply.TaskNum < 0 {
		return reply.TaskNum, reply.TasksRemaining, reply.TasksProcessing
	}

	DoReduce(reducef, reply.TaskNum, reply.NMap)
	return reply.TaskNum, reply.TasksRemaining, reply.TasksProcessing
}

func DoReduce(reducef func(string, []string) string, taskNum int, nMap int) {
	kvmap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		oname := fmt.Sprintf("mr-%d-%d", i, taskNum)
		file, err := os.Open(oname)
		if err != nil {
			// it's okay if some map tasks doesn't produce some of the intermediate files
			continue
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}

	ofile, err := os.CreateTemp("", "mr-tmp-*")
	if err != nil {
		log.Printf("cannot create reduce output '%v'", ofile.Name())
		return
	}
	defer os.Remove(ofile.Name())

	for k, v := range kvmap {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}

	ofile.Close()

	oname := fmt.Sprintf("mr-out-%d", taskNum)
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Printf("cannot rename reduce output '%v'", oname)
		return
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
