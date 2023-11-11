package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nMap                   int
	nReduce                int
	mutex                  sync.Mutex
	unprocessedFiles       map[int]string
	processingFiles        map[int]string
	finishedFiles          map[int]string
	unprocessedReduceTasks []int
	processingReduceTasks  []int
	finishedReduceTasks    []int
}

// Your code here -- RPC handlers for the worker to call.

//
func (c *Coordinator) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.unprocessedFiles) == 0 {
		reply.TaskNum = -1
	} else {
		for k, v := range c.unprocessedFiles {
			reply.TaskNum = k
			reply.Filename = v
			reply.NReduce = c.nReduce
			delete(c.unprocessedFiles, k)
			c.processingFiles[k] = v
			break
		}
	}
	reply.TasksRemaining = len(c.unprocessedFiles)
	reply.TasksProcessing = len(c.processingFiles)
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	v, _ := c.processingFiles[args.TaskNum]
	delete(c.processingFiles, args.TaskNum)
	c.finishedFiles[args.TaskNum] = v
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.unprocessedReduceTasks) == 0 {
		reply.TaskNum = -1
	} else {
		reply.TaskNum = c.unprocessedReduceTasks[0]
		c.unprocessedReduceTasks = c.unprocessedReduceTasks[1:]
		c.processingReduceTasks = append(c.processingReduceTasks, reply.TaskNum)
		reply.NMap = c.nMap
	}
	reply.TasksRemaining = len(c.unprocessedReduceTasks)
	reply.TasksProcessing = len(c.processingReduceTasks)
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, v := range c.processingReduceTasks {
		if v == args.TaskNum {
			c.processingReduceTasks = append(c.processingReduceTasks[:i], c.processingReduceTasks[i+1:]...)
			c.finishedReduceTasks = append(c.finishedReduceTasks, args.TaskNum)
			break
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.finishedReduceTasks) == c.nReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce

	c.unprocessedFiles = make(map[int]string)
	for i := 0; i < len(files); i++ {
		c.unprocessedFiles[i] = files[i]
	}
	c.processingFiles = make(map[int]string)
	c.finishedFiles = make(map[int]string)

	c.unprocessedReduceTasks = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.unprocessedReduceTasks[i] = i
	}
	c.processingReduceTasks = make([]int, 0)
	c.finishedReduceTasks = make([]int, 0)

	c.server()
	return &c
}
