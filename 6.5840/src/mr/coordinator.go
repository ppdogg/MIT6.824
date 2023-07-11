package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type coordinatorState int

const (
	Mapping coordinatorState = iota
	Reducing
	WaitMapping
	WaitReducing
	Exiting
)

type Coordinator struct {
	// Your definitions here.

	// input
	files   []string // input for map task
	nReduce int      // nums of reducer

	// state of coordinator
	state       coordinatorState // record temporary state: mapping/reducing/waiting/exiting
	reduceFiles map[int][]int    // input for reduce task: reduce task id -> [map task id]

	// state of task
	failedTask  chan int              // failed task id
	runningTask map[int]chan struct{} // running task

	// concurrent control
	mapTask    chan int // map task concurrent control
	reduceTask chan int // reduce task concurrent control

	latch sync.Mutex // avoid race condition
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskRPCArgs, reply *TaskRPCReply) error {
	c.latch.Lock()
	defer c.latch.Unlock()
	switch c.state {
	case Mapping:
		select {
		case taskId := <-c.mapTask:
			reply.NReduce = c.nReduce
			reply.Task = Task{
				TaskId:    taskId,
				TaskType:  MapTask,
				InputFile: []string{c.files[taskId]},
			}
			localchan := c.MoniterTask(taskId)
			c.runningTask[taskId] = localchan
		default:
			c.state = WaitMapping
			reply.Task = Task{
				TaskType: WaitTask,
			}
		}
	case Reducing:
		select {
		case taskId := <-c.reduceTask:
			var inputFiles []string
			for _, mid := range c.reduceFiles[taskId] {
				inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", mid, taskId))
			}
			reply.Task = Task{
				TaskId:    taskId,
				TaskType:  ReduceTask,
				InputFile: inputFiles, // mr-X-Y
			}
			localchan := c.MoniterTask(taskId)
			c.runningTask[taskId] = localchan
		default:
			c.state = WaitReducing
			reply.Task = Task{
				TaskType: WaitTask,
			}
		}
	case WaitMapping:
		reply.Task = Task{
			TaskType: WaitTask,
		}
		var undoneTask []int
		for i := 0; i < len(c.files); i++ {
			if c.runningTask[i] != nil {
				undoneTask = append(undoneTask, i)
			}
		}
		if len(undoneTask) != 0 {
			break
		}

		flag := false
		for ok := false; !ok; {
			select {
			case tid := <-c.failedTask:
				flag = true
				c.mapTask <- tid
			default:
				ok = true
			}
		}

		if flag {
			c.state = Mapping
			c.ResetState(len(c.files))
		} else {
			c.state = Reducing
			c.ResetState(c.nReduce)
		}

	case WaitReducing:
		reply.Task = Task{
			TaskType: WaitTask,
		}
		var undoneTask []int
		for i := 0; i < c.nReduce; i++ {
			if c.runningTask[i] != nil {
				undoneTask = append(undoneTask, i)
			}
		}
		if len(undoneTask) != 0 {
			break
		}

		flag := false
		for ok := false; !ok; {
			select {
			case tid := <-c.failedTask:
				flag = true
				c.reduceTask <- tid
			default:
				ok = true
			}
		}

		if flag {
			c.state = Reducing
			c.ResetState(c.nReduce)
		} else {
			c.state = Exiting
		}
	case Exiting:
		reply.Task = Task{
			TaskType: DoneTask,
		}
	}
	return nil
}

func (c *Coordinator) Report(args *ReportRPCArgs, reply *ReportRPCReply) error {
	c.latch.Lock()
	defer c.latch.Unlock()
	task := args.Task
	switch task.TaskStatus {
	case Idle:
		break
	case Running:
		if c.runningTask[task.TaskId] != nil {
			c.runningTask[task.TaskId] <- struct{}{}
			select {
			case <-c.runningTask[task.TaskId]:
				close(c.runningTask[task.TaskId])
				localChan := c.MoniterTask(task.TaskId)
				c.runningTask[task.TaskId] = localChan
			}
		}
	case Finished:
		if c.runningTask[task.TaskId] != nil {
			c.runningTask[task.TaskId] <- struct{}{}
			select {
			case <-c.runningTask[task.TaskId]:
				close(c.runningTask[task.TaskId])
				c.runningTask[task.TaskId] = nil
			}
			if task.TaskType == MapTask {
				for rid, ok := range args.OutputFile {
					if ok {
						c.reduceFiles[rid] = append(c.reduceFiles[rid], task.TaskId)
					}
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) MoniterTask(taskId int) chan struct{} {
	localChan := make(chan struct{})
	go func() {
		select {
		case <-c.runningTask[taskId]:
			localChan <- struct{}{}
		case <-time.After(10 * time.Second):
			c.failedTask <- taskId
			close(c.runningTask[taskId])
			c.runningTask[taskId] = nil
		}
	}()
	return localChan
}

func (c *Coordinator) ResetState(length int) {
	c.failedTask = make(chan int, length)
	c.runningTask = make(map[int]chan struct{})
}

//
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
	if c.state == Exiting {
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
	c.files = files
	c.nReduce = nReduce

	c.state = Mapping
	c.reduceFiles = make(map[int][]int)

	c.failedTask = make(chan int, len(files))
	c.runningTask = make(map[int]chan struct{})

	c.mapTask = make(chan int, len(files))
	c.reduceTask = make(chan int, nReduce)
	for i := 0; i < len(files); i++ {
		c.mapTask <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTask <- i
	}

	c.server()
	return &c
}
