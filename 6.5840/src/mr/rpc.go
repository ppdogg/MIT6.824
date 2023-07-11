package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	DoneTask
	WaitTask
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Finished
)

type Task struct {
	TaskId     int
	TaskType   TaskType
	TaskStatus TaskStatus
	InputFile  []string // input
}

type TaskRPCArgs struct{}

type TaskRPCReply struct {
	Task    Task
	NReduce int
}

type ReportRPCArgs struct {
	Task       Task
	OutputFile map[int]bool // output
}

type ReportRPCReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
