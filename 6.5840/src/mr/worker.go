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
		// get task from coordinator
		args := TaskRPCArgs{}
		reply := TaskRPCReply{}
		if ok := call("Coordinator.GetTask", &args, &reply); !ok {
			fmt.Printf("call failed!\n")
			return
		}
		task := reply.Task
		if task.TaskType == DoneTask {
			break
		} else if task.TaskType == WaitTask {
			// waiting for new task
			time.Sleep(time.Second)
			continue
		}

		// send heartbeat to notify coordinator
		heartbeat := HeartBeat(task.TaskId, task.TaskType)

		// var rArgs ReportRPCArgs
		outputFile := make(map[int]bool)
		// do computing
		if task.TaskType == MapTask {
			res := make([]KeyValue, 0)
			for _, filename := range task.InputFile {
				// read file as input
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", task.InputFile)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", task.InputFile)
				}
				file.Close()

				// map job
				res = append(res, mapf(filename, string(content))...)
			}

			// write output to file: mr-X-Y
			resMap := make(map[string][]KeyValue)
			for _, kv := range res {
				reduceId := ihash(kv.Key) % reply.NReduce
				outputFileName := fmt.Sprintf("mr-%d-%d", task.TaskId, reduceId)
				resMap[outputFileName] = append(resMap[outputFileName], kv)
				outputFile[reduceId] = true
			}
			for k, v := range resMap {
				data, err := json.Marshal(v)
				if err != nil {
					log.Fatalf("cannot marshal to json: %v\n", res)
				}
				if err = os.WriteFile(k, data, 0666); err != nil {
					log.Fatalf("cannot write %v", k)
				}
			}
		} else if task.TaskType == ReduceTask {
			var kvs []KeyValue
			for _, filename := range task.InputFile {
				// read file as input
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", task.InputFile)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", task.InputFile)
				}
				file.Close()

				var kv []KeyValue
				if err := json.Unmarshal(content, &kv); err != nil {
					log.Fatalf("cannot unmarshal: %v\n", string(content))
				}
				kvs = append(kvs, kv...)
			}

			k2vMap := make(map[string][]string, 0)
			for _, kv := range kvs {
				k2vMap[kv.Key] = append(k2vMap[kv.Key], kv.Value)
			}
			// reducde job
			var res string
			for k, v := range k2vMap {
				total := reducef(k, v)
				res = fmt.Sprintf("%s %s\n%s", k, total, res)
			}

			outputFileName := fmt.Sprintf("mr-out-%d", task.TaskId)
			if err := os.WriteFile(outputFileName, []byte(res), 0666); err != nil {
				log.Fatalf("cannot write %v", outputFileName)
			}
		}
		heartbeat <- struct{}{}
		task.TaskStatus = Finished
		rArgs := ReportRPCArgs{Task: task, OutputFile: outputFile}
		if ok := call("Coordinator.Report", &rArgs, nil); !ok {
			fmt.Printf("call failed!\n")
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func HeartBeat(taskId int, taskType TaskType) chan struct{} {
	c := make(chan struct{})
	rArgs := ReportRPCArgs{
		Task: Task{
			TaskId:     taskId,
			TaskType:   taskType,
			TaskStatus: Running,
		},
	}

	go func() {
		heartbeat := time.NewTicker(1 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case <-c:
				return
			case <-heartbeat.C:
				if ok := call("Coordinator.Report", &rArgs, nil); !ok {
					fmt.Printf("task %d heartbeat call failed!\n", rArgs.Task.TaskId)
				}
			}
		}
	}()
	return c
}

//
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
