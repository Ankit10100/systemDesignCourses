package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

}

var exitCoordinator bool
var currentTask Task

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	defer func() {
		exitCoordinator = true
	}()
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *ExampleArgs, task *Task) error {
	defer func() {
		exitCoordinator = true
	}()

	task.FileName = currentTask.FileName
	task.OperationName = currentTask.OperationName
	task.TaskNumber = currentTask.TaskNumber

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
	ret := exitCoordinator

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	exitCoordinator = false
	currentTask = Task{
		FileName:      files[0],
		OperationName: NO_TASK,
		TaskNumber:    0,
	}

	c.server()
	return &c
}
