package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	nosMapTask          atomic.Int32
	nosReduceTask       atomic.Int32
	newTaskQueue        map[string]Task
	inprogressTaskQueue map[string]Task

	newTaskQueueMutex        sync.Mutex
	inprogressTaskQueueMutex sync.Mutex
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

	task.OperationName = NO_TASK

	c.newTaskQueueMutex.Lock()

	defer func() {
		// exitCoordinator = true
		c.newTaskQueueMutex.Unlock()
	}()

	//Pops a task from newTaskQueue
	//Adds the task to inprogress queue
	//Returns the task

	for taskId, newTask := range c.newTaskQueue {
		task.FileName = newTask.FileName
		task.OperationName = newTask.OperationName
		task.TaskNumber = newTask.TaskNumber
		task.TaskStartTime = time.Now()

		c.inprogressTaskQueueMutex.Lock()
		newTask.TaskStartTime = task.TaskStartTime
		c.inprogressTaskQueue[taskId] = newTask
		c.inprogressTaskQueueMutex.Unlock()

		delete(c.newTaskQueue, taskId)

		break
	}

	return nil
}

func (c *Coordinator) StartReduceTasks() {
	c.newTaskQueueMutex.Lock()
	for i := 0; i < NREDUCE; i++ {
		reduceTaskId := strconv.Itoa(i)
		c.newTaskQueue[reduceTaskId] = Task{
			FileName:      reduceTaskId,
			OperationName: REDUCE_TASK,
			TaskNumber:    i,
		}
	}

	c.newTaskQueueMutex.Unlock()
}

// Called by worker via RPC to mark completion of task
func (c *Coordinator) FinishTask(task *Task, ok *bool) error {

	fmt.Println("Finished Task: ", task)

	//todo: Remove Task from inprogress queue
	c.inprogressTaskQueueMutex.Lock()
	delete(c.inprogressTaskQueue, task.FileName)
	c.inprogressTaskQueueMutex.Unlock()

	switch task.OperationName {
	case MAP_TASK:
		c.nosMapTask.Add(-1)
		if c.nosMapTask.Load() == 0 {
			//Start reduce tasks
			fmt.Println("All Map Tasks Finished, starting reduce tasks")
			c.StartReduceTasks()
		}

	case REDUCE_TASK:
		c.nosReduceTask.Add(-1)
		if c.nosReduceTask.Load() == 0 {
			//All tasks completed, now exit the coordinator
			fmt.Println("All Reduce Tasks Finished, Exiting Coordinatior")
			exitCoordinator = true
		}
	}

	return nil
}

// Runs as a separate background thread and monitors inprogressTaskQueue
// If a task is in progress for more than 10 secs then moves it back to newQueue for rescheduling
func (c *Coordinator) taskMonitor() {
	for {
		//Go through the list and moitor the inprogress queue periodically
		fmt.Println("Started Task Monitor")
		expiredTasks := make(map[string]Task)

		//Lock the inprogress task list
		c.inprogressTaskQueueMutex.Lock()

		fmt.Println("Task Monitor, NewTaskList Length: ", len(c.newTaskQueue))
		fmt.Println("Task Monitor, InprogressTaskList Length: ", len(c.inprogressTaskQueue))

		for taskId, task := range c.inprogressTaskQueue {
			if time.Now().Sub(task.TaskStartTime).Seconds() > WORKER_FAIL_DETECT_TIME_SECONDS {
				//Take out task from inprogress and add it back to New tasklist
				expiredTasks[taskId] = task

				delete(c.inprogressTaskQueue, taskId)
			}
		}
		//UnLock the inprogress task list
		c.inprogressTaskQueueMutex.Unlock()

		if len(expiredTasks) > 0 {
			fmt.Println("Found Expired Tasks: ", len(expiredTasks))

			c.newTaskQueueMutex.Lock()
			for taskId, task := range expiredTasks {
				c.newTaskQueue[taskId] = task
			}
			c.newTaskQueueMutex.Unlock()
		}
		fmt.Println("Exiting Task Monitor")
		time.Sleep(5 * time.Second)
	}
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
	// ret := exitCoordinator

	// Your code here.

	return exitCoordinator
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nosMapTask.Store(int32(len(files)))
	// c.nosMapTask.Store(int32(1))
	c.nosReduceTask.Store(int32(NREDUCE))

	c.newTaskQueue = make(map[string]Task)
	c.inprogressTaskQueue = make(map[string]Task)

	// Your code here.
	exitCoordinator = false

	for taskNumber, fileName := range files {
		currentTask = Task{
			FileName:      fileName,
			OperationName: MAP_TASK,
			TaskNumber:    taskNumber,
		}

		c.newTaskQueue[fileName] = currentTask
	}

	go c.taskMonitor()

	c.server()
	return &c
}
