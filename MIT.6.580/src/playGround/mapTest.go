package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Task struct {
	FileName   string
	TaskNumber int
}

func main() {

	var mu sync.Mutex

	mu.Lock()
	mu.Lock()

	newTasksQueue := make(map[string]Task)

	if len(newTasksQueue) == 0 {
		fmt.Printf("Queue is empty\n")
	}

	newTasksQueue["key1"] = Task{
		FileName:   "File1",
		TaskNumber: 1,
	}

	fmt.Printf("%v\n", newTasksQueue["key1"])

	var ops atomic.Int32

	ops.Store(55)
	ops.Add(-1)

	fmt.Printf("%v\n", ops.Load())

}
