package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

const baseTempDir = "./tempFiles"

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

func writeToFileAtomically(kvSlice []KeyValue, task Task, reduceBucketNumber int) {

	final_file_name := fmt.Sprintf("%v/MR-%v-%v", baseTempDir, task.TaskNumber, reduceBucketNumber)

	file, err := os.CreateTemp(baseTempDir, "temp-mapdata-")

	if err != nil {
		panic(err)
	}

	enc := json.NewEncoder(file)

	for _, kv := range kvSlice {
		err := enc.Encode(kv)
		if err != nil {
			panic(err)
		}
	}

	err = os.Rename(file.Name(), final_file_name)
	if err != nil {
		panic(err)
	}

	// file.Close()
}

func handleMapTask(mapf func(string, string) []KeyValue, task Task) {
	fmt.Printf("Handling Map Task\n")
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	kva := mapf(task.FileName, string(content))

	fmt.Printf("%v", kva[0])

	var map_output [NREDUCE][]KeyValue

	for _, kv := range kva {
		key_index := ihash(kv.Key) % NREDUCE

		map_output[key_index] = append(map_output[key_index], kv)
	}

	// fmt.Printf("%v\n", map_output[0])
	for reduceBucket, kv := range map_output {
		writeToFileAtomically(kv, task, reduceBucket)
	}
}

func handleReduceTask(reducef func(string, []string) string, task Task) {
	fmt.Printf("Handling Reduce Task\n")

	fileGlobPattern := fmt.Sprintf("%v/MR-*-%v", baseTempDir, task.TaskNumber)

	fileNames, err := filepath.Glob(fileGlobPattern)

	if err != nil {
		panic(err)
	}

	intermediate := []KeyValue{}

	for _, fileName := range fileNames {
		// filePath := fmt.Sprintf("%v/%v", baseTempDir, fileName)

		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Found and opened the file\n")
		dec := json.NewDecoder(file)

		for {
			var kvRead KeyValue
			// fmt.Printf("Inside the for loop\n")
			if err := dec.Decode(&kvRead); err != nil {
				fmt.Printf("Encountered Error\n")
				break
			}
			intermediate = append(intermediate, kvRead)
			// fmt.Printf("%v: %v", kvRead.Key, kvRead.Value)
			// break
		}
	}
	sort.Sort(ByKey(intermediate))

	fmt.Printf("%v", intermediate)

	oname := fmt.Sprintf("mr-out-%v", task.TaskNumber)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func GetTaskFromCoordinator() (Task, bool) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {

		task, ok := GetTaskFromCoordinator()

		if ok {
			fmt.Printf("reply.fileName %v\n", task.FileName)
			switch task.OperationName {
			case MAP_TASK:
				handleMapTask(mapf, task)
			case REDUCE_TASK:
				handleReduceTask(reducef, task)
			case NO_TASK:
				fmt.Printf("Found no task, sleeping\n")
				time.Sleep(5 * time.Second)
			}
		} else {
			fmt.Printf("call failed! Coordinator might not be running. Exiting.\n")
		}
	}

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
