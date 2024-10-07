package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type KeyValue struct {
	Key   string
	Value string
}

func main() {

	//Writing to a file

	// content := "Something to be written to the file"

	// file, err := os.Create("./someTestFile")
	// if err != nil {
	// 	fmt.Printf("File Creation Failled")
	// 	panic(err)
	// }

	// io.WriteString(file, content)

	//Reading from the file
	// file, err := os.Open("./someTestFile")

	// if err != nil {
	// 	panic(err)
	// }

	// file_content, err := io.ReadAll(file)

	// fmt.Printf("%v\n", string(file_content))

	//Create a temp file and then rename it

	file, err := os.CreateTemp("./temp", "mr-test-")

	if err != nil {
		panic(err)
	}

	// io.WriteString(file, content)

	enc := json.NewEncoder(file)

	var kva []KeyValue

	kva = append(kva, KeyValue{Key: "Key1", Value: "ValueForKey1"})
	kva = append(kva, KeyValue{Key: "Key2", Value: "ValueForKey2"})

	for _, kv := range kva {
		err := enc.Encode(kv)
		if err != nil {
			panic(err)
		}
	}

	os.Rename(file.Name(), "Non-TempFile")

	file.Close()

	/////

	//Open and again read the file
	// fmt.Printf("Trying to open the file\n")

	// file, err = os.Open("Non-TempFile")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("Found and opened the file\n")
	// dec := json.NewDecoder(file)

	// for {
	// 	var kvRead KeyValue
	// 	fmt.Printf("Inside the for loop\n")
	// 	if err := dec.Decode(&kvRead); err != nil {
	// 		fmt.Printf("Encountered Error\n")
	// 		break
	// 	}
	// 	fmt.Printf("%v: %v", kvRead.Key, kvRead.Value)
	// }

	files, err := filepath.Glob("./tempFiles/MR-14-*")
	if err != nil {
		fmt.Printf("Got error")
		panic(err)
	}

	for _, file := range files {
		fmt.Printf("%v\n", file)
	}

}
