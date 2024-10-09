package main

import "fmt"

func main() {
	myMap := map[string]int{
		"apple":  1,
		"banana": 2,
		"cherry": 3,
	}

	// Get any key from the map
	for key := range myMap {
		// Delete the key-value pair
		delete(myMap, key)

		// Print the key and value
		fmt.Println("Removed:", key, myMap[key])

		// Break after removing the first item
		break
	}

	fmt.Println("Remaining map:", myMap)
}
