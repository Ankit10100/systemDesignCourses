package main

import (
	"fmt"
	"time"
)

func main() {
	timeStart := time.Now()
	fmt.Printf("TimeStart: %v \n", timeStart)
	time.Sleep(5 * time.Second)

	elapased := time.Now().Sub(timeStart)
	fmt.Printf("TimeElapsed: %v \n", elapased.Seconds())
}
