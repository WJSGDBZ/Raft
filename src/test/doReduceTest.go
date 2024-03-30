package main

import (
	"fmt"
	"mit6.824/mr"
	"strconv"
)

func TestDoReduce(reply *mr.WorkerReply) {
	err := mr.DoReduce(Reduce, reply)
	if err != nil {
		fmt.Printf("DoReduce test err %v", err)
	}
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
