package main

import (
	"fmt"
	"mit6.824/mr"
	"strings"
	"unicode"
)

//Method   string
//Filename string
//NReduce  int
//MapId    int
//ReduceId int
func TestDoMap(reply *mr.WorkerReply) {
	err := mr.DoMap(Map, reply)
	if err != nil {
		fmt.Printf("DoMap test err %v", err)
	}
}

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
