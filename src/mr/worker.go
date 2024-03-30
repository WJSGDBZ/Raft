package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)
import "net/rpc"
import "hash/fnv"

//The workers will talk to the coordinator via RPC.
//Each worker process will ask the coordinator for a task,
//read the task's input from one or more files, execute the task,
//and write the task's output to one or more files

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	lastMethod := Ready
	lastId := -1
	// Your worker implementation here.
	for {
		args := ReadyArg{
			Method: lastMethod,
			Id:     lastId,
		}

		reply := WorkerReply{}

		ok := call("Coordinator.Ready", &args, &reply)
		if ok {
			//fmt.Printf("reply %v\n", reply)
		} else {
			//fmt.Printf("call failed!\n")
			time.Sleep(time.Second)
			continue
		}

		switch reply.Method {
		case Map:
			err := DoMap(mapf, &reply)
			if err != nil {
				fmt.Printf("doMap err %v", err)
			}
		case Reduce:
			err := DoReduce(reducef, &reply)
			if err != nil {
				fmt.Printf("doReduce err %v", err)
			}
		case Done:
			//fmt.Printf("machine worker exit\n")
			os.Exit(0)
		default:
			fmt.Printf("can't identify method %s", reply.Method)
		}

		lastId = reply.WorkerId
		lastMethod = reply.Method
	}
}

func DoMap(mapf func(string, string) []KeyValue, reply *WorkerReply) error {
	filename := reply.Filename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}
	// read input file and send it to map function
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	intermediatekva := mapf(filename, string(content))
	// open temporary files and encoder for each file
	mapFiles, mapFilesName, err := openTempMapFile(reply.NReduce)
	defer closeFile(mapFiles)
	if err != nil {
		return err
	}
	encoders := createEncoder(mapFiles)

	for _, kv := range intermediatekva {
		reduceId := ihash(kv.Key) % reply.NReduce
		encoders[reduceId].Encode(&kv)
		//fmt.Fprintf(mapFiles[reduceId], "%v %v\n", kv.Key, kv.Value)
	}

	// atomically rename temp file to final intermediate files
	for i := 0; i < reply.NReduce; i++ {
		atomicRenameMapfiles(mapFilesName[i], reply.MapId, i)
	}

	return nil
}

func atomicRenameMapfiles(oldpath string, taskId int, number int) {
	newpath := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(number)
	dir, err := os.Getwd()
	err = os.Rename(oldpath, dir+"/"+newpath)
	if err != nil {
		log.Printf("fail to rename file %s %v", oldpath, err)
		return
	}
}

func openTempMapFile(nReduce int) ([]*os.File, []string, error) {
	var tempfiles []*os.File
	var tempfilesname []string
	dir, _ := os.Getwd()
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile(dir, "")
		if err != nil {
			return nil, nil, err
		}

		tempfilesname = append(tempfilesname, tempFile.Name())
		tempfiles = append(tempfiles, tempFile)
	}

	return tempfiles, tempfilesname, nil
}

func createEncoder(files []*os.File) []*json.Encoder {
	var encoders []*json.Encoder
	for _, f := range files {
		enc := json.NewEncoder(f)
		encoders = append(encoders, enc)
	}

	return encoders
}

func closeFile(files []*os.File) error {
	for _, f := range files {
		f.Close()
	}

	return nil
}

func DoReduce(reducef func(string, []string) string, reply *WorkerReply) error {
	reduceFiles, err := openReduceFile(reply.NReduce, reply.ReduceId)
	defer closeFile(reduceFiles)
	if err != nil {
		return err
	}

	kvm := make(map[string][]string)
	for _, f := range reduceFiles {
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
		}
	}
	dir, _ := os.Getwd()
	tempfile, err := ioutil.TempFile(dir, "")
	defer tempfile.Close()
	if err != nil {
		return err
	}

	for key, values := range kvm {
		output := reducef(key, values)
		fmt.Fprintf(tempfile, "%v %v\n", key, output)
	}

	atomicRenameReducefiles(tempfile.Name(), reply.ReduceId)
	return nil
}

func atomicRenameReducefiles(oldpath string, taskId int) {
	newpath := "mr-out-" + strconv.Itoa(taskId)
	dir, err := os.Getwd()
	err = os.Rename(oldpath, dir+"/"+newpath)
	if err != nil {
		log.Printf("fail to rename file %s %v", oldpath, err)
		return
	}
}

func openReduceFile(nReduce int, reduceId int) ([]*os.File, error) {
	var files []*os.File
	for i := 0; i < nReduce; i++ {
		// mr-X-Y  X is mapId Y is reduceId
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId)
		if ok, _ := FileExist(filename); ok {
			file, err := os.Open(filename)
			if err != nil {
				return nil, err
			}

			files = append(files, file)
		}
	}

	return files, nil
}

func FileExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
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
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
