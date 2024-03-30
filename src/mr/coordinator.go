package mr

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

//timeout :  use ten seconds and give the same task to a different worker.

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	mapJobs     []string
	curMapId    int
	curReduceId int
	freeWorker  chan *WorkerReply
	timer       sync.Map
}

var (
	jobsLength int
	finishTask int
	mu         sync.RWMutex
)

const (
	TIMEOUT = 10 //second
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Ready(args *ReadyArg, reply *WorkerReply) error {
	if args.Id != -1 {
		go func() {
			//fmt.Printf("workerId = %d finish\n", args.Id)
			if t, ok := c.timer.LoadAndDelete(args.Id); ok {
				t.(chan bool) <- true // tell the timeout goroutine task already finish
			}
		}()
	}
	if args.Method == Reduce || args.Method == Map {
		mu.Lock()
		finishTask++
		mu.Unlock()
	}

	r := <-c.freeWorker
	reply.NReduce = r.NReduce
	reply.Method = r.Method
	reply.Filename = r.Filename
	reply.MapId = r.MapId
	reply.ReduceId = r.ReduceId
	reply.WorkerId = r.WorkerId

	return nil
}

func (c *Coordinator) Schedule() {
	workerId := 0
	go func() {
		for {
			reply := WorkerReply{
				NReduce:  c.nReduce,
				WorkerId: workerId,
			}

			if c.isMap() { // if map task has finished then assign reduce task
				task := c.mapJobs[0]
				c.mapJobs = c.mapJobs[1:]
				reply.Method = Map
				reply.MapId = c.curMapId % c.nReduce
				reply.Filename = task

				c.curMapId++
			} else if c.isReduce() {
				reply.ReduceId = c.curReduceId % c.nReduce
				reply.Method = Reduce

				c.curReduceId++
			} else if c.isFinish() { // if all task has been completed then tell worker to exit
				reply.Method = Done

				c.freeWorker <- &reply
				continue
			} else { // if this task is not recognized, skip it
				continue
			}

			t := make(chan bool, 1)
			c.timer.Store(workerId, t)
			c.freeWorker <- &reply
			c.startTimeOut(reply, t) // start a timeout goroutine
			workerId++
		}
	}()
}

func (c *Coordinator) isMap() bool {
	ret := false
	if len(c.mapJobs) > 0 {
		ret = true
	}

	return ret
}

func (c *Coordinator) isReduce() bool {
	ret := false
	mu.RLock()
	defer mu.RUnlock()
	// map task must be completed before reduce task
	if finishTask >= jobsLength && c.curReduceId < c.nReduce {
		ret = true
	}

	return ret
}

func (c *Coordinator) isFinish() bool {
	ret := false
	mu.RLock()
	defer mu.RUnlock()
	if finishTask == c.nReduce+jobsLength {
		ret = true
	}
	return ret
}

func (c *Coordinator) startTimeOut(r WorkerReply, t chan bool) {
	go func() {
		timeout := time.After(TIMEOUT * time.Second)
		for {
			select {
			case <-timeout:
				fmt.Printf("Task TimeOut workerId = %d\n", r.WorkerId)
				c.freeWorker <- &r
				timeout = time.After(TIMEOUT * time.Second)
			case <-t:
				//fmt.Printf("Task Finish workerId = %d\n", r.WorkerId)
				close(t)
				runtime.Goexit()
			}
		}
	}()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.RLock()
	// Your code here.
	if finishTask == c.nReduce+jobsLength {
		mu.RUnlock()
		//fmt.Printf("mapreduce finish!\n")
		time.Sleep(TIMEOUT * time.Second) //wait machine to close
		return true
	}

	mu.RUnlock()
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	freeWorker := make(chan *WorkerReply)
	c := Coordinator{
		nReduce:     nReduce,
		mapJobs:     files,
		curMapId:    0,
		curReduceId: 0,
		freeWorker:  freeWorker,
	}
	jobsLength = len(files)
	finishTask = 0
	//fmt.Printf("mapjobs = %v\n", c.mapJobs)
	c.Schedule()

	c.server()
	return &c
}
