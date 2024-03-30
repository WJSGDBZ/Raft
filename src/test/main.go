package main

import (
	"fmt"
	"mit6.824/mr"
)

func main() {
	//t := make(chan bool)
	//go func() {
	//	to := time.After(11 * time.Second)
	//	<-to
	//	t <- true
	//}()
	//
	//timeout := time.Tick(10 * time.Second)
	//for {
	//	select {
	//	case <-timeout:
	//		fmt.Printf("timeout!!!\n")
	//		return
	//	case <-t:
	//		fmt.Printf("finish\n")
	//		return
	//	}
	//}

	//wg := sync.WaitGroup{}
	//wg.Add(1)
	//
	//go func() {
	//	defer wg.Done()
	//	for i := 0; i < 1000; i++ {
	//		if i == 100 {
	//			os.Exit(-1)
	//		}
	//	}
	//}()
	//
	//wg.Wait()
	//
	//fmt.Printf("I am here!!\n")
	//reply := TestCoordinate()

	//testMapReduce(&reply)
}

func testMapReduce(reply *mr.WorkerReply) {
	TestDoMap(reply)

	//wg := sync.WaitGroup{}
	//for i := 0; i < reply.NReduce; i++ {
	//	wg.Add(1)
	//	go func(i int) {
	//		defer wg.Done()
	//		TestDoReduce(i, reply.NReduce)
	//	}(i)
	//}
	//
	//wg.Wait()
}

func TestCoordinate() mr.WorkerReply {
	args := mr.ReadyArg{
		Method: mr.Map,
		Id:     1,
	}

	reply := mr.WorkerReply{}

	ok := CallCoordinate("Coordinator.Ready", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		panic("call failed!\n")
	}

	return reply
}
