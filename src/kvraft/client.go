package kvraft

import (
	"mit6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int32
	TaskId   map[int]int
	mu       sync.Mutex
	CID      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.TaskId = make(map[int]int)
	ck.CID = nrand()

	return ck
}

func (ck *Clerk) getTaskId(op int) int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.TaskId[op]++
	return ck.TaskId[op]
}

const (
	NONE   = 0
	GET    = 1
	PUT    = 2
	APPEND = 3
)

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	length := int32(len(ck.servers))
	args := GetArgs{
		Key:    key,
		CID:    ck.CID,
		TaskId: ck.getTaskId(GET),
	}
	IDChange := false

	id := atomic.LoadInt32(&ck.leaderId)
	for {
		reply := GetReply{}
		if ok := ck.ClerkCall(id, "KVServer.Get", &args, &reply); ok {
			switch reply.Err {
			case OK:
				if IDChange {
					atomic.StoreInt32(&ck.leaderId, id)
				}
				return string(reply.Value)
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				time.Sleep(10 * time.Millisecond)
			default:
				panic("UNEXPECT ERR :" + reply.Err)
			}
		}
		id = (id + 1) % length // id disconnect current server then find other server
		IDChange = true
	}

}
func (ck *Clerk) GetOp(op string) int {
	switch op {
	case "Get":
		return GET
	case "Put":
		return PUT
	case "Append":
		return APPEND
	}

	return NONE
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value []byte, op string) {
	// You will have to modify this function.
	//DPrintf("key = %s, value = %s, op = %s", key, value, op)
	length := int32(len(ck.servers))

	operation := ck.GetOp(op)
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     operation,
		CID:    ck.CID,
		TaskId: ck.getTaskId(operation),
	}
	IDChange := false

	id := atomic.LoadInt32(&ck.leaderId)
	for {
		reply := PutAppendReply{}
		DPrintf("client[%d] send PutAppend to server%d %v", ck.CID, id, args)
		if ok := ck.ClerkCall(id, "KVServer.PutAppend", &args, &reply); ok {
			switch reply.Err {
			case OK:
				if IDChange {
					atomic.StoreInt32(&ck.leaderId, id)
				}
				return
			case ErrWrongLeader:
				DPrintf("ErrWrongLeader")
				time.Sleep(10 * time.Millisecond)
			default:
				panic("UNEXPECT ERR :" + reply.Err)
			}
		}

		id = (id + 1) % length // id disconnect current server then find other server
		IDChange = true
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, []byte(value), "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, []byte(value), "Append")
}

func (ck *Clerk) ClerkCall(i int32, MethodName string, args interface{}, reply interface{}) bool {
	done := make(chan bool)

	go func() {
		if ok := ck.servers[i].Call(MethodName, args, reply); ok {
			done <- ok
		}
	}()

	timeout := time.After(100 * time.Millisecond)

	select {
	case ok := <-done:
		return ok
	case <-timeout:
		return false
	}
}
