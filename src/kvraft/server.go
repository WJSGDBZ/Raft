package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"mit6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opname string
	Method int
	Key    string
	Value  []byte
	TaskId int
	TID    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine *Database
	signal       map[string]chan struct{} // wake specify goroutine which has been done
	persister    *raft.Persister

	waitSnapshotIndex int
	commitIndex       int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("server[%d] get GetL request", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.rf.ReadOnly(kv.commitIndex) { // Do not need to replicate
		reply.Value = kv.stateMachine.GetReplicaL(args.Key)
		if reply.Value == nil {
			reply.Err = ErrNoKey
		}
		return
	}

	reply.Err = OK
	tid := fmt.Sprintf("%d_%d_%d", args.CID, GET, args.TaskId)
	op := Op{
		TID:    tid,
		Method: GET,
		Key:    args.Key,
	}

	if _, _, isleader := kv.rf.Start(op); !isleader {
		reply.Err = ErrWrongLeader
		DPrintf("server[%d]: I am not leader to client[%d]", kv.me, args.CID)
	} else {
		wait := make(chan struct{}, 1)
		kv.signal[tid] = wait
		kv.mu.Unlock()

		timeout := time.After(2 * time.Second)
		select {
		case <-wait:
			kv.mu.Lock()
			reply.Value = kv.stateMachine.GetReplicaL(op.Key)
			if reply.Value == nil {
				reply.Err = ErrNoKey
			}
			delete(kv.signal, tid)
			DPrintf("server[%d]: client[%d] request %v done", kv.me, args.CID, "GetL")
		case <-timeout:
			kv.mu.Lock()
			delete(kv.signal, tid)
			reply.Err = ErrTimeOut
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = OK
	opname := fmt.Sprintf("%d_%d", args.CID, args.Op)
	if kv.stateMachine.RepeatCommandL(opname, args.TaskId) { // task have done
		DPrintf("server[%d] request %v done", kv.me, args.Op)
		return
	}

	tid := fmt.Sprintf("%d_%d_%d", args.CID, args.Op, args.TaskId)
	op := Op{
		Opname: opname,
		Method: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		TaskId: args.TaskId,
		TID:    tid,
	}
	if _, _, isleader := kv.rf.Start(op); !isleader {
		DPrintf("server[%d]: I am not leader to client[%d]", kv.me, args.CID)
		reply.Err = ErrWrongLeader
	} else {
		wait := make(chan struct{}, 1)
		kv.signal[tid] = wait
		kv.mu.Unlock()

		timeout := time.After(2 * time.Second)
		select {
		case <-wait:
			DPrintf("server[%d]: client[%d] request %v done", kv.me, args.CID, args.Op)
			kv.mu.Lock()
			delete(kv.signal, tid)
		case <-timeout:
			reply.Err = ErrTimeOut
			kv.mu.Lock()
			delete(kv.signal, tid)
		}
	}
}

func (kv *KVServer) executor() {
	//count := 0
	for kv.killed() == false {
		ac := <-kv.applyCh
		kv.mu.Lock()
		if ac.CommandValid && ac.Command != nil {
			op := ac.Command.(Op)
			signal, ok := kv.signal[op.TID]
			if op.Method != GET && kv.stateMachine.RepeatCommandL(op.Opname, op.TaskId) { // repeat task
				kv.mu.Unlock()
				DPrintf("server[%d] RepeatCommandL %s", kv.me, op.Opname)
				if ok {
					signal <- struct{}{}
				}
				continue
			}

			switch op.Method {
			case GET:
			case PUT:
				kv.stateMachine.PutL(op.Key, op.Value, op.Opname, op.TaskId)
			case APPEND:
				kv.stateMachine.AppendL(op.Key, op.Value, op.Opname, op.TaskId)
			}

			kv.commitIndex = ac.CommandIndex
			if kv.maxraftstate != -1 && kv.persister.RaftLogStateSize() >= kv.maxraftstate { // snapshot
				kv.snapshotL(ac.CommandIndex)
			}

			kv.mu.Unlock()

			if ok {
				signal <- struct{}{} //leader
			}
		} else if ac.SnapshotValid {
			if ac.SnapshotIndex > kv.waitSnapshotIndex { // log is too short wait for leader snapshot
				kv.setSnapShotL(ac.Snapshot)
				kv.waitSnapshotIndex = ac.SnapshotIndex
				DPrintf("server[%d] snapshot %d done", kv.me, ac.SnapshotIndex)
			}

			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) snapshotL(index int) {
	DPrintf("server[%d] start to snapshot %d", kv.me, index)
	kv.waitSnapshotIndex = index
	kv.rf.Snapshot(index, kv.getSnapShotL())
}

func (kv *KVServer) setSnapShotL(data []byte) {
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)

	err := decoder.Decode(&kv.stateMachine)
	if err != nil {
		log.Fatalf("fail to set snapshot %v", err)
	}
}

func (kv *KVServer) getSnapShotL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.stateMachine)
	if err != nil {
		log.Fatalf("fail to encode snapshot %v", err)
	}

	return w.Bytes()
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.signal = make(map[string]chan struct{})
	kv.persister = persister
	if persister.SnapshotSize() > 0 {
		kv.setSnapShotL(persister.ReadSnapshot())
	} else {
		kv.stateMachine = NewDatabase()
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waitSnapshotIndex = 0
	kv.commitIndex = 0
	// You may need initialization code here.
	go kv.executor()
	return kv
}
