package shardkv

import "mit6.824/labrpc"
import "mit6.824/raft"
import "sync"
import "mit6.824/labgob"

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	prevConfig       shardctrler.Config
    currConfig       shardctrler.Config
    persister        *raft.Persister
    scClerk          *shardctrler.Clerk
    waitChs          map[int]chan CommandResponse
    db               map[int]*Shard
    lastAppliedIndex int
}

type ShardStatus int

const (
    // The group serves and owns the shard.
    Serving ShardStatus = iota
    // The group serves the shard, but does not own the shard yet.
    Pulling
    // The group does not serve and own the partition.
    Invalid
    // The group owns but does not serve the shard.
    Erasing
    // The group own the shard and serve it, but it's waiting for ex-owner to delete it
    Waiting
)

type Shard struct {
    Status       ShardStatus
    KV           map[string]string
    LastSessions map[int]*Session
}

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// }
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    op := Op{
        Opname: "Get",
        Key:    args.Key,
    }

	if _, _, isleader := kv.rf.Start(op); !isleader {
		reply.WrongLeader = true
		return
	}

    kv.mu.Lock()
    ch := make(chan OpResult, 1)
    kv.waitChs[index] = ch
    kv.mu.Unlock()

    select {
    case opResult := <-ch:
        reply.Value = opResult.Value
        reply.Err = opResult.Err
    case <-time.After(500 * time.Millisecond): 
        reply.Err = ErrTimeout
    }

    kv.mu.Lock()
    delete(kv.waitChs, index)
    kv.mu.Unlock()
}

// // PutL or AppendL
// type PutAppendArgs struct {
// 	// You'll have to add definitions here.
// 	Key   string
// 	Value string
// 	Op    string // "PutL" or "AppendL"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    shardID := key2shard(args.Key) 
    kv.mu.Lock()
    if !kv.isShardMatch(shardID) { 
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()

    op := Op{
        Opname: args.Op, 
        Key: args.Key, 
        Value: args.Value,
    }

	if _, _, isleader := kv.rf.Start(op); !isleader {
        reply.WrongLeader = true
        return
	}

    kv.mu.Lock()
    ch := make(chan OpResult, 1)
    kv.waitChs[index] = ch
    kv.mu.Unlock()

    select {
    case opResult := <-ch:
        if opResult.Err != OK {
            reply.Err = opResult.Err /
        } else {
            reply.Err = OK
        }
    case <-time.After(500 * time.Millisecond): 
        reply.Err = ErrTimeout
    }

    kv.mu.Lock()
    delete(kv.waitChs, index)
    kv.mu.Unlock()
}

func (kv *ShardKV) isShardMatch(shardId int) bool {
    return kv.currConfig.Shards[shardId] == kv.gid && (kv.db[shardId].Status == Serving || kv.db[shardId].Status == Waiting)
}

func (kv *ShardKV) fetchConfig() {
    canFetchConf := true
    kv.mu.RLock()
    currConfNum := kv.currConfig.Num
    for shardId, shard := range kv.db {
        if shard.Status != Serving && shard.Status != Invalid {
            canFetchConf = false
            break
        }
    }

    kv.mu.RUnlock()
    if canFetchConf {
        latestConfig := kv.scClerk.Query(currConfNum + 1)
        if latestConfig.Num == currConfNum+1 {
            kv.rf.Start(newRaftLogCommand(ConfChange, latestConfig))
        }
    }
}

func (kv *ShardKV) pullData() {
    kv.mu.RLock()
    groupToShards := kv.getGroupToShards(Pulling)
    currConfNum := kv.currConfig.Num
    wg := sync.WaitGroup{}
    for gid, shards := range groupToShards {
        wg.Add(1)
        servers := kv.prevConfig.Groups[gid]
        go func(servers []string, shards []int, confNum int) {
            defer wg.Done()
            for _, server := range servers {
                shardOwner := kv.make_end(server)
                args := PullDataRequest{
                    ConfNum:  confNum,
                    ShardIds: shards,
                }

                reply := PullDataResponse{}
                if shardOwner.Call("ShardKV.PullData", &args, &reply) && reply.Err == OK {
                    kv.rf.Start(newRaftLogCommand(InsertData, resp))
                    break
                }
            }
        }(servers, shards, currConfNum)
    }

    kv.mu.RUnlock()
    wg.Wait()
}

func (kv *ShardKV) PullData(args *PullDataRequest, reply *PullDataResponse) {
    defer DPrintf("[Group %d][Server %d] reply %s for PULL DATA request %s", kv.gid, kv.me, reply, args)
    DPrintf("[Group %d][Server %d] received a PULL DATA request %s", kv.gid, kv.me, args)
    if _, isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    kv.mu.RLock()
    defer kv.mu.RUnlock()

    if kv.currConfig.Num < args.ConfNum {
        reply.Err = ErrNotReady
        return
    }

    if kv.currConfig.Num > args.ConfNum {
        panic("duplicated pull data request")
    }

    replyShards := make(map[int]*Shard)

    for _, shardId := range args.ShardIds {
        shard := kv.db[shardId]
        replyShards[shardId] = deepCopyShard(shard)
    }

    reply.ConfNum = kv.currConfig.Num
    reply.Shards = replyShards
    reply.Err = OK
}

type EraseDataRequest struct {
    ConfNum  int
    ShardIDs []int
}

type EraseDataResponse struct {
    Err Err
}

func (kv *ShardKV) eraseData() {
    kv.mu.RLock()
    groupToShards := kv.getGroupToShards(Waiting)
    currConfNum := kv.currConfig.Num
    wg := sync.WaitGroup{}
    for gid, shards := range groupToShards {
        wg.Add(1)
        servers := kv.prevConfig.Groups[gid]
        go func(servers []string, shards []int, confNum int) {
            defer wg.Done()
            for _, server := range servers {
                shardOwner := kv.make_end(server)
                args := EraseDataRequest{
                    ConfNum:  confNum,
                    ShardIDs: shards,
                }

                reply := EraseDataResponse{}
                if shardOwner.Call("ShardKV.EraseData", &args, &reply) && reply.Err == OK {
                    kv.rf.Start(newRaftLogCommand(StopWaiting, req))
                    break
                }
            }
        }(servers, shards, currConfNum)
    }

    kv.mu.RUnlock()
    wg.Wait()
}

func (kv *ShardKV) EraseData(req *EraseDataRequest, resp *EraseDataResponse) {
    defer DPrintf("[Group %d][Server %d] resp %s for ERASE DATA request %s", kv.gid, kv.me, resp, req)
    DPrintf("[Group %d][Server %d] received a ERASE DATA request %s", kv.gid, kv.me, req)
    index, _, isLeader := kv.rf.Start(newRaftLogCommand(EraseData, *req))
    if !isLeader {
        resp.Err = ErrWrongLeader
        return
    }

    kv.mu.Lock()
    ch := kv.getWaitCh(index)
    kv.mu.Unlock()

    select {
    case response := <-ch:
        resp.Err = response.Err

    case <-time.NewTimer(500 * time.Millisecond).C:
        resp.Err = ErrTimeout
    }

    go func() {
        kv.mu.Lock()
        kv.removeWaitCh(index)
        kv.mu.Unlock()
    }()
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

type CommandType int

const (
    ClientRequest CommandType = iota
    ConfChange
    InsertData
    EraseData
    StopWaiting
    Empty
)

type RaftLogCommand struct {
    CommandType
    Data interface{}
}

func newRaftLogCommand(commandType CommandType, data interface{}) RaftLogCommand {
    return RaftLogCommand{
        CommandType: commandType,
        Data: data,
    }
}

func (kv *ShardKV) applier() {
    for !kv.killed() {
        select {
        case applyMsg := <-kv.applyCh:
            if applyMsg.CommandValid {
                command := applyMsg.Command.(RaftLogCommand)
                kv.mu.Lock()

                if applyMsg.CommandIndex <= kv.lastAppliedIndex {
                    DPrintf("[Group %d][Server %d] discard out-of-date apply Msg [index %d]", kv.gid, kv.me, applyMsg.CommandIndex)
                    kv.mu.Unlock()
                    continue
                }

                kv.lastAppliedIndex = applyMsg.CommandIndex
                response := &CommandResponse{}
                switch command.CommandType {
                case Empty:
                    DPrintf("[Group %d][Server %d] get empty in apply Msg [index %d]", kv.gid, kv.me, applyMsg.CommandIndex)
                case ConfChange:
                    lastestConf := command.Data.(shardctrler.Config)
                    kv.applyConfChange(lastestConf, applyMsg.CommandIndex)

                case InsertData:
                    resp := command.Data.(PullDataResponse)
                    kv.applyInsertData(resp, applyMsg.CommandIndex)

                case StopWaiting:
                    req := command.Data.(EraseDataRequest)
                    kv.applyStopWaiting(req, applyMsg.CommandIndex)

                case EraseData:
                    req := command.Data.(EraseDataRequest)
                    response = kv.applyEraseData(req, applyMsg.CommandIndex)
                    if currentTerm, isLeader := kv.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
                        ch := kv.getWaitCh(applyMsg.CommandIndex)
                        ch <- *response
                    }

                case ClientRequest:
                    request := command.Data.(CommandRequest)
                    response = kv.applyClientRequest(&request, applyMsg.CommandIndex)
                    if currentTerm, isLeader := kv.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
                        ch := kv.getWaitCh(applyMsg.CommandIndex)
                        ch <- *response
                    }
                }

                if kv.needToSnapshot(applyMsg.RaftStateSize) {
                    DPrintf("[Group %d][Server %d] take a snapshot till [index %d]", kv.gid, kv.me, applyMsg.CommandIndex)
                    kv.takeSnapshot(applyMsg.CommandIndex)
                }

                kv.mu.Unlock()
            } else {
                kv.mu.Lock()
                DPrintf("[Group %d][Server %d] received a snapshot from raft layer [index %d]", kv.gid, kv.me, applyMsg.SnapshotIndex)
                if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
                    kv.applySnapshot(applyMsg.Snapshot)
                    kv.lastAppliedIndex = applyMsg.SnapshotIndex
                }

                kv.mu.Unlock()
            }
        }
    }
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.scClerk = shardctrler.MakeClerk(kv.ctrlers)
    kv.mu = sync.RWMutex{}
    kv.waitChs = make(map[int]chan CommandResponse)
    kv.db = make(map[int]*Shard)
    for i := 0; i < shardctrler.NShards; i++ {
        kv.db[i] = &Shard{
            Status:       Invalid,
            KV:           make(map[string]string),
            LastSessions: make(map[int]*Session),
        }
    }

    kv.lastAppliedIndex = -1
    kv.prevConfig = shardctrler.Config{}
    kv.currConfig = shardctrler.Config{}
    kv.applySnapshot(persister.ReadSnapshot())

    go kv.applier()
    go kv.daemon(kv.fetchConfig)
    go kv.daemon(kv.pullData)
    go kv.daemon(kv.eraseData)
	return kv
}

func (kv *ShardKV) daemon(action func()) {
    for !kv.killed() {
        if _, isLeader := kv.rf.GetState(); isLeader {
            action()
        }

        time.Sleep(50 * time.Millisecond)
    }
}
