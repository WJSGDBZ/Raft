package shardctrler


import "mit6.824/raft"
import "mit6.824/labrpc"
import "sync"
import "mit6.824/labgob"
import (
    "crypto/sha256"
    "encoding/binary"
    "sort"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
}


type Op struct {
}


func findNodeByHash(ring HashRing, hash int) bool {

    return ring[hash].Hash == hash
}

func findNodeByHashAndGid(ring HashRing, hash int, gid int) bool {

    return ring[hash].Hash == hash && ring[hash].GID == gid;
}

func adjustHash(hash int) int {

    return hash + NShards/10
}

func hashFunction(key int) int {
    keyBytes := make([]byte, 4)
    binary.BigEndian.PutUint32(keyBytes, uint32(key))
    
    hashBytes := sha256.Sum256(keyBytes)
    
    return int(binary.BigEndian.Uint32(hashBytes[:4]) % NShards)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
    sc.mu.Lock()
    defer sc.mu.Unlock()

    newConfig := Config{
        Num:    len(sc.configs),
        Groups: make(map[int][]string),
        HashRing:   make(HashRing, NShards),
    }

    // copy pre-config
    if len(sc.configs) > 0 {
        lastConfig := sc.configs[len(sc.configs)-1]
        for gid, servers := range lastConfig.Groups {
            newConfig.Groups[gid] = servers
        }
        newConfig.HashRing = make(HashRing, len(lastConfig.HashRing))
        copy(newConfig.HashRing, lastConfig.HashRing)
    }

    for gid, servers := range args.Servers {
        newConfig.Groups[gid] = servers 
        gidHash := hashFunction(gid)
        for {
            if exists := findNodeByHash(newConfig.HashRing, gidHash); !exists {
                newConfig.HashRing = append(newConfig.HashRing, HashNode{Hash: gidHash, GID: gid})
                break
            }
            // fix hash conflict
            gidHash = adjustHash(gidHash)
        }
    }

    sort.Slice(newConfig.HashRing, func(i, j int) bool {
        return newConfig.HashRing[i].Hash < newConfig.HashRing[j].Hash
    })

    sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
    sc.mu.Lock()
    defer sc.mu.Unlock()

    newConfig := Config{
        Num:    len(sc.configs),
        Groups: make(map[int][]string),
        HashRing:   make(HashRing, NShards),
    }

    // copy pre-config
    if len(sc.configs) > 0 {
        lastConfig := sc.configs[len(sc.configs)-1]
        for gid, servers := range lastConfig.Groups {
            newConfig.Groups[gid] = servers
        }

        newConfig.HashRing = make(HashRing, len(lastConfig.HashRing))
        copy(newConfig.HashRing, lastConfig.HashRing)
    }

    for _, gid := range args.GIDs {
        delete(newConfig.Groups, gid)

		// update hashRing
		gidHash := hashFunction(gid)
		for {
            if exists := findNodeByHashAndGid(newConfig.HashRing, gidHash, gid); exists {
				newConfig.HashRing[gidHash].Hash = 0
				newConfig.HashRing[gidHash].GID = 0
                break
            }

            // fix hash conflict
            gidHash = adjustHash(gidHash)
        }
    }

    sc.configs = append(sc.configs, newConfig)
}

// type MoveArgs struct {
// 	Shard int
// 	GID   int
// }
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

}

// type QueryArgs struct {
// 	Num int // desired config number
// }
// type QueryReply struct {
// 	WrongLeader bool
// 	Err         Err
// 	Config      Config
// }
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	length := len(sc.configs)
    config := Config{}
    if args.Num == -1 || args.Num >= length {
        config = sc.configs[length-1]
    } else {
        config = sc.configs[args.Num]
    }

	newGroups := deepCopyMap(config.Groups)
    newConfig := Config{
        Num:    config.Num,
        HashRing: config.HashRing,
        Groups: newGroups,
    }

	reply.Config = newConfig
}

func deepCopyMap(originalMap map[int][]string) map[int][]string {
    newMap := make(map[int][]string)

    for key, valueSlice := range originalMap {
        newValueSlice := make([]string, len(valueSlice))
        copy(newValueSlice, valueSlice) 

        newMap[key] = newValueSlice
    }

    return newMap
}


func (sc *ShardCtrler) findResponsibleShard(gid int, ring HashRing) []int {
	shards := []int{}
	newConfig := sc.configs[len(sc.configs)-1]

    gidHash := hashFunction(gid)
	for {
		if exists := findNodeByHashAndGid(newConfig.HashRing, gidHash, gid); exists {
			break
		}

		// fix hash conflict
		gidHash = adjustHash(gidHash)
	}
	
	shards = append(shards, gidHash)
	gidHash++
	for exists := findNodeByHashAndGid(newConfig.HashRing, gidHash, gid); !exists ; gidHash++ {
		shards = append(shards, gidHash)
	}

    return shards
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	return sc
}
