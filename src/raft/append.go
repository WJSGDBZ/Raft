package raft

import (
	"math"
	"sync"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	ConflictLogTerm  int
	ConflictLogIndex int
	LogLen           int
	Success          bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	done := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		done <- ok
	}()

	timeout := time.After(RPCTimeOut * time.Millisecond)

	select {
	case ok := <-done:
		return ok
	case <-timeout:
		return false
	}

	// return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) tickHeartBeatsOrAppendEntry() {
	ticker := time.NewTicker(time.Duration(HeartBeats) * time.Millisecond)
	defer ticker.Stop()

	for rf.killed() == false && rf.isLeader() {
		rf.mu.Lock()
		lastIndex := rf.log.LastIndex()
		rf.lastSend = time.Now()
		//log.Printf("heartbeat")
		if lastIndex > rf.commitIndex {
			rf.persist(false, true) // persist raft state before replication
			rf.mu.Unlock()
			rf.sendEntries(lastIndex)
		} else {
			rf.mu.Unlock()
			rf.sendEntries(lastIndex) // send heartbeat

			select {
			case <-rf.ready:
			case <-ticker.C:
			}
		}
	}
}

func (rf *Raft) sendEntries(lastIndex int) {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	finish := 1
	count := 1

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(s int) {
				defer func() {
					mu.Lock()
					finish++
					mu.Unlock()
				}()
				rf.mu.Lock()
				if rf.isLeader() {
					lastIncludeIndex, _ := rf.log.ReadSnapShotState()
					// InstallSnapShot
					if rf.nextIndex[s] <= lastIncludeIndex { //follower if too later
						ok := rf.sendSnapShotL(s, lastIncludeIndex)
						if !ok {
							rf.mu.Unlock()
							return
						}
					}
					// AppendEntries
					realLastIncludeIndex, _ := rf.log.ReadSnapShotState() // To avoid Raft snapshot again after send snapshot
					if realLastIncludeIndex >= rf.nextIndex[s] {
						rf.mu.Unlock()
						return
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[s] - 1,
						PrevLogTerm:  rf.log.GetTerm(rf.nextIndex[s] - 1),
						LeaderCommit: rf.commitIndex,
					}

					if rf.nextIndex[s] <= lastIndex {
						args.Entries = append(args.Entries, rf.log.SliceCopy(rf.nextIndex[s], lastIndex+1)...)
					}
					//DPrintf("server[%d] send to server[%d] entry = %v", rf.me, s, args.Entries)
					lastSendIndex := rf.nextIndex[s] + len(args.Entries) - 1
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(s, &args, &reply)
					if ok {
						if succ := rf.processAppendReply(s, lastSendIndex, &args, &reply); succ {
							mu.Lock()
							count++
							mu.Unlock()
						}
					}
				} else {
					rf.mu.Unlock()
				}
				cond.Broadcast()
			}(i)
		}
	}

	l := len(rf.peers)
	mu.Lock()
	defer mu.Unlock()
	for count <= l/2 && finish != l && rf.isLeader() {
		cond.Wait()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if count > l/2 {
		if rf.commitIndex < lastIndex {
			n := 1
			for _, mi := range rf.matchIndex {
				if mi == lastIndex {
					n++
				}
			}
			if n > l/2 {
				// only commit current term log
				if rf.log.GetTerm(lastIndex) == rf.currentTerm {
					rf.commitIndex = lastIndex
					rf.applyCond.Broadcast()
				}
			}
		}
	}
}

func (rf *Raft) sendSnapShotL(s int, lastIncludeIndex int) bool {
	args := InstallSnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	reply := InstallSnapShotReply{}
	DPrintf("server[%d] send snapshot %d to server[%d]", rf.me, rf.log.LastIncludedIndex, s)
	rf.mu.Unlock()
	ok := rf.sendInstallSnapShot(s, &args, &reply)
	rf.mu.Lock()
	if ok {
		if args.Term < rf.currentTerm { // Old RPC
			return false
		}
		rf.nextIndex[s] = lastIncludeIndex + 1
		rf.matchIndex[s] = lastIncludeIndex
	} else {
		if reply.Term > rf.currentTerm {
			rf.BecomeFollowerL(reply.Term)
		}
		return false
	}

	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIncludeIndex, _ := rf.log.ReadSnapShotState()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.PrevLogIndex < lastIncludeIndex { //refuse
		reply.Success = false
		return
	}

	rf.leaderId = args.LeaderId
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && !rf.isFollower()) {
		rf.BecomeFollowerL(args.Term)
		rf.leaderId = args.LeaderId
		reply.Term = rf.currentTerm
	}

	rf.lastReceive = time.Now()
	rf.resetElectionTimeoutL()
	if rf.log.IsExitEntry(args.PrevLogIndex, args.PrevLogTerm) {
		// replicate log
		if len(args.Entries) != 0 {
			if ok := rf.log.Put(args.PrevLogIndex+1, args.Entries); ok {
				rf.persist(false, true)
				DPrintf("server[%d] put log form %d to %d", rf.me, args.PrevLogIndex, rf.log.LastIndex())
			}
		}
	} else {
		reply.ConflictLogIndex = lastIncludeIndex + 1
		if args.PrevLogIndex > rf.log.LastIndex() {
			reply.ConflictLogIndex = rf.log.LastIndex() + 1
		} else {
			reply.ConflictLogTerm = rf.log.GetTerm(args.PrevLogIndex)
			for index := lastIncludeIndex + 1; index <= args.PrevLogIndex; index++ { //找到冲突term的首次出现位置
				if rf.log.Get(index).Term == reply.ConflictLogTerm {
					reply.ConflictLogIndex = index
					break
				}
			}
		}

		reply.Success = false
		DPrintf("server[%d] sync log commitIndex = %d", rf.me, rf.commitIndex)
		return
	}

	LeaderCommit := int(math.Min(float64(args.PrevLogIndex), float64(args.LeaderCommit))) // To avoid follower commit early
	if LeaderCommit > rf.commitIndex {                                                    // Commit entry to state machine
		rf.commitIndex = int(math.Min(float64(LeaderCommit), float64(rf.log.LastIndex())))
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) processAppendReply(server int, lastSendIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.BecomeFollowerL(reply.Term)
		return false
	}

	if args.Term < rf.currentTerm { // Old RPC
		return false
	}

	// sync log from leader to server
	if !reply.Success {
		rf.nextIndex[server] = reply.ConflictLogIndex
		DPrintf("server[%d] processAppendReply ConflictLogIndex nextIndex[%d] change to %d %+v", rf.me, server, rf.nextIndex[server], *reply)
	} else {
		// update next index
		rf.nextIndex[server] = lastSendIndex + 1
		rf.matchIndex[server] = lastSendIndex
	}

	return reply.Success
}
