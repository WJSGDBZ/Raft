package raft

import "time"

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	done := make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
		done <- ok
	}()

	timeout := time.After(RPCTimeOut * time.Millisecond)

	select {
	case ok := <-done:
		return ok
	case <-timeout:
		return false
	}

	// return rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { //refuse
		return
	}

	lastIncludeIndex, lastIncludeTerm := rf.log.ReadSnapShotState()
	if args.Term < lastIncludeTerm ||
		(args.Term == lastIncludeTerm && args.LastIncludedIndex <= lastIncludeIndex) { //old Snapshot
		return
	}

	DPrintf("server[%d] receive snapshot %d", rf.me, args.LastIncludedIndex)
	rf.log.SaveSnapShotState(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.log.clear()
	rf.persister.SaveStateAndSnapshot(rf.getRaftvarStatte(), rf.getRaftlogStatte(), args.Data)

	rf.waitingSnapshot = args.Data
	rf.waitingIndex = args.LastIncludedIndex
	rf.waitingTerm = args.LastIncludedTerm

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.applyCond.Broadcast()
}
