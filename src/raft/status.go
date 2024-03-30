package raft

import (
	"sync/atomic"
	"time"
)

const (
	Leader    = 1
	Follower  = 2
	Candidate = 3
)

func (rf *Raft) BecomeLeaderL() {
	DPrintf("server[%d] become leader on term %d", rf.me, rf.currentTerm)
	atomic.StoreInt32(&rf.state, Leader)
	rf.lastReceive = time.Now()

	index := rf.log.LastIndex()
	for i, _ := range rf.peers {
		rf.nextIndex[i] = index + 1
		rf.matchIndex[i] = -1
	}

	//send no-op log at its currentTerm to other server
	rf.rollBackIndex = rf.log.Append(nil, rf.currentTerm)
}

func (rf *Raft) BecomeFollowerL(newTerm int) {
	atomic.StoreInt32(&rf.state, Follower)
	rf.currentTerm = newTerm
	rf.voteFor = -1
	rf.persist(true, false)
	DPrintf("server[%d] become follower on term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) BecomeCandidateL(preVote bool) {
	atomic.StoreInt32(&rf.state, Candidate)
	if !preVote {
		rf.voteFor = rf.me
		rf.currentTerm++
		rf.persist(true, false)
	}
	DPrintf("server[%d] become candidate on term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32(&rf.state) == Leader
}

func (rf *Raft) isFollower() bool {
	return atomic.LoadInt32(&rf.state) == Follower
}

func (rf *Raft) isCandidate() bool {
	return atomic.LoadInt32(&rf.state) == Candidate
}
