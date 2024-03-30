package raft

import (
	"math/rand"
	"sync"
	"time"
)

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type VoteType uint64

const (
	VoteOther VoteType = iota
	ReceiveOldLog
)

const (
	Normal int = iota
	Fatal
)

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VType       VoteType
	VoteGranted bool
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	done := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		done <- ok
	}()

	timeout := time.After(RPCTimeOut * time.Millisecond)

	select {
	case ok := <-done:
		return ok
	case <-timeout:
		return false
	}

	//return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) startElection() {
	for stage := 1; stage <= 2; stage++ { // two phase election
		count := 1
		finish := 1
		mu := sync.Mutex{}
		cond := sync.NewCond(&mu)
		preVote := true
		if stage == 2 {
			preVote = false
		}
		rf.BecomeCandidateL(preVote)
		for i, _ := range rf.peers {
			if i != rf.me {
				go func(s int) {
					vote := rf.requestVote(s, preVote)
					mu.Lock()
					defer mu.Unlock()
					if vote {
						count++
					}
					finish++
					cond.Broadcast()
				}(i)
			}
		}

		mu.Lock()
		l := len(rf.peers)
		for count <= l/2 && finish != l && rf.isCandidate() {
			cond.Wait()
		}

		mu.Unlock()
		if count <= l/2 {
			return
		}

		if !preVote {
			rf.mu.Lock()
			rf.BecomeLeaderL()
			rf.mu.Unlock()

			go rf.tickHeartBeatsOrAppendEntry()
		}

	}
}

func (rf *Raft) requestVote(server int, preVote bool) bool {
	rf.mu.Lock()
	if rf.isCandidate() {
		lastIndex := rf.log.LastIndex()
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  rf.log.GetTerm(lastIndex),
			PreVote:      preVote,
		}
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		if ok := rf.sendRequestVote(server, &args, &reply); ok {
			rf.mu.Lock()
			if args.Term < rf.currentTerm { // Old RPC
				rf.mu.Unlock()
				return false
			}
			if reply.Term > rf.currentTerm || reply.VType == ReceiveOldLog { // Old Log \ Term
				rf.BecomeFollowerL(reply.Term)
			}
			rf.mu.Unlock()
			//DPrintf("server[%d] sendRequestVote for server[%d]", rf.me, i)
			return reply.VoteGranted
		}
	} else {
		rf.mu.Unlock()
	}
	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("server[%d] receive requestVote\n", rf.me)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // refuse
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.BecomeFollowerL(args.Term)
		//reply.Term = rf.currentTerm
	}

	if rf.voteFor == -1 || args.PreVote { // first come first server or preVote
		// Compare whose logs are updated
		lastIndex, lastTerm := rf.log.GetLastEntryState()
		if (lastTerm < args.LastLogTerm) ||
			(lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.lastReceive = time.Now()
			if !args.PreVote { // True Vote
				rf.resetElectionTimeoutL()
				rf.voteFor = args.CandidateId
				rf.persist(true, false)
				//DPrintf("server[%d] vote for [%d] on term %d\n", rf.me, args.CandidateId, rf.currentTerm)
			}

			return
		} else {
			reply.VType = ReceiveOldLog
		}
	} else {
		reply.VType = VoteOther
	}

	reply.VoteGranted = false
}

func (rf *Raft) resetElectionTimeoutL() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = BaseElectionTime + rand.Int31()%ElectionInterval
}
