package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"mit6.824/labgob"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"mit6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// all servers
	state           int32
	lastReceive     time.Time
	lastSend        time.Time
	electionTimeout int32
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	ready           chan struct{}
	//persist
	currentTerm       int
	log               Log
	voteFor           int
	rollBackIndex     int // To judge ReadOnly
	realSnapshotIndex int // To prevent update Snapshot and outdate log
	// leader
	nextIndex  []int // leader should send [nextIndex: lastIndex] log to followers
	matchIndex []int // index of the highest log entry known to be replicated on server
	// follower
	lastApplied int // highest index that has been committed to state machine
	commitIndex int // highest index that could be committed to state machine
	leaderId    int // indicate who is leader
	//snapshot state
	waitingSnapshot []byte
	waitingIndex    int
	waitingTerm     int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.isLeader()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(savevar bool, savelog bool) {
	// Your code here (2C).
	if savevar {
		vstate := rf.getRaftvarStatte()
		rf.persister.SaveRaftState(vstate, nil)
	}

	if savelog {
		logstate := rf.getRaftlogStatte()
		rf.persister.SaveRaftState(nil, logstate)
	}

	//DPrintf("server[%d] persist", rf.me)
}

func (rf *Raft) getRaftlogStatte() []byte {
	log := new(bytes.Buffer)
	l := labgob.NewEncoder(log)
	l.Encode(rf.log)

	logstate := log.Bytes()

	return logstate
}

func (rf *Raft) getRaftvarStatte() []byte {
	variable := new(bytes.Buffer)
	v := labgob.NewEncoder(variable)
	v.Encode(rf.voteFor)
	v.Encode(rf.currentTerm)

	vstate := variable.Bytes()
	return vstate
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(vstate []byte, logstate []byte) {
	// Your code here (2C).
	if vstate != nil && len(vstate) > 0 { // bootstrap without any state?
		vr := bytes.NewBuffer(vstate)
		vd := labgob.NewDecoder(vr)

		err := vd.Decode(&rf.voteFor)
		if err != nil {
			log.Fatalf("can't read voteFor %v\n", err)
		}

		err = vd.Decode(&rf.currentTerm)
		if err != nil {
			log.Fatalf("fail to read currentTerm state")
		}

		DPrintf("server[%d] read raft statedata voteFor = %d currentTerm = %d",
			rf.me, rf.voteFor, rf.currentTerm)
	}

	if logstate != nil && len(logstate) > 0 {
		lr := bytes.NewBuffer(logstate)
		ld := labgob.NewDecoder(lr)

		err := ld.Decode(&rf.log)
		if err != nil {
			log.Fatalf("fail to read log state")
		}

		DPrintf("server[%d] read raft statedata log = %v",
			rf.me, rf.log)
	}

	//rf.waitingSnapshot = rf.persister.ReadSnapshot()
	//if len(rf.waitingSnapshot) != 0 {
	//	rf.waitingIndex, _ = rf.log.ReadSnapShotState()
	//}
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//if rf.commitIndex > lastIncludedIndex {
	//	return false
	//}
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("server[%d] before snapshot %d entries = %v", rf.me, index, rf.log.Entries)
	if ok := rf.log.StartSnapShot(index); ok {
		DPrintf("server[%d] after snapshot %d entries = %v", rf.me, index, rf.log.Entries)
		rf.persister.SaveStateAndSnapshot(rf.getRaftvarStatte(), rf.getRaftlogStatte(), snapshot)

		rf.waitingSnapshot = snapshot
		rf.waitingIndex = index
		rf.waitingTerm = rf.log.GetTerm(index)
		rf.lastApplied = index

		rf.applyCond.Broadcast()
	}
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isLeader() {
		isLeader = true
		term = rf.currentTerm
		index = rf.log.Append(command, term)
		DPrintf("server[%d] get client request log %d %v on term %d", rf.me, index, command, rf.currentTerm)

		if len(rf.ready) == 0 {
			rf.ready <- struct{}{}
		}
	}

	return index, term, isLeader
}

// if leader is real leader and has already roll back entry, then could excute read operate without polling
func (rf *Raft) ReadOnly(smCommitIndex int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rtt := rf.lastReceive.Sub(rf.lastSend)
	safeTime := time.Duration(LeaderLease)*time.Millisecond - rtt
	//log.Printf("smCommitIndex = %d, rollBackIndex = %d", smCommitIndex, rf.rollBackIndex)
	if safeTime < 0 || rf.rollBackIndex <= smCommitIndex {
		return false
	}

	//log.Printf("read-only is %v", rf.lastReceive.Add(safeTime).After(time.Now()))
	if rf.isLeader() && rf.lastReceive.Add(safeTime).After(time.Now()) {
		return true
	}

	return false
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(20 * time.Millisecond)

		rf.mu.Lock()
		now := time.Now()
		if rf.lastReceive.
			Add(time.Duration(rf.electionTimeout) * time.Millisecond).
			Before(now) { // timeout!!!
			if !rf.isLeader() {
				//DPrintf("server[%d] startElection on term %d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				rf.startElection()
				rf.mu.Lock()

				rf.lastReceive = time.Now()
				rf.resetElectionTimeoutL()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied, _ = rf.log.ReadSnapShotState()

	for rf.killed() == false {
		lastIncludeIndex, _ := rf.log.ReadSnapShotState()
		if rf.waitingSnapshot != nil && rf.waitingIndex >= lastIncludeIndex {
			DPrintf("server[%d] ready apply snapshot %d", rf.me, rf.waitingIndex)
			apply := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotIndex: rf.waitingIndex,
				SnapshotTerm:  rf.waitingTerm,
			}
			rf.waitingSnapshot = nil

			rf.mu.Unlock()
			rf.applyCh <- apply
			rf.mu.Lock()
			DPrintf("server[%d] apply snapshot %d", rf.me, rf.waitingIndex)
		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.log.LastIndex() &&
			rf.lastApplied+1 >= lastIncludeIndex+1 {
			DPrintf("server[%d] ready apply log %d %v", rf.me, rf.lastApplied, rf.log.Get(rf.lastApplied).Command)
			rf.lastApplied += 1
			apply := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log.Get(rf.lastApplied).Command,
			}

			rf.mu.Unlock()
			rf.applyCh <- apply
			rf.mu.Lock()
			DPrintf("server[%d] apply log %d %v", rf.me, apply.CommandIndex, apply.Command)
		} else {
			//DPrintf("server[%d] wait")
			rf.applyCond.Wait()
		}
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	rf.ready = make(chan struct{}, 1)
	rf.state = Follower
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.leaderId = -1
	if rf.persister.RaftLogStateSize() < 1 {
		rf.log = *NewLog()
	}

	if rf.persister.RaftVarStateSize() < 1 {
		rf.voteFor = -1
		rf.currentTerm = 0
	}

	rf.resetElectionTimeoutL()
	// initialize from state persisted before a crash
	vstate, logstate := persister.ReadRaftState()
	rf.readPersist(vstate, logstate)
	go rf.Applier()
	// start ticker goroutine to start elections
	rf.lastReceive = time.Now()
	go rf.ticker()
	return rf
}
