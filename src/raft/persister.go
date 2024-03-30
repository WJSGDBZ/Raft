package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu           sync.Mutex
	raftlogstate []byte
	raftvarstate []byte
	snapshot     []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftlogstate = ps.raftlogstate
	np.raftvarstate = ps.raftvarstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(varstate, logstate []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if varstate != nil {
		ps.raftvarstate = clone(varstate)
	}

	if logstate != nil {
		ps.raftlogstate = clone(logstate)
	}
}

func (ps *Persister) ReadRaftState() (varstate []byte, logstate []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftvarstate), clone(ps.raftlogstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftvarstate) + len(ps.raftlogstate)
}

func (ps *Persister) RaftVarStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftvarstate)
}

func (ps *Persister) RaftLogStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftlogstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(varstate []byte, logstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = clone(snapshot)
	ps.raftvarstate = clone(varstate)
	ps.raftlogstate = clone(logstate)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
