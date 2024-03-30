package raft

const (
	// HeartBeats per millisecond
	HeartBeats = 100
	RPCTimeOut = 100
	// BaseElectionTime per millisecond
	BaseElectionTime = 300
	ElectionInterval = 600
	LeaderLease      = BaseElectionTime / 0.8
)
