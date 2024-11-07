package raft

import "github.com/bakalover/raft/machine"

type (
	RaftReply struct {
		Error    error               `json:"error,omitempty"`
		Response machine.MachineType `json:"reply,omitempty"`
	}

	RequestVoteArgs struct {
		Term      uint64
		Candidate string
		LastTerm  uint64
		LastIndex uint64
	}

	RequestVoteReply struct {
		Granted bool   
		Term    uint64 
	}

	AppendEntriesArgs struct {
		Term         uint64
		Leader       string
		PrevTerm     uint64
		PrevIndex    uint64
		Entries      []machine.RSMcmd
		LeaderCommit uint64
	}
	AppendEntriesReply struct {
		Success       bool
		Term          uint64
		NextIndexHint uint64
	}
)
