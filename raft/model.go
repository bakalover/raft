package raft

import (
	"fmt"

	"github.com/bakalover/raft/machine"
	"github.com/bakalover/raft/persistence"
)

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
		Entries      persistence.LogEntryPack
		LeaderCommit uint64
	}
	AppendEntriesReply struct {
		Success       bool
		Term          uint64
		NextIndexHint uint64
	}

	RetryableError struct {
		reason string
	}
)

func (r RetryableError) Error() string {
	return fmt.Sprintf("Retry me: [%s]", r.reason)
}
