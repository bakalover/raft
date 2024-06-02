package node

import (
	"math/rand"
	"time"
)

const (
	NullCanidateId = "nodeNull"
)

type State struct {
	role            *RoleStateMachine
	persistentState *PersistentState
	commitIndex     uint64
	lastApplied     uint64
	nextIndex       []uint64
	matchIndex      []uint64
}

type Node struct {
	state         State
	electionTimer time.Timer
}

func ElectionTimeout() time.Duration {
	seconds := rand.Intn(2) + 2
	milliseconds := rand.Intn(2000)
	return time.Duration(seconds)*time.Second + time.Duration(milliseconds)*time.Millisecond
}

func (n *Node) ResetElectionTimer() {
	n.electionTimer.Stop()
	select {
	case <-n.electionTimer.C:
	default:
	}
	n.electionTimer.Reset(ElectionTimeout())
}

// =======================================Appending=======================================
type AppendEntriesArgs struct {
	term         uint64
	leaderId     string
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []string
	leaderCommit uint64
}

type AppendEntriesResult struct {
	term    uint64
	success bool
}

func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResult) error {

	ps := n.state.persistentState
	currentTerm := ps.CurrentTerm()
	reply.term = currentTerm

	// Authority Heartbeat
	if len(args.entries) == 0 {
		n.ResetElectionTimer()
		reply.success = true
		return nil
	}

	if args.term < currentTerm {
		reply.success = false
		return nil
	}

	if args.term > currentTerm {
		ps.SetTerm(args.term)
	}

	// Seek same log position. If we have 0 - ok, just append
	if args.prevLogIndex > 0 {
		e := ps.NthEntry(args.prevLogIndex)
		if e == nil || e.Term != args.prevLogTerm {
			reply.success = false
			return nil
		}
	}

	ps.ClearAbove(args.prevLogIndex)

	// Batch append starts with current [prevLogIndex + 1] position
	ps.Append(args.term, args.prevLogIndex+1, args.entries)

	if args.leaderCommit > n.state.commitIndex {
		//Mutex protection during compaction???
		n.state.commitIndex = min(args.leaderCommit, args.prevLogIndex+uint64(len(args.entries)))
	}

	reply.success = true
	return nil
}

//=======================================Appending=======================================

//========================================Voting=========================================

type RequestVoteArgs struct {
	term         uint64
	candidateId  string
	lastLogIndex uint64
	lastLogTerm  uint64
}

type RequestVoteResult struct {
	term        uint64
	voteGranted bool
}

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteResult) error {
	currentTerm := n.state.persistentState.CurrentTerm()
	reply.term = currentTerm

	if args.term < currentTerm {
		reply.voteGranted = false
		return nil
	} else {
		votedFor := n.state.persistentState.VotedFor()
		lastEntry := n.state.persistentState.LastEntry()
		if (votedFor != NullCanidateId) && (votedFor != args.candidateId) {
			reply.voteGranted = false
			return nil
		}

		if args.lastLogTerm < lastEntry.Term {
			reply.voteGranted = false
			return nil
		}

		if args.lastLogTerm > lastEntry.Term {
			reply.voteGranted = true
			n.state.persistentState.Set(args.term, args.candidateId)
			return nil
		}

		if args.lastLogTerm == lastEntry.Term {
			if args.lastLogIndex >= lastEntry.Index {
				reply.voteGranted = true
				n.state.persistentState.SetVotedFor(args.candidateId)
				return nil
			} else {
				reply.voteGranted = false
				return nil
			}
		}

		return nil
	}
}

//========================================Voting=========================================

// Timers, goroutines, compaction, etc...
func (n *Node) BootRun() {
	// TODO
}
