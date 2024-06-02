package node

const (
	NullCanidateId = "nodeNull"
)

type Node struct {
	state State
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
	if args.term < currentTerm {
		reply.term = currentTerm
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
			reply.term = currentTerm
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

	reply.term = currentTerm
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
			return nil
		}

		if args.lastLogTerm == lastEntry.Term {
			if args.lastLogIndex >= lastEntry.Index {
				reply.voteGranted = true
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
