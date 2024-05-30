package node

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
	currentTerm := n.state.termState.CurrentTerm()
	if args.term < currentTerm {
		reply.term = currentTerm
		reply.success = false
		return nil
	}

	term, _, err := n.state.log.NthEntry(args.prevLogIndex)

	// Not found or found but term is not the same
	if err != nil || term != args.prevLogTerm {
		reply.term = currentTerm
		reply.success = false
		return nil
	}

	

	return nil
}

//=======================================Appending=======================================

//========================================Voting=========================================

type RequestVoteArgs struct {
	term         uint64
	candidateId  string
	lastLogIndex uint64
	lastLogterm  uint64
}

type RequestVoteResult struct {
	term        uint64
	voteGranted bool
}

func (n *Node) ReqestVote(args *RequestVoteArgs, reply *RequestVoteResult) error {
	// TODO
	return nil
}

//========================================Voting=========================================
