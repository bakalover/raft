package node

import (
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
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
	id  int
	ids int
	// Protect reconnClients
	reconnMu sync.Mutex
	// Transfer info about which nodes is down (ids) and need to be reconnected
	reconnC       chan int
	reconnClients []*rpc.Client
	state         *State
	electionTimer time.Timer
}

func NewNode(id int, ids int) *Node {
	countWithoutMe := ids - 1
	nextIndex := make([]uint64, countWithoutMe)
	matchIndex := make([]uint64, countWithoutMe)
	reconnClients := make([]*rpc.Client, countWithoutMe)
	reconnC := make(chan int)

	for i := range countWithoutMe {
		nextIndex[i] = 1
		matchIndex[i] = 0
	}

	state := &State{
		role:            NewRoleStateMachine(),
		persistentState: NewPersistentState(),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       nextIndex,
		matchIndex:      matchIndex,
	}

	return &Node{
		id:            id,
		ids:           ids,
		reconnClients: reconnClients,
		reconnC:       reconnC,
		state:         state,
	}
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
		n.state.role.Exchange(Follower)
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
	ps := n.state.persistentState
	currentTerm := ps.CurrentTerm()
	reply.term = currentTerm

	if args.term < currentTerm {
		reply.voteGranted = false
		return nil
	} else {
		votedFor := ps.VotedFor()
		lastEntry := ps.LastEntry()

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
			ps.Set(args.term, args.candidateId)
			n.state.role.Exchange(Follower)
			return nil
		}

		if args.lastLogTerm == lastEntry.Term {
			if args.lastLogIndex >= lastEntry.Index {
				reply.voteGranted = true
				ps.SetVotedFor(args.candidateId)
				n.state.role.Exchange(Follower)
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

// Recieve reconnection signals via chan
func (n *Node) ConnectRPC() {
	for id := range n.reconnC {
		var client *rpc.Client
		var err error
		for client, err = rpc.Dial("tcp", "node"+string(id)+":8080"); err != nil; {
			log.Printf("error connecting to server node%v via RPC", id)
			time.Sleep(5 * time.Millisecond)
		}
		n.reconnMu.Lock()
		n.reconnClients[id] = client
		n.reconnMu.Unlock()
	}
}

type Vote struct{}

func (n *Node) ImmediateElection() {
	quorum := (n.ids - 1) / 2 //n.ids % 2 == 1
	ps := n.state.persistentState

	// Transit state
	ps.IncremenTerm()
	n.state.role.Exchange(Candidate)

	// Buffer prevents deadlock that leads to memory leak:
	// In case when quorum is gathered and there more granted votes
	// Checkout (!)
	countVote := make(chan Vote, n.ids)

	// Prepare RPC args
	term := ps.CurrentTerm()
	last := ps.LastEntry()

	lastLogIndex := last.Index
	lastLogTerm := last.Term

	args := &RequestVoteArgs{
		term:         term,
		candidateId:  string(n.id),
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
	}

	// Begin election process
	// If timeout occures -> transit into next term and then retry election
	n.ResetElectionTimer()
	for id := range n.ids {
		go func(id int) {
			reply := new(RequestVoteResult)
			// Semi-bottleneck =( ; .Go(...) partly neutralizes mutex effect
			n.reconnMu.Lock()
			call := n.reconnClients[id].Go("Node.RequestVote", args, reply, nil)
			n.reconnMu.Unlock()

			<-call.Done
			if call.Error != nil {
				n.reconnC <- id // Request reconnection to node
				return          // This RequestVote call failed, no retries
			}
			if reply.voteGranted {
				countVote <- Vote{} // (!) May blocks forever without buffer!!
			} else {
				//	Diffent rpc calls may rewrite persistent term
				//	So we are facing some race here
				//	To prevent such thing we use thread-safe persistent storage
				// 	Performance negative impact =(
				if ps.CurrentTerm() < reply.term {
					ps.SetTerm(reply.term)
				}
			}
		}(id)
	}

	gotVotes := 0
loop:
	for {
		select {
		case <-countVote:
			gotVotes++
			if gotVotes >= quorum {
				n.state.role.Exchange(Leader)
				go n.Replicate() // Start replication
				break loop
			}
		case <-n.electionTimer.C:
			// Just starting another IMMEDIATE election if that fails
			go n.ImmediateElection() // Seems like AsYnc ReCuRSioN =)) ;
			break loop
		}
	}

}

func (n *Node) TimeoutElection() {
	n.ResetElectionTimer()
	<-n.electionTimer.C
	n.ImmediateElection()
}

func (n *Node) Replicate() {
	// Authority heatbeat in separate goroutine???
	// Append entries with reseting timer on each send????
}

func (n *Node) BootRun() {

	rpc.Register(new(Node))
	rpc.HandleHTTP()

	http.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		// ?????????????????????????????????????????????????????????
		// command := r.FormValue("command")
		// for {
		// 	role := n.state.role.Load()
		// 	switch role {
		// 	case Follower:
		// 		leader := n.state.persistentState.VotedFor()
		// 		if leader == NullCanidateId {
		// 			time.Sleep(5 * time.Millisecond)
		// 			continue
		// 		}
		// 		http.Redirect(w, r, leader+":8080", http.StatusMovedPermanently)
		// 	case Leader:
		// 	case Candidate:
		// 	}
		// }
	})

	go http.ListenAndServe("node"+string(n.id)+":8080", nil)

	//	First of all we need to establish rpc connections
	// 	with all nodes (incuding caller)
	// 	We will use that subroutine to reconnect rpc via channel signal
	//	if we encounter node death
	go n.ConnectRPC()

	for id := range n.ids {
		n.reconnC <- id
	}

	go n.TimeoutElection()
}
