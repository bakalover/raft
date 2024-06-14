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
	NULL_CANDIDATE_ID = "nodeNull"
	AUTHORITY_TIMEOUT = 200 * time.Millisecond
)

type State struct {
	role            *RoleStateMachine
	persistentState *PersistentState
	commitIndex     uint64
	lastApplied     uint64
	stateMu         sync.Mutex
	nextIndex       []uint64
	matchIndex      []uint64
}

type Node struct {
	id  int
	ids int
	// Protects reconnClients
	reconnMu sync.Mutex
	// Transfers info about which nodes is down (ids) and need to be reconnected
	reconnC       chan int
	reconnClients []*rpc.Client
	// Channel for clients commands
	clientC       chan string
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
		role:            new(RoleStateMachine),
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

func (n *Node) UpdateClient(id int, c *rpc.Client) {
	n.reconnMu.Lock()
	defer n.reconnMu.Unlock()
	n.reconnClients[id] = c
}

func (n *Node) GetClient(id int) *rpc.Client {
	n.reconnMu.Lock()
	defer n.reconnMu.Unlock()
	return n.reconnClients[id]
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
	if args.entries == nil {
		n.ResetElectionTimer()
		if args.term < currentTerm {
			reply.success = false
		} else {
			reply.success = true
		}
		return nil
	}

	if args.term < currentTerm {
		reply.success = false
		return nil
	}

	if args.term > currentTerm {
		ps.SetTerm(args.term)
		n.state.role.TransitTo(Follower)
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
	ps.AppendToLog(args.term, args.prevLogIndex+1, args.entries)

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

		if (votedFor != NULL_CANDIDATE_ID) && (votedFor != args.candidateId) {
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
			n.state.role.TransitTo(Follower)
			return nil
		}

		if args.lastLogTerm == lastEntry.Term {
			if args.lastLogIndex >= lastEntry.Index {
				reply.voteGranted = true
				ps.SetVotedFor(args.candidateId)
				n.state.role.TransitTo(Follower)
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
	var (
		client *rpc.Client
		err    error
	)
	for id := range n.reconnC {
		for client, err = rpc.Dial("tcp", "node"+string(id)+":8080"); err != nil; {
			log.Printf("error connecting to server node%v via RPC", id)
			time.Sleep(5 * time.Millisecond)
		}
		n.UpdateClient(id, client)
	}
}

func (n *Node) DefferedElection() {
	n.ResetElectionTimer()
	// Other goroutines may also reset timer several or infinite times
	<-n.electionTimer.C
	n.ImmediateElection()
}

type Vote struct{} // Sugar

func (n *Node) ImmediateElection() {
	quorum := (n.ids - 1) / 2 //n.ids % 2 == 1
	ps := n.state.persistentState
	term := n.TransitToCandidate()

	// Buffer prevents deadlock that leads to memory leak:
	// In case when quorum is gathered and there more granted votes
	// Checkout (!)
	countVote := make(chan Vote, n.ids)

	// Prepare RPC args
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
		go n.MakeVoteCall(id, args, countVote)
	}

	n.GatherQuorumOrRetry(countVote, quorum)
}

func (n *Node) MakeVoteCall(id int, args *RequestVoteArgs, c chan<- Vote) {
	reply := new(RequestVoteResult)
	ps := n.state.persistentState
	err := n.GetClient(id).Call("Node.RequestVote", args, reply)
	if err != nil {
		n.reconnC <- id // Request reconnection to node
		return          // This RequestVote call failed, no retries
	}
	if reply.voteGranted {
		c <- Vote{} // (!) May blocks forever without buffer!!
	} else {
		//	Diffent rpc calls may rewrite persistent term
		//	So we are facing some race here
		//	To prevent such thing we use thread-safe persistent storage
		// 	Performance negative impact =(
		if ps.CurrentTerm() < reply.term {
			ps.SetTerm(reply.term)
			n.state.role.TransitTo(Follower) // Discover new Term
		}
	}
}

func (n *Node) TransitToCandidate() uint64 {
	term := n.state.persistentState.IncrementAndFetchTerm()
	n.state.role.TransitTo(Candidate)
	return term
}

func (n *Node) GatherQuorumOrRetry(c <-chan Vote, atLeast int) {
	gotVotes := 0
	for {
		select {
		case <-c:
			gotVotes++
			if gotVotes >= atLeast {
				n.state.role.TransitTo(Leader)
				go n.ServeClients() // Start replication
				return
			}
		case <-n.electionTimer.C:
			// Just starting another Immediate election if that fails
			// In case there is Follower state (stored during parallel AppendEntries call -> start Deffered Election)
			if n.state.role.Whoami() == Follower {
				go n.DefferedElection()
			} else {
				go n.ImmediateElection() // Seems like AsYnc ReCuRSioN =)) ;
			}
			return
		}
	}
}

func (n *Node) ServeClients() {

	lastIndex := n.state.persistentState.LastEntry().Index

	// (Reinitialized after election)
	n.state.stateMu.Lock()
	for i := range len(n.state.nextIndex) {
		n.state.nextIndex[i] = lastIndex + 1
		n.state.matchIndex[i] = 0
	}
	n.state.stateMu.Unlock()

	go n.AuthorityHeartbeats()

	for upd := range n.clientC {
		ps := n.state.persistentState
		entries := []string{upd} // TODO: batches
		term := ps.CurrentTerm()
		last := ps.LastEntry()

		ps.AppendToLog(term, last.Index+1, entries)

		args := &AppendEntriesArgs{
			term:         term,
			leaderId:     string(n.id),
			prevLogIndex: last.Index,
			prevLogTerm:  last.Term,
			entries:      entries,
			leaderCommit: n.state.commitIndex,
		}

		for id := range n.ids {
			go n.Replicate(id, args)
		}
		if n.state.role.Whoami() == Follower {
			return
		}
	}
}

func (n *Node) AuthorityHeartbeats() {
	ti := time.NewTicker(AUTHORITY_TIMEOUT)
	emptyArgs := new(AppendEntriesArgs)
	stopBeatsC := make(chan struct{}, n.ids)
	for {
		select {
		case <-ti.C:
			for id := range n.ids {
				go func(id int) {
					reply := new(AppendEntriesResult)
					err := n.GetClient(id).Call("Node.AppendEntries", emptyArgs, reply)
					if err != nil {
						n.reconnC <- id // Request reconnection to node
						return          // This RequestVote call failed, no retries
					}
					if !reply.success {
						ps := n.state.persistentState
						if ps.CurrentTerm() < reply.term {
							ps.SetTerm(reply.term)
							stopBeatsC <- struct{}{}
						}
					}
				}(id)
			}

		case <-stopBeatsC:
			n.state.role.TransitTo(Follower)
			go n.DefferedElection()
			return
		}
	}
}

func (n *Node) Replicate(id int, args *AppendEntriesArgs) {
	reply := new(AppendEntriesResult)
	ps := n.state.persistentState

	for {
		err := n.GetClient(id).Call("Node.AppendEntries", args, reply)
		if err != nil {
			n.reconnC <- id
			return
		}
		if !reply.success {
			if ps.CurrentTerm() < reply.term { // Observed higher term
				ps.SetTerm(reply.term)
				n.state.role.TransitTo(Follower)
				return
			} else { // Seek over incosistent log
				n.state.stateMu.Lock()
				if n.state.nextIndex[id] == 0 {
					panic("bruuuuuuuh: nextIndex is 0")
				}
				n.state.nextIndex[id]--
				n.state.stateMu.Unlock()
				continue // retry
			}
		} else {
			n.state.stateMu.Lock()
			n.state.matchIndex[id] = n.state.nextIndex[id]
			n.state.nextIndex[id]++
			n.state.stateMu.Unlock()
		}
	}
}

func (n *Node) BootRun() {

	// rpc.Register(new(Node))
	// rpc.HandleHTTP()

	http.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		// ?????????????????????????????????????????????????????????
		// command := r.FormValue("command")
		// for {
		// 	role := n.state.role.Whoami()
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

	// Establishing connections
	for id := range n.ids {
		n.reconnC <- id
	}

	go n.DefferedElection()

	// TODO n.LogCompaction()
}
