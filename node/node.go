package node

import (
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	NullCandidateId                   = "nodeNull"
	AuthorityTimeout                  = 200 * time.Millisecond
	ElectionTimeoutSeconds            = 2
	ElectionTimeoutSecondsLowerBorder = 2
	ElectionTimeoutMilliseconds       = 2000
)

type State struct {
	role            *RoleStateMachine
	persistentState *PersistentState
	stateMu         sync.Mutex
	commitIndex     uint64
	lastApplied     uint64
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
	logger        *log.Logger
}

func NewNode(id int, ids int) *Node {
	countWithoutMe := ids - 1
	nextIndex := make([]uint64, countWithoutMe)
	matchIndex := make([]uint64, countWithoutMe)
	reconnClients := make([]*rpc.Client, countWithoutMe)
	reconnC := make(chan int)
	logger := log.New(os.Stdout, "[INFO]", log.Lshortfile|log.Ltime|log.Lmicroseconds)

	for i := range countWithoutMe {
		nextIndex[i] = 1
		matchIndex[i] = 0
	}

	state := &State{
		role:            new(RoleStateMachine),
		persistentState: NewPersistentState(strconv.Itoa(id)),
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
		logger:        logger,
	}
}

func (n *Node) UpdateClient(id int, c *rpc.Client) {
	n.reconnMu.Lock()
	defer n.reconnMu.Unlock()
	n.reconnClients[id] = c
	n.logger.Printf("New rpc client for id - %v\n", id)
}

func (n *Node) GetClient(id int) *rpc.Client {
	n.reconnMu.Lock()
	defer n.reconnMu.Unlock()
	return n.reconnClients[id]
}

func ElectionTimeout() time.Duration {
	seconds := rand.Intn(ElectionTimeoutSeconds) + ElectionTimeoutSecondsLowerBorder
	milliseconds := rand.Intn(ElectionTimeoutMilliseconds)
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
		n.logger.Println("Heartbeat!")
		n.ResetElectionTimer()
		if args.term < currentTerm {
			reply.success = false
		} else {
			reply.success = true
		}
		return nil
	}

	if args.term < currentTerm {
		n.logger.Printf("Reject appending: my term - %v, request term - %v\n", currentTerm, args.term)
		reply.success = false
		return nil
	}

	if args.term > currentTerm {
		ps.SetTerm(args.term)
		n.state.role.TransitTo(Follower)
		n.logger.Printf("Observerd higher term during appending - transit to follower\n")
	}

	// Seek same log position. If we have 0 - ok, just append
	if args.prevLogIndex > 0 {
		e := ps.NthEntry(args.prevLogIndex)
		if e == nil {
			n.logger.Panic("Nil Nth entry!")
		}
		n.logger.Printf("Seeking: current term and index: %v, %v. Needed term - %v\n", e.Term, e.Index, args.prevLogTerm)
		if e == nil || e.Term != args.prevLogTerm {
			reply.success = false
			return nil
		}
	}

	ps.ClearAbove(args.prevLogIndex)

	// Batch append starts with current [prevLogIndex + 1] position
	ps.AppendToLog(args.term, args.prevLogIndex+1, args.entries)

	n.state.stateMu.Lock()
	if args.leaderCommit > n.state.commitIndex {
		//Mutex protection during compaction???
		n.state.commitIndex = min(args.leaderCommit, args.prevLogIndex+uint64(len(args.entries)))
		n.logger.Printf("New observed commit index during appending - %v\n", n.state.commitIndex)
	}
	n.state.stateMu.Unlock()

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
		n.logger.Printf("Reject vote args.term < currentTerm: %v < %v\n", args.term, currentTerm)
		reply.voteGranted = false
		return nil
	} else {
		votedFor := ps.VotedFor()
		lastEntry := ps.LastEntry()
		n.logger.Printf("Voted for: %v\n", votedFor)

		if (votedFor != NullCandidateId) && (votedFor != args.candidateId) {
			n.logger.Printf("Reject vote. Already voted: votedFor: %v, candidate: %v\n", votedFor, args.candidateId)
			reply.voteGranted = false
			return nil
		}

		if args.lastLogTerm < lastEntry.Term {
			n.logger.Printf("Reject vote. Local term higher: %v < %v\n", args.lastLogIndex, lastEntry.Term)
			reply.voteGranted = false
			return nil
		}

		if args.lastLogTerm > lastEntry.Term {
			reply.voteGranted = true
			ps.Set(args.term, args.candidateId)
			n.state.role.TransitTo(Follower)
			n.logger.Printf("Vote granted. Candidate - %v with higher term - %v. Transit to Follower\n", args.candidateId, args.term)
			return nil
		}

		if args.lastLogTerm == lastEntry.Term {
			if args.lastLogIndex >= lastEntry.Index {
				reply.voteGranted = true
				ps.SetVotedFor(args.candidateId)
				n.state.role.TransitTo(Follower)
				n.logger.Printf("Vote granted. Candidate - %v with higher index - %v. Transit to Follower\n", args.candidateId, args.lastLogIndex)
				return nil
			} else {
				n.logger.Printf("Vote rejected. Candidate - %v with lower index - %v.\n", args.candidateId, args.lastLogIndex)
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
		client          *rpc.Client
		err             error
		connectionTrack sync.Map
	)

	for id := range n.ids {
		connectionTrack.Store(id, true) // Assuming that all connections are ok
	}

	for id := range n.reconnC {
		n.logger.Printf("Requested reconnection for node with id - %v\n", id)
		if connectionTrack.CompareAndSwap(id, true, false) { // Defence from double Dialing
			go func(id int) {
				defer connectionTrack.Store(id, true) // Allow to new Recconnection error to proceed
				defer func() {
					n.UpdateClient(id, client)
					n.logger.Printf("Connection establish to node%v via RPC\n", id)
				}()
				for client, err = rpc.Dial("tcp", "node"+strconv.Itoa(id)+":8080"); err != nil; {
					n.logger.Printf("Couldn't establish connection to node%v via RPC. Retrying...\n", id)
					time.Sleep(5 * time.Millisecond)
				}
			}(id)
		}
	}
}

func (n *Node) DefferedElection() {
	n.logger.Println("Start Deffered election")
	n.ResetElectionTimer()
	// Other goroutines may also reset timer several or infinite times
	<-n.electionTimer.C
	n.logger.Println("Start Immediate election")
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
		candidateId:  strconv.Itoa(n.id),
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
		n.logger.Printf("Error in Node.RequestVote Call: %v on Client - %v. Requesting reconnection...\n", err, id)
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
		currentTerm := ps.CurrentTerm()
		if currentTerm < reply.term {
			n.logger.Printf("Observed higher term while MakeVoteCall: Current - %v, Observerd - %v\n", currentTerm, reply.term)
			ps.SetTerm(reply.term)
			n.state.role.TransitTo(Follower) // Discover new Term
		}
	}
}

func (n *Node) TransitToCandidate() uint64 {
	ps := n.state.persistentState
	term := ps.IncrementAndFetchTerm()
	ps.SetVotedFor(NullCandidateId) // Prepare slot for votedFor
	n.state.role.TransitTo(Candidate)
	n.logger.Printf("Transit to Candidate state. New term - %v\n", term)
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
				n.logger.Println("Election Successful. Transit to Leader state")
				go n.ServeClients() // Start replication
				return
			}
		case <-n.electionTimer.C:
			// Just starting another Immediate election if that fails
			// In case there is Follower state (stored during parallel AppendEntries call -> start Deffered Election)
			n.logger.Println("Election timeout occur while gathering vote quorum")
			if n.state.role.Whoami() == Follower {
				n.logger.Println("Someone made me Follower while Appending during my election")
				go n.DefferedElection()
			} else {
				n.logger.Println("Retrying Election...")
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

	ps := n.state.persistentState
	currentTerm := ps.CurrentTerm()

	go n.Commiter(currentTerm)

	for upd := range n.clientC {
		n.logger.Printf("Recieved command from client: %v\n", upd)

		entries := []string{upd} // TODO: batches
		last := ps.LastEntry()

		n.logger.Println("Appending command to local Leader log")
		ps.AppendToLog(currentTerm, last.Index+1, entries)

		n.state.stateMu.Lock()
		ci := n.state.commitIndex
		n.state.stateMu.Unlock()

		args := &AppendEntriesArgs{
			term:         currentTerm,
			leaderId:     strconv.Itoa(n.id),
			prevLogIndex: last.Index,
			prevLogTerm:  last.Term,
			entries:      entries,
			leaderCommit: ci,
		}

		n.logger.Println("Begin Replication...")
		for id := range n.ids {
			go n.Replicate(id, args)
		}
		if n.state.role.Whoami() == Follower {
			n.logger.Println("Stop serving clients because transited Leader -> Follower")
			return
		}
	}
}

func (n *Node) AuthorityHeartbeats() {
	ti := time.NewTicker(AuthorityTimeout)
	emptyArgs := new(AppendEntriesArgs)
	stopBeatsC := make(chan struct{}, n.ids)
	for {
		select {
		case <-ti.C:
			n.logger.Println("Make Heartbeat")
			for id := range n.ids {
				go func(id int) {
					reply := new(AppendEntriesResult)
					err := n.GetClient(id).Call("Node.AppendEntries", emptyArgs, reply)
					if err != nil {
						n.logger.Printf("Error on heartbeat. Client - %v. Requesting reconnection...\n", id)
						n.reconnC <- id // Request reconnection to node
						return          // This RequestVote call failed, no retries
					}
					if !reply.success {
						ps := n.state.persistentState
						currentTerm := ps.CurrentTerm()
						if currentTerm < reply.term {
							n.logger.Printf("Error on heartbeat. Observed higher term Client - %v. Local term - %v, Observed term - %v.Stop heartbeats\n", id, currentTerm, reply.term)
							ps.SetTerm(reply.term)
							stopBeatsC <- struct{}{}
							return
						}
						n.logger.Panic("Unreachable")
					}
				}(id)
			}

		case <-stopBeatsC:
			n.state.role.TransitTo(Follower)
			n.logger.Println("Tnansit from Leader to Follower")
			ti.Stop()
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
		n.logger.Printf("Error on AppendEntries Call: %v. Requesting reconnection...\n", err)
		if err != nil {
			n.reconnC <- id
			return
		}
		if !reply.success {
			currentTerm := ps.CurrentTerm()
			if currentTerm < reply.term { // Observed higher term
				n.logger.Printf("Error on appending. Observed higher term. Client - %v. Local term - %v. Observed term - %v. Transit to Follower\n", id, currentTerm, reply.term)
				ps.SetTerm(reply.term)
				n.state.role.TransitTo(Follower)
				return
			} else { // Seek over inconsistent log
				n.state.stateMu.Lock()
				if n.state.nextIndex[id] == 0 {
					n.logger.Panic("While seeking on other node's log: n.state.nextIndex[id] == 0")
				}
				n.state.nextIndex[id]--
				n.state.stateMu.Unlock()
				continue // retry
			}
		} else {
			n.state.stateMu.Lock()
			n.state.matchIndex[id] = n.state.nextIndex[id]
			n.state.nextIndex[id]++
			n.logger.Printf("Updated for node%v nextIndex - %v, matchIndex - %v\n", id, n.state.nextIndex[id], n.state.matchIndex[id])
			n.state.stateMu.Unlock()
		}
	}
}

func (n *Node) Commiter(currentTerm uint64) {
	n.logger.Println("Begin seeking commitIndex...")
	quorum := (n.ids - 1) / 2
	ps := n.state.persistentState
	ti := time.NewTicker(30 * time.Millisecond)
	for {
		<-ti.C
		lastIndex := ps.LastEntry().Index
		n.state.stateMu.Lock()
		N := n.state.commitIndex + 1
		for index := N; index <= lastIndex; index++ {
			count := 0
			for id := range n.ids {
				if n.state.matchIndex[id] >= N {
					count++
				}
			}
			if count >= quorum && currentTerm == ps.NthEntry(index).Term {
				n.logger.Printf("Found quorum on commitIndex! New commitIndex - %v\n", N)
				n.state.commitIndex = N
			}
		}
		n.state.stateMu.Unlock()

		if n.state.role.Whoami() == Follower {
			n.logger.Println("Stop seeking commitIndex because transited Leader -> Follower")
			ti.Stop()
			return
		}
	}
}

func (n *Node) Apply() {
	n.logger.Println("Begin seeking applying commands to state machine...")
	ti := time.NewTicker(30 * time.Millisecond)
	for {
		<-ti.C
		n.state.stateMu.Lock()
		ci := n.state.commitIndex
		n.state.stateMu.Unlock()

		if ci > n.state.lastApplied {
			n.logger.Println("Found command covered by commitIndex! Applying...")
			// Apply to machine and respond to client
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

	//	First of all we need to establish rpc connections
	// 	with all nodes (excluding caller)
	// 	We will use that subroutine to reconnect rpc via channel signal
	//	if we encounter node death

	go n.ConnectRPC()
	go n.Apply()
	go n.DefferedElection()

	n.InitConnections()
	http.ListenAndServe("node"+strconv.Itoa(n.id)+":8080", nil)
}

func (n *Node) InitConnections() {
	for id := range n.ids {
		if id != n.id {
			n.reconnC <- id
		}
	}
}
