package node

import (
	"context"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/bakalover/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	NullCandidateId                        = "nodeNull"
	AuthorityTimeout                       = 200 * time.Millisecond
	ElectionTimeoutMilliseconds            = 100
	ElectionTimeoutMillisecondsLowerBorder = 300
	CommiterTick                           = 3000 * time.Millisecond
	ClientTimout                           = 3 * time.Second
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
	proto.UnimplementedNodeServer
	id  int
	ids int
	// Protects reconnClients
	reconnMu sync.Mutex
	// Transfers info about which nodes is down (ids) and need to be reconnected
	reconnC       chan int
	reconnClients []proto.NodeClient
	// Channel for clients commands
	clientC       chan *AwaitableCommand
	state         *State
	electionTimer *time.Timer
	logger        *log.Logger
}

func NewNode(id int, ids int) *Node {
	nextIndex := make([]uint64, ids)
	matchIndex := make([]uint64, ids)
	reconnClients := make([]proto.NodeClient, ids)
	reconnC := make(chan int)
	logger := log.New(os.Stdout, "[INFO] ", log.Lshortfile|log.Ltime|log.Lmicroseconds)
	electionTimer := time.NewTimer(ElectionTimeout())

	for i := range ids {
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
		electionTimer: electionTimer,
	}
}

func (n *Node) UpdateClient(id int, c proto.NodeClient) {
	n.reconnMu.Lock()
	defer n.reconnMu.Unlock()
	n.reconnClients[id] = c
	n.logger.Printf("New rpc client for id - %v\n", id)
}

func (n *Node) GetClient(id int) proto.NodeClient {
	n.reconnMu.Lock()
	defer n.reconnMu.Unlock()
	return n.reconnClients[id]
}

func Quorum(n int) int {
	return (n + 1) / 2
}

func ElectionTimeout() time.Duration {
	millis := rand.Intn(ElectionTimeoutMilliseconds) + ElectionTimeoutMillisecondsLowerBorder
	d := time.Duration(millis) * time.Millisecond
	log.Printf("Selected timeout: %v", d)
	return d
}

func (n *Node) ResetElectionTimer() {
	n.logger.Println("Resetting election timer")
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(ElectionTimeout())
}

func (n *Node) Nop(ctx context.Context, args *proto.Empty) (*proto.Empty, error) {
	// Health Check
	return nil, nil
}

// =======================================Appending=======================================
func (n *Node) AppendEntries(ctx context.Context, args *proto.AppendEntriesArgs) (*proto.AppendEntriesResult, error) {
	reply := new(proto.AppendEntriesResult)
	ps := n.state.persistentState
	currentTerm := ps.CurrentTerm()
	reply.Term = currentTerm

	// Authority Heartbeat
	if args.Entries == nil {
		n.logger.Println("Heartbeat!")
		n.ResetElectionTimer()
		if args.Term < currentTerm {
			reply.Success = false
		} else {
			if currentTerm < args.Term {
				ps.SetTerm(args.Term)
			}
			reply.Success = true
			ps.SetVotedFor(args.LeaderId) //???????????????????
		}
		return reply, nil
	}

	if args.Term < currentTerm {
		n.logger.Printf("Reject appending: my term - %v, request term - %v\n", currentTerm, args.Term)
		reply.Success = false
		return reply, nil
	}

	if args.Term > currentTerm {
		ps.SetTerm(args.Term)
		n.state.role.TransitTo(Follower)
		n.logger.Printf("Observerd higher term during appending - transit to follower\n")
	}

	// Seek same log position. If we have 0 - ok, just append
	if args.PrevLogIndex > 0 {
		e := ps.NthEntry(args.PrevLogIndex)
		if e == nil {
			n.logger.Panic("Nil Nth entry!")
		}
		n.logger.Printf("Seeking: current term and index: %v, %v. Needed term - %v\n", e.Term, e.Index, args.PrevLogIndex)
		if e == nil || e.Term != args.PrevLogIndex {
			reply.Success = false
			return reply, nil
		}
	}

	ps.ClearAbove(args.PrevLogIndex)

	// Batch append starts with current [prevLogIndex + 1] position
	ps.AppendToLog(args.Term, args.PrevLogIndex+1, args.Entries)

	n.state.stateMu.Lock()
	if args.LeaderCommit > n.state.commitIndex {
		//Mutex protection during compaction???
		n.state.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+uint64(len(args.Entries)))
		n.logger.Printf("New observed commit index during appending - %v\n", n.state.commitIndex)
	}
	n.state.stateMu.Unlock()

	reply.Success = true
	return reply, nil
}

//=======================================Appending=======================================

//========================================Voting=========================================

func (n *Node) RequestVote(ctx context.Context, args *proto.RequestVoteArgs) (*proto.RequestVoteResult, error) {
	reply := new(proto.RequestVoteResult)
	ps := n.state.persistentState
	currentTerm := ps.CurrentTerm()
	reply.Term = currentTerm

	if args.Term < currentTerm {
		n.logger.Printf("Reject vote args.term < currentTerm: %v < %v\n", args.Term, currentTerm)
		reply.VoteGranted = false
		return reply, nil
	} else {
		votedFor := ps.VotedFor()
		lastEntry := ps.LastEntry()
		n.logger.Printf("Voted for: %v\n", votedFor)

		if (votedFor != NullCandidateId) && (votedFor != args.CandidateId) {
			n.logger.Printf("Reject vote. Already voted: votedFor: %v, candidate: %v\n", votedFor, args.CandidateId)
			reply.VoteGranted = false
			return reply, nil
		}

		if args.LastLogTerm < lastEntry.Term {
			n.logger.Printf("Reject vote. Local term higher: %v < %v\n", args.LastLogIndex, lastEntry.Term)
			reply.VoteGranted = false
			return reply, nil
		}

		if args.LastLogTerm > lastEntry.Term {
			reply.VoteGranted = true
			ps.Set(args.Term, args.CandidateId)
			n.state.role.TransitTo(Follower)
			n.logger.Printf("Vote granted. Candidate - %v with higher term - %v. Transit to Follower\n", args.CandidateId, args.Term)
			return reply, nil
		}

		if args.LastLogTerm == lastEntry.Term {
			if args.LastLogIndex >= lastEntry.Index {
				reply.VoteGranted = true
				ps.SetVotedFor(args.CandidateId)
				n.state.role.TransitTo(Follower)
				n.logger.Printf("Vote granted. Candidate - %v with higher index - %v. Transit to Follower\n", args.CandidateId, args.LastLogIndex)
				return reply, nil
			} else {
				n.logger.Printf("Vote rejected. Candidate - %v with lower index - %v.\n", args.CandidateId, args.LastLogIndex)
				reply.VoteGranted = false
				return reply, nil
			}
		}

		return reply, nil
	}
}

//========================================Voting=========================================

// Recieve reconnection signals via chan
func (n *Node) ConnectRPC() {
	var connectionTrack sync.Map

	for id := range n.ids {
		if id != n.id {
			connectionTrack.Store(id, true) // Assuming that all connections are ok
		}
	}

	for id := range n.reconnC {
		n.logger.Printf("Requested reconnection for node with id: %v\n", id)
		if connectionTrack.CompareAndSwap(id, true, false) { // Defence from double Dialing
			go func(id int) {
				var (
					conn   *grpc.ClientConn
					err    error
					client proto.NodeClient
				)
				defer connectionTrack.Store(id, true) // Allow to new Recconnection error to proceed
				for {
					conn, _ = grpc.NewClient(":606"+strconv.Itoa(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
					client = proto.NewNodeClient(conn)
					if _, err = client.Nop(context.Background(), nil); err != nil {
						n.logger.Printf("Couldn't establish connection to node%v via RPC: %v. Retrying...\n", id, err)
						time.Sleep(1 * time.Second)
					} else {
						break
					}
				}
				n.UpdateClient(id, client)
				n.logger.Printf("Connection establish to node%v via RPC\n", id)
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
	quorum := Quorum(n.ids) //n.ids % 2 == 1
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
	args := &proto.RequestVoteArgs{
		Term:         term,
		CandidateId:  strconv.Itoa(n.id),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Begin election process
	// If timeout occures -> transit into next term and then retry election
	n.ResetElectionTimer()
	for id := range n.ids {
		if id != n.id {
			go n.MakeVoteCall(id, args, countVote)
		}
	}

	n.GatherQuorumOrRetry(countVote, quorum)
}

func (n *Node) MakeVoteCall(id int, args *proto.RequestVoteArgs, c chan<- Vote) {
	ps := n.state.persistentState
	client := n.GetClient(id)
	if client == nil {
		return
	}
	reply, err := client.RequestVote(context.Background(), args)
	if err != nil {
		n.logger.Printf("Error in Node.RequestVote Call: %v on Client - %v. Requesting reconnection...\n", err, id)
		n.reconnC <- id // Request reconnection to node
		return          // This RequestVote call failed, no retries
	}
	if reply.VoteGranted {
		c <- Vote{} // (!) May blocks forever without buffer!!
	} else {
		//	Diffent rpc calls may rewrite persistent term
		//	So we are facing some race here
		//	To prevent such thing we use thread-safe persistent storage
		// 	Performance negative impact =(
		currentTerm := ps.CurrentTerm()
		if currentTerm < reply.Term {
			n.logger.Printf("Observed higher term while MakeVoteCall: Current - %v, Observerd - %v\n", currentTerm, reply.Term)
			ps.SetTerm(reply.Term)
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
	n.logger.Println("Gathering vote quorum...")
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

		entries := []string{upd.command} // TODO: batches
		last := ps.LastEntry()

		n.logger.Println("Appending command to local Leader log")
		ps.AppendToLog(currentTerm, last.Index+1, entries)

		n.state.stateMu.Lock()
		ci := n.state.commitIndex
		n.state.stateMu.Unlock()

		args := &proto.AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     strconv.Itoa(n.id),
			PrevLogIndex: last.Index,
			PrevLogTerm:  last.Term,
			Entries:      entries,
			LeaderCommit: ci,
		}

		n.logger.Println("Begin Replication...")
		for id := range n.ids {
			if id != n.id {
				go n.Replicate(id, args)
			}
		}
		if n.state.role.Whoami() == Follower {
			n.logger.Println("Stop serving clients because transited Leader -> Follower")
			return
		}
	}
}

func (n *Node) AuthorityHeartbeats() {
	ti := time.NewTicker(AuthorityTimeout)
	emptyArgs := new(proto.AppendEntriesArgs)
	emptyArgs.Term = n.state.persistentState.CurrentTerm()
	stopBeatsC := make(chan struct{}, n.ids)
	for {
		select {
		case <-ti.C:
			n.logger.Println("Making Heartbeat")
			for id := range n.ids {
				if id != n.id {
					go func(id int) {
						client := n.GetClient(id)
						if client == nil {
							return
						}
						reply, err := client.AppendEntries(context.Background(), emptyArgs)
						if err != nil {
							n.logger.Printf("Error on heartbeat. Client - %v. Requesting reconnection...\n", id)
							n.reconnC <- id // Request reconnection to node
							return          // This RequestVote call failed, no retries
						}
						if !reply.Success {
							ps := n.state.persistentState
							currentTerm := ps.CurrentTerm()
							if currentTerm < reply.Term {
								n.logger.Printf("Error on heartbeat. Observed higher term Client - %v. Local term - %v, Observed term - %v.Stop heartbeats\n", id, currentTerm, reply.Term)
								ps.SetTerm(reply.Term)
								stopBeatsC <- struct{}{}
								return
							}
							if n.state.role.Whoami() == Follower {
								stopBeatsC <- struct{}{}
								return
							}
						}
					}(id)
				}
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

func (n *Node) Replicate(id int, args *proto.AppendEntriesArgs) {
	ps := n.state.persistentState

	for {
		client := n.GetClient(id)
		if client == nil {
			return
		}
		reply, err := client.AppendEntries(context.Background(), args)
		n.logger.Printf("Error on AppendEntries Call: %v. Requesting reconnection...\n", err)
		if err != nil {
			n.reconnC <- id
			return
		}
		if !reply.Success {
			currentTerm := ps.CurrentTerm()
			if currentTerm < reply.Term { // Observed higher term
				n.logger.Printf("Error on appending. Observed higher term. Client - %v. Local term - %v. Observed term - %v. Transit to Follower\n", id, currentTerm, reply.Term)
				ps.SetTerm(reply.Term)
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
	quorum := Quorum(n.ids)
	ps := n.state.persistentState
	ti := time.NewTicker(CommiterTick)
	for {
		<-ti.C
		lastIndex := ps.LastEntry().Index
		n.state.stateMu.Lock()
		N := n.state.commitIndex + 1
		for index := N; index <= lastIndex; index++ {
			count := 0
			for id := range n.ids {
				if id != n.id && n.state.matchIndex[id] >= N {
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

type AwaitableCommand struct {
	command string
	c       chan struct{} // Fullfil when commited
}

func NewAwaitableCommand(command string) *AwaitableCommand {
	c := make(chan struct{})
	return &AwaitableCommand{command: command, c: c}
}

func (n *Node) BootRun() {

	l, err := net.Listen("tcp", ":606"+strconv.Itoa(n.id))
	if err != nil {
		n.logger.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServer(grpcServer, n)
	go grpcServer.Serve(l)

	http.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		command := r.FormValue("command")
		ac := NewAwaitableCommand(command)

		for {
			role := n.state.role.Whoami()
			switch role {
			case Follower:
				// Eventually under heartbeats all followers will store leader here
				// Theoretically does not violate whole protocol
				leader := n.state.persistentState.VotedFor()
				if leader == NullCandidateId {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				http.Redirect(w, r, ":606"+leader, http.StatusMovedPermanently)
			case Leader:
				n.clientC <- ac
				t := time.NewTimer(ClientTimout)
				select { // Timeout with commiting after client retry lead to "more than exactly once"
				case <-ac.c:
					w.WriteHeader(http.StatusOK)
					return
				case <-t.C:
					w.WriteHeader(http.StatusRequestTimeout)
					return
				}
			case Candidate:
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	})

	//	First of all we need to establish rpc connections
	// 	with all nodes (excluding caller)
	// 	We will use that subroutine to reconnect rpc via channel signal
	//	if we encounter node death
	go n.ConnectRPC()
	n.InitConnections()

	go n.Apply()
	go n.DefferedElection()

	n.logger.Printf("Listening on: :606" + strconv.Itoa(n.id))
	log.Panic(http.ListenAndServe(":607"+strconv.Itoa(n.id), nil))
}

func (n *Node) InitConnections() {
	for id := range n.ids {
		if id != n.id {
			n.reconnC <- id
		}
	}
}
