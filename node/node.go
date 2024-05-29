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
	CommiterTick                           = 100 * time.Millisecond
	ClientTimout                           = 6 * time.Second
	ApplyInterval                          = 100 * time.Millisecond
)

type Node struct {
	proto.UnimplementedNodeServer
	id  int
	ids int
	// Protects reconnClients
	reconnLock sync.Mutex
	// Transfers info about which nodes is down (ids) and need to be reconnected
	reconnChan chan int
	// Client connections to other nodes
	reconnClients []proto.NodeClient
	// Recieving commands from http clients if Leader
	clientChan chan *AwaitableCommand
	// Keep track of commands while replicating
	applyRegistry sync.Map
	state         *State
	electionTimer *time.Timer
	logger        *log.Logger
}

type State struct {
	// Race - free fields
	role            *RoleStateMachine
	persistentState *PersistentState
	// Protects all fields below
	stateLock   sync.Mutex
	commitIndex uint64
	lastApplied uint64
	nextIndex   []uint64
	matchIndex  []uint64
}

func NewNode(id int, ids int) *Node {
	nextIndex := make([]uint64, ids)
	matchIndex := make([]uint64, ids)
	reconnClients := make([]proto.NodeClient, ids)
	reconnChan := make(chan int)
	logger := log.New(os.Stdout, "[INFO] ", log.Lshortfile|log.Ltime|log.Lmicroseconds)
	electionTimer := time.NewTimer(ElectionTimeout())
	clientChan := make(chan *AwaitableCommand)

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
		reconnChan:    reconnChan,
		clientChan:    clientChan,
		state:         state,
		logger:        logger,
		electionTimer: electionTimer,
	}
}

func (n *Node) UpdateClient(id int, c proto.NodeClient) {
	n.reconnLock.Lock()
	defer n.reconnLock.Unlock()
	n.reconnClients[id] = c
	n.logger.Printf("New rpc client for id - %v\n", id)
}

func (n *Node) GetClient(id int) proto.NodeClient {
	n.reconnLock.Lock()
	defer n.reconnLock.Unlock()
	return n.reconnClients[id]
}

func Quorum(n int) int {
	return (n - 1) / 2
}

func ElectionTimeout() time.Duration {
	millis := rand.Intn(ElectionTimeoutMilliseconds) + ElectionTimeoutMillisecondsLowerBorder
	d := time.Duration(millis) * time.Millisecond
	log.Printf("Selected timeout: %v", d)
	return d
}

func (n *Node) ResetElectionTimer() {
	n.logger.Println("Election timer reset!")
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(ElectionTimeout())
}

func (n *Node) Nop(ctx context.Context, args *proto.Empty) (*proto.Empty, error) {
	// Just health Check
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
		n.logger.Println("Recieved heartbeat!")
		n.ResetElectionTimer()
		if args.Term < currentTerm {
			reply.Success = false
		} else {
			if currentTerm < args.Term {
				ps.SetTerm(args.Term)
			}
			reply.Success = true
			n.logger.Printf("Learn Leader from Heartbeat: node%v\n", args.LeaderId)
			ps.SetVotedFor(args.LeaderId)
		}
		return reply, nil
	}

	if args.Term < currentTerm {
		n.logger.Printf("Reject appending: my term - %v, leader term - %v\n", currentTerm, args.Term)
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
		if e == nil || e.Term != args.PrevLogTerm {
			n.logger.Printf("Seek inconsistent log back while appending: Index: %v", args.PrevLogIndex)
			reply.Success = false
			return reply, nil
		}
	}

	ps.ClearAbove(args.PrevLogIndex)

	// Batch append starts with current [prevLogIndex + 1] position
	ps.AppendToLog(args.Term, args.PrevLogIndex+1, args.Entries)

	n.state.stateLock.Lock()
	if args.LeaderCommit > n.state.commitIndex {
		n.state.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+uint64(len(args.Entries)))
		n.logger.Printf("New observed commit index during appending - %v\n", n.state.commitIndex)
	}
	n.state.stateLock.Unlock()

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
	}

	votedFor := ps.VotedFor()
	lastEntry := ps.LastEntry()
	n.logger.Printf("Voted for: %v\n", votedFor)

	if (votedFor != NullCandidateId) && (votedFor != args.CandidateId) {
		n.logger.Printf("Reject vote. Already voted: votedFor: node%v, candidate: %v\n", votedFor, args.CandidateId)
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
		n.logger.Printf("Vote granted. Candidate - %v with higher term - %v. Transited to Follower\n", args.CandidateId, args.Term)
		return reply, nil
	}

	if args.LastLogTerm == lastEntry.Term {
		if args.LastLogIndex >= lastEntry.Index {
			reply.VoteGranted = true
			ps.SetVotedFor(args.CandidateId)
			n.state.role.TransitTo(Follower)
			n.logger.Printf("Vote granted. Candidate - node%v with higher index - %v. Transited to Follower\n", args.CandidateId, args.LastLogIndex)
			return reply, nil
		} else {
			n.logger.Printf("Vote rejected. Candidate - node%v with lower index - %v.\n", args.CandidateId, args.LastLogIndex)
			reply.VoteGranted = false
			return reply, nil
		}
	}

	return reply, nil

}

//========================================Voting=========================================

// Recieve reconnection signals via chan
func (n *Node) ReconnectActivity() {
	var connectionTrack sync.Map

	for id := range n.ids {
		if id != n.id {
			connectionTrack.Store(id, true) // Assuming that all connections are ok
		}
	}

	for id := range n.reconnChan {
		n.logger.Printf("Requested reconnection for node with id: %v\n", id)
		// I recomment do not even think about what is going on below
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
	quorum := Quorum(n.ids)
	ps := n.state.persistentState
	term := n.TransitToCandidate()

	// Buffer prevents deadlock that leads to memory leak:
	// In case when quorum is gathered and there more granted votes
	// Checkout (!)
	countVote := make(chan Vote, n.ids)

	// Prepare RPC args
	last := ps.LastEntry()
	args := &proto.RequestVoteArgs{
		Term:         term,
		CandidateId:  strconv.Itoa(n.id),
		LastLogIndex: last.Index,
		LastLogTerm:  last.Term,
	}

	// Begin election process
	// If timeout occures -> transit into next term and then retry election
	n.ResetElectionTimer()
	for id := range n.ids {
		if id != n.id {
			go n.RequestVoteAnalyze(id, args, countVote)
		}
	}

	n.GatherQuorumOrRetry(countVote, quorum)
}

func (n *Node) RequestVoteAnalyze(id int, args *proto.RequestVoteArgs, c chan<- Vote) {
	ps := n.state.persistentState
	client := n.GetClient(id)
	if client == nil {
		return
	}
	reply, err := client.RequestVote(context.Background(), args)
	if err != nil {
		n.logger.Printf("Error in Node.RequestVote call: %v on Client - %v. Reconnection requested!\n", err, id)
		n.reconnChan <- id // Request reconnection to node
		return             // This RequestVote call failed, no retries
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
	n.logger.Printf("Transited to Candidate! New term - %v\n", term)
	return term
}

func (n *Node) TransitToLeader() {
	n.state.role.TransitTo(Leader)
	n.logger.Println("Election Successful! Transited to Leader")
	go n.ServeClients() // Start Replication
}

func (n *Node) GatherQuorumOrRetry(c <-chan Vote, atLeast int) {
	n.logger.Println("Gathering vote quorum...")
	gotVotes := 0

	if atLeast == 0 { // Single node config
		n.TransitToLeader()
		return
	}

	for {
		select {
		case <-c:
			gotVotes++
			if gotVotes >= atLeast {
				n.TransitToLeader()
				return
			}
		case <-n.electionTimer.C:
			// Just starting another Immediate election if that fails
			// In case there is Follower state (stored during parallel AppendEntries call -> start Deffered Election)
			n.logger.Println("Election timeout occured while gathering vote quorum")
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
	n.state.stateLock.Lock()
	for i := range len(n.state.nextIndex) {
		n.state.nextIndex[i] = lastIndex + 1
		n.state.matchIndex[i] = 0
	}
	n.state.stateLock.Unlock()

	go n.AuthorityHeartbeats()

	ps := n.state.persistentState
	currentTerm := ps.CurrentTerm()
	go n.CommitActivity(currentTerm)

	for upd := range n.clientChan {
		n.logger.Printf("Recieved command from client: %v\n", upd)

		entries := []string{upd.command} // TODO: batches
		last := ps.LastEntry()

		n.logger.Println("Appending command to local Leader log")
		ps.AppendToLog(currentTerm, last.Index+1, entries)
		n.applyRegistry.Store(last.Index+1, upd) // Memorize to answer client later via chan
		n.logger.Printf("Stored into applyRegistry key - %v", last.Index+1)

		n.logger.Println("Begin replication...")
		for id := range n.ids {
			if id != n.id {
				go n.Replicate(id, currentTerm, entries)
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
	emptyArgs.LeaderId = strconv.Itoa(n.id)
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
							n.logger.Printf("Error on heartbeat. Client - %v. Reconnection requested\n", id)
							n.reconnChan <- id // Request reconnection to node
							return             // This RequestVote call failed, no retries
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

func (n *Node) Replicate(id int, currentTerm uint64, entries []string) {
	ps := n.state.persistentState

	//Need local copy to mutate prev log index if occur inconsistent log
	n.state.stateLock.Lock()
	ci := n.state.commitIndex

	lastEntry := n.state.persistentState.LastEntry()
	prevIndex := lastEntry.Index - 1
	var prevTerm uint64
	if prevIndex == 0 {
		prevTerm = 0
	} else {
		prevTerm = ps.NthEntry(prevIndex).Term
	}

	argsLocalCopy := &proto.AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     strconv.Itoa(n.id),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: ci,
	}
	n.state.stateLock.Unlock()

	for {
		client := n.GetClient(id)
		if client == nil {
			return
		}
		reply, err := client.AppendEntries(context.Background(), argsLocalCopy)
		if err != nil {
			n.logger.Printf("Error on AppendEntries Call: %v. Requesting reconnection...\n", err)
			n.reconnChan <- id
			return
		}
		if !reply.Success {
			currentTerm := ps.CurrentTerm()
			if currentTerm < reply.Term { // Observed higher term
				n.logger.Printf("Error on appending. Observed higher term. Client - %v. Local term - %v. Observed term - %v. Transited to Follower\n", id, currentTerm, reply.Term)
				ps.SetTerm(reply.Term)
				n.state.role.TransitTo(Follower)
				return
			} else { // Seek over inconsistent log
				n.state.stateLock.Lock()
				argsLocalCopy.PrevLogIndex--
				entry := ps.NthEntry(argsLocalCopy.PrevLogIndex + 1)
				argsLocalCopy.PrevLogTerm = entry.Term
				argsLocalCopy.Entries = append([]string{entry.Command}, argsLocalCopy.Entries...) // Prepending. If rrewinding log appending all that follower currentry dont have
				n.logger.Printf("Command to send: %v\n", argsLocalCopy.Entries)
				n.logger.Printf("Trying PrevLogIndex:%v for node%v", argsLocalCopy.PrevLogIndex, id)
				n.state.stateLock.Unlock()
				continue // retry
			}
		} else {
			n.state.stateLock.Lock()
			n.state.matchIndex[id] = lastEntry.Index
			n.state.nextIndex[id] = lastEntry.Index + 1
			n.logger.Printf("Updated for node%v nextIndex - %v, matchIndex - %v\n", id, n.state.nextIndex[id], n.state.matchIndex[id])
			n.state.stateLock.Unlock()
			return
		}
	}
}

func (n *Node) CommitActivity(currentTerm uint64) {
	quorum := Quorum(n.ids)
	ps := n.state.persistentState
	ti := time.NewTicker(CommiterTick)
	for {
		<-ti.C
		lastIndex := ps.LastEntry().Index
		n.state.stateLock.Lock()
		N := n.state.commitIndex + 1
		n.logger.Printf("gathering commit quorum: N = %v, lastIndex = %v", N, lastIndex)
		n.logger.Printf("nextIndex[] state: %v", n.state.nextIndex)
		for index := N; index <= lastIndex; index++ {
			count := 0
			for id := range n.ids {
				if id != n.id && n.state.matchIndex[id] >= N {
					count++
				}
			}
			if count >= quorum && currentTerm == ps.NthEntry(index).Term {
				n.logger.Printf("Found commit quorum! New commitIndex - %v\n", N)
				n.state.commitIndex = N
			}
		}
		n.state.stateLock.Unlock()

		if n.state.role.Whoami() == Follower {
			n.logger.Println("Stop seeking commitIndex because transited to Follower")
			ti.Stop()
			return
		}
	}
}

func (n *Node) ApplyActivity() {
	ti := time.NewTicker(ApplyInterval)
	for {
		<-ti.C
		n.state.stateLock.Lock()
		ci := n.state.commitIndex
		n.state.stateLock.Unlock()

		// If encounder death, it will be equivalent to rewinding
		if ci > n.state.lastApplied {
			for ci > n.state.lastApplied {
				n.logger.Println("Found command covered by commit index! Applying...")
				n.state.lastApplied++
				// Apply to machine goes here
				n.logger.Printf("New applied index: %v\n", n.state.lastApplied)
				if n.state.role.Whoami() == Leader {
					val, loaded := n.applyRegistry.LoadAndDelete(n.state.lastApplied)
					if loaded { // If node was dead we are lost all clients. They will get timeout =)
						ac := val.(*AwaitableCommand)
						ac.c <- struct{}{} // Signal client that command is applied to at least quorum
					}
				}
			}
		}
	}
}

type AwaitableCommand struct {
	command string
	c       chan struct{} // Fullfil when applied
}

func NewAwaitableCommand(command string) *AwaitableCommand {
	c := make(chan struct{}, 1)
	return &AwaitableCommand{command: command, c: c}
}

func (n *Node) BootRun() {

	// RPC server settings
	l, err := net.Listen("tcp", ":606"+strconv.Itoa(n.id))
	if err != nil {
		n.logger.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterNodeServer(grpcServer, n)
	go grpcServer.Serve(l)

	// Frontend
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
				n.logger.Printf("Redirecting to Leader: node%v\n", leader)
				http.Redirect(w, r, "http://localhost:607"+leader+"/replicate", http.StatusMovedPermanently)
				return
			case Leader:
				n.clientChan <- ac
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
	// 	We will use that subroutine to suto-reconnect with retry rpc via channel signal
	//	if we encounter node death
	go n.ReconnectActivity()
	n.InitConnections()

	go n.ApplyActivity()
	go n.DefferedElection()

	n.logger.Printf("Listening on: :607" + strconv.Itoa(n.id))
	log.Panic(http.ListenAndServe(":607"+strconv.Itoa(n.id), nil))
}

func (n *Node) InitConnections() {
	for id := range n.ids {
		if id != n.id {
			n.reconnChan <- id
		}
	}
}
