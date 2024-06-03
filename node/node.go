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
	// Protect rpcClients
	mu sync.Mutex
	// Transfer info about which rpc connection is needed to be established again
	rpcC          chan int
	rpcClients    []*rpc.Client
	state         *State
	electionTimer time.Timer
}

func NewNode(id int, ids int) *Node {
	countWithoutMe := ids - 1
	nextIndex := make([]uint64, countWithoutMe)
	matchIndex := make([]uint64, countWithoutMe)
	rpcClients := make([]*rpc.Client, countWithoutMe)
	rpcC := make(chan int)

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
		id:         id,
		ids:        ids,
		rpcClients: rpcClients,
		rpcC:       rpcC,
		state:      state,
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

// Recieve recconnection signals via chan
func (n *Node) ConnectRPC() {
	for id := range n.rpcC {
		var client *rpc.Client
		var err error
		for client, err = rpc.Dial("tcp", "node"+string(id)+":8080"); err != nil; {
			log.Println("error connecting to server node%v via RPC", id)
			time.Sleep(5 * time.Millisecond)
		}
		n.mu.Lock()
		n.rpcClients[id] = client
		n.mu.Unlock()
	}
}

func (n *Node) ElectionProcess() {
	quorum := n.ids/2 + 1 // Includes caller (NO CHANGE IT FIRSTRLY VOTE FOR SELF THEN TIMNER THEN OTHERS); n.ids % 2 == 1

	ps := n.state.persistentState

	for {
		electedC := make(chan struct{}) // Signals that quorum was fulfilled

		select {
		case <-n.electionTimer.C:

		case <-electedC:
			//TODO
		}

		ps.IncremenTerm()
		n.state.role.Exchange(Candidate)

		// Buffer prevents deadlock(aka memory leak):
		// in case when quorum is gathered and there more granted votes
		// Checkout (!!)
		countVote := make(chan struct{}, n.ids)
		
		n.ResetElectionTimer()

		for id := range n.ids {
			term := ps.CurrentTerm()
			last := ps.LastEntry()
			var lastLogTerm uint64
			var lastLogIndex uint64

			if last == nil {
				lastLogIndex = 0
				lastLogTerm = 0
			} else {
				lastLogIndex = last.Index
				lastLogTerm = last.Term
			}

			args := &RequestVoteArgs{
				term:         term,
				candidateId:  string(n.id),
				lastLogIndex: lastLogIndex,
				lastLogTerm:  lastLogTerm,
			}

			var wg sync.WaitGroup
			wg.Add(quorum)

			go func(id int) {
				var reply RequestVoteResult
				n.rpcClients[id].Call("Node.RequestVote", args, &reply) // Retry if fail???????
				if reply.voteGranted {
					countVote <- struct{}{} // (!!)
				} else {
					if term < reply.term {
						ps.SetTerm(reply.term)
					}
				}
			}(id)
		}

		go func() {
			gotVotes := 0
		loop:
			for {
				select {
				case <-countVote:
					gotVotes++
					if gotVotes >= quorum {
						electedC <- struct{}{}
						break loop
					}
				case <-n.electionTimer.C:
					break loop
				}
			}
		}()
	}
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

	//	First of all we need to establish rpc connection
	// 	with all nodes (incuding caller)
	// 	We will use that subroutine to reconnect rpc via channel signal
	//	if we encounter node death
	go n.ConnectRPC()

	for id := range n.ids {
		n.rpcC <- id
	}

	n.ResetElectionTimer()
	go n.ElectionProcess()
}
