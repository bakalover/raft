package raft

import (
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"github.com/bakalover/raft/infra"
	"github.com/bakalover/raft/machine"
	"github.com/bakalover/raft/persistence"
)

const (
	timeoutBase   = 500
	heartbeatBase = 200
)

type (
	Raft struct {
		strand         infra.Strand // Synchronizes whole state below
		me             string
		neighbours     map[string]*rpc.Client
		neighboursNum  int
		electionTimer  *time.Timer
		role           Role
		log            persistence.Log
		stateMachine   machine.StateMachine
		nextIndex      map[string]uint64
		matchIndex     map[string]uint64
		commitIndex    uint64
		lastApplied    uint64
		term           uint64
		leader         string
		votedFor       string
		quorum         int
		logger         *log.Logger
		clientResponse []chan *RaftReply
	}

	Config struct {
		LogKey     string
		Me         string
		Neighbours []string
	}
)

func NewRaft(c *Config) *Raft {
	fileLog := persistence.NewFileLog(c.LogKey)
	raft := &Raft{
		strand:         infra.NewStrand(),
		me:             c.Me,
		neighbours:     make(map[string]*rpc.Client),
		neighboursNum:  len(c.Neighbours), // evade TSAN
		role:           Follower,
		log:            fileLog,
		term:           fileLog.LastEntry().Term, // No need in separate term in persistence??
		stateMachine:   machine.NewStateMachine(),
		nextIndex:      make(map[string]uint64),
		matchIndex:     make(map[string]uint64),
		quorum:         len(c.Neighbours)/2 + 1,
		logger:         log.New(os.Stdout, "INFO: ", log.Lmicroseconds|log.Lshortfile),
		clientResponse: make([]chan *RaftReply, 0),
	}
	for _, n := range c.Neighbours {
		// Just store keys in each map
		raft.neighbours[n] = nil
		raft.nextIndex[n] = 0
		raft.matchIndex[n] = 0
	}
	return raft
}

func (r *Raft) Run() {
	rpc.Register(r)
	rpc.HandleHTTP()

	awaitServer := make(chan struct{}, 1)
	defer func() { <-awaitServer }()

	go func() {
		defer close(awaitServer)
		r.logger.Println(http.ListenAndServe(r.me, nil))
	}()

	for peer := range r.neighbours {
		r.doReconnectBlocking(peer) // Init Connections
	}

	// First election
	// Timer is represented by rescheduling function
	firstElection := func() {
		r.electionTimer = time.AfterFunc(timeout(), func() {
			r.doElection()
		})
	}
	r.strand.Combine(firstElection)
}

// base - 3xbase ms random timeout
func timeout() time.Duration {
	return time.Duration((timeoutBase + 2*rand.Intn(timeoutBase))) * time.Millisecond
}

func (r *Raft) become(role Role) {
	prevRole := r.role
	r.role = role
	if prevRole != role {
		r.logger.Printf("Role changed: %s -> %s", prevRole.Repr(), role.Repr())
	}
}

func (r *Raft) whoAmI() Role {
	return r.role
}

func (r *Raft) increaseTerm() {
	r.setTerm(r.term + 1)
}

func (r *Raft) setTerm(newTerm uint64) {
	r.term = newTerm
	r.logger.Printf("New term: %d", r.term)
}

func (r *Raft) Park() { // Should success
	r.strand.Await()
}

func (r *Raft) doResetTimer() {
	do := func() {
		r.electionTimer.Reset(timeout()) // Safe, because that timer was created by AfterFunc
	}
	r.strand.Combine(do)
}

func (r *Raft) doReconnectBlocking(peer string) {
	do := func() {
		for {
			client, err := rpc.DialHTTP("tcp", peer)
			if err != nil {
				r.logger.Printf("Could not reconnect to peer [%s].", peer)
				time.Sleep(time.Second)
				continue
			}
			r.logger.Printf("Peer: [%s] connected!", peer)
			r.neighbours[peer] = client
			return
		}
	}
	r.strand.Combine(do)
}

func (r *Raft) doReconnect(peer string) {
	do := func() {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			r.logger.Printf("Could not reconnect to peer [%s].", peer)
			return
		}
		r.logger.Printf("Peer: [%s] connected!", peer)
		r.neighbours[peer] = client
	}
	r.strand.Combine(do)
}

// RPC frontend
func (r *Raft) Apply(args machine.RSMcmd, reply *RaftReply) error {
	do := func(replyChannel chan<- *RaftReply) {
		switch r.whoAmI() {
		case Follower: // Redirection to the leader
			r.logger.Printf("I am not a leader. Redirecting to [%s]", r.leader)
			go func() {
				var proxyReply RaftReply
				proxyReply.Leader = r.leader
				defer func() {
					replyChannel <- &proxyReply
				}()
				if err := r.neighbours[r.leader].Call("Raft.Apply", args, &proxyReply); err != nil {
					r.logger.Printf("Could not redirect request to [%s]. Error: [%s]", r.leader, err.Error())
					proxyReply = RaftReply{
						Error: err,
					}
					r.doReconnect(r.leader)
				}
			}()
		case Candidate:
			replyChannel <- &RaftReply{
				Error: RetryableError{"wait election"},
			}
		case Leader:
			lastEntry := r.log.LastEntry()
			index := lastEntry.Index + 1
			term := r.term

			ch := make(chan *RaftReply, 1)
			r.clientResponse = append(r.clientResponse, ch)

			// Now we should release Strand so Raft can proceed
			// Client will await on doReply channel
			// r.clientResponse[index] will recieve value when according commitIndex's majority advance
			go func() {
				pack := persistence.LogEntryPack{ // Batch time window optimization or another Strand!
					{
						Term:   term,
						Index:  index,
						RSMCmd: args,
					},
				}
				r.log.Append(pack, index)
				r.doAppendEntries(pack)

				replyChannel <- <-ch
			}()
		}
	}
	doReply := <-infra.CombineAndGet(r.strand, do)
	reply.Response = doReply.Response
	reply.Error = doReply.Error
	return nil
}

// Phase 1
// RPC
func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	do := func(replyChannel chan<- *RequestVoteReply) {
		if args.Term > r.term {
			r.setTerm(args.Term)
			replyChannel <- &RequestVoteReply{
				Granted: true,
			}
			r.become(Follower)
			return
		}
		if args.Term < r.term {
			replyChannel <- &RequestVoteReply{
				Granted: false,
				Term:    r.term,
			}
			return
		}
		if r.votedFor == "" || r.votedFor == args.Candidate {
			lastEntry := r.log.LastEntry()
			if args.LastTerm >= lastEntry.Term {
				replyChannel <- &RequestVoteReply{
					Granted: true,
				}
				r.votedFor = args.Candidate
				return
			}
			if args.LastTerm == lastEntry.Term && args.LastIndex >= lastEntry.Index {
				replyChannel <- &RequestVoteReply{
					Granted: true,
				}
				return
			}
		}
		replyChannel <- &RequestVoteReply{
			Granted: false,
			Term:    r.term,
		}
	}
	doReply := <-infra.CombineAndGet(r.strand, do)
	reply.Granted = doReply.Granted
	reply.Term = doReply.Term
	return nil
}

func (r *Raft) doElection() {
	do := func(replyChannel chan<- *RequestVoteReply) {
		r.logger.Println("Election started!")
		r.become(Candidate)
		r.increaseTerm()
		r.votedFor = ""
		r.doResetTimer()
		lastEntry := r.log.LastEntry()
		for peer, peerClient := range r.neighbours {
			args := RequestVoteArgs{
				Term:      r.term,
				Candidate: r.me,
				LastTerm:  lastEntry.Term,
				LastIndex: lastEntry.Index,
			}
			go func() {
				var reply RequestVoteReply
				defer func() {
					replyChannel <- &reply
				}()
				if err := peerClient.Call("Raft.RequestVote", args, &reply); err != nil {
					r.logger.Printf("Could not call RequestVote on peer: [%s]. Error: [%s]. Requested reconnection", peer, err.Error())
					r.doReconnect(peer)
				} else {
					r.logger.Printf("RequestVoteReply from peer: [%s]. Granted: [%t]", peer, reply.Granted)
				}
			}()
		}
	}

	replyChannel := infra.CombineAndGet(r.strand, do)

	votes := 0
	backoffTerm := uint64(0) // Highest term observed from rejecting nodes
	for range r.neighboursNum {
		reply := <-replyChannel
		if reply.Granted {
			votes++
		} else {
			if reply.Term > backoffTerm {
				backoffTerm = reply.Term
			}
		}
	}

	if votes >= r.quorum-1 {
		changeToLeader := func() {
			if r.whoAmI() == Follower { // Someone took advantage on AppendEntries
				return
			}
			r.become(Leader)
			lastIndex := r.log.LastEntry().Index
			for peer := range r.nextIndex {
				r.nextIndex[peer] = lastIndex + 1
				r.matchIndex[peer] = 0
			}
			r.doHeartbeat()
		}
		r.strand.Combine(changeToLeader)
	} else {
		backToFollower := func() {
			r.become(Follower)
			if backoffTerm > r.term {
				r.setTerm(backoffTerm)
			}
		}
		r.strand.Combine(backToFollower)
	}
}

// Phase 2
// RPC
func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.logger.Printf("Args Recv: [%+v]", args)
	do := func(replyChannel chan<- *AppendEntriesReply) {
		replyToChannel := new(AppendEntriesReply)
		defer func() {
			replyChannel <- replyToChannel
		}()
		replyToChannel.Term = r.term
		if r.term > args.Term {
			replyToChannel.NextIndexHint = r.log.LastEntry().Index + 1 // Lets help leader and send back suitable start point
			replyToChannel.Success = false
			return
		} else if r.term < args.Term {
			r.setTerm(args.Term)
		}
		localPrevTerm := r.log.At(args.PrevIndex).Term
		if localPrevTerm == 0 && r.log.Size() != 0 && args.PrevIndex != 0 { // Not Found
			replyToChannel.NextIndexHint = r.log.LastEntry().Index + 1
			replyToChannel.Success = false
			return
		}
		r.doResetTimer()
		if r.leader != args.Leader {
			r.leader = args.Leader
		}
		if r.log.Size() == 0 && args.PrevIndex != 0 {
			replyToChannel.Success = false
			replyToChannel.NextIndexHint = 1
			return
		}
		if localPrevTerm != args.PrevTerm && args.PrevIndex != 0 {
			r.log.TrimS(args.PrevIndex)
		}
		r.log.Append(args.Entries, args.PrevIndex+1)
		replyToChannel.Success = true
		replyToChannel.NextIndexHint = r.log.LastEntry().Index + 1 // Renew hint
		if args.LeaderCommit > r.commitIndex && len(args.Entries) == 0 {
			for i := r.commitIndex + 1; i <= args.LeaderCommit; i++ {
				r.applyToStateMachine(i) // Safe
			}
			r.commitIndex = args.LeaderCommit
			return
		}
		if args.LeaderCommit > r.commitIndex && len(args.Entries) != 0 {
			r.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
			return
		}
		return
	}
	doReply := <-infra.CombineAndGet(r.strand, do)
	reply.Success = doReply.Success
	reply.Term = doReply.Term
	reply.NextIndexHint = doReply.NextIndexHint
	return nil
}

// Under strand context
func (r *Raft) doHeartbeat() {
	if r.whoAmI() != Leader {
		return
	}
	r.doResetTimer()
	awaitPrevious := make(chan struct{})

	for i := r.advanceCommitIndex() + 1; i <= r.commitIndex; i++ {
		result := r.applyToStateMachine(i)
		r.respondClient(result) // Raft's commit succeed
	}

	// No entries
	// Launch in separate goroutine to prevent strand recursive submit deadlock
	go func() {
		defer close(awaitPrevious)
		r.doAppendEntries(persistence.EmptyPack())
	}()

	time.AfterFunc(heartbeatBase*time.Millisecond, func() {
		<-awaitPrevious
		r.strand.Combine(r.doHeartbeat)
	})
}

// Under strand context
func (r *Raft) applyToStateMachine(index uint64) (result machine.MachineType) {
	entry := r.log.At(index) // Should be safe, beacause index is commited
	result = r.stateMachine.Apply(entry.RSMCmd)
	r.logger.Printf("New state machine value: [%d]", result)
	return result
}

// Under strand context
func (r *Raft) respondClient(response machine.MachineType) {
	var responseChannel chan *RaftReply
	if len(r.clientResponse) == 0 { // Rewind wait on init -> need to fix =( + snapshots
		return
	}
	responseChannel, r.clientResponse = r.clientResponse[0], r.clientResponse[1:] // Pop first client
	responseChannel <- &RaftReply{
		Response: response,
	}
}

// Under strand context
func (r *Raft) advanceCommitIndex() (prevCommitIndex uint64) {
	prev := r.commitIndex
	N := r.commitIndex + 1
	for {
		peerAdvanceCount := 0
		for _, index := range r.matchIndex {
			if index >= N {
				peerAdvanceCount++
			}
		}

		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm
		// Note: all previos commited entries must gather quorum up to last commited one
		if peerAdvanceCount < r.quorum {
			break
		} else {
			if r.log.Term(N) == r.term {
				r.commitIndex = N
			}
		}
		N++
	}
	if prev != r.commitIndex {
		r.logger.Printf("Advanced commitIndex: [%d] -> [%d]", prev, r.commitIndex)
	}
	return prev
}

func (r *Raft) doAppendEntries(entries persistence.LogEntryPack) {
	do := func(replyChannel chan<- struct {
		Reply *AppendEntriesReply
		Peer  string
	}) {
		for peer, peerClient := range r.neighbours {
			go func() {
				var replyToChannel AppendEntriesReply
				defer func() {
					replyChannel <- struct {
						Reply *AppendEntriesReply
						Peer  string
					}{&replyToChannel, peer}
				}()
				precedingEntry := <-infra.CombineAndGet(r.strand, func(replyChannel chan<- *persistence.LogEntry) {
					if len(entries) == 0 { // heartbeat
						replyChannel <- r.log.LastEntry()
					} else { // replication, leader already appended new entry, so we should step back on 1
						size := r.log.Size()
						if size <= 1 { // At(0) [aka last entry] == At(1) iff r.log.Size() == 1
							replyChannel <- &persistence.LogEntry{}
						} else {
							at := r.log.At(size - 1)
							log.Printf("%+v", at)
							replyChannel <- at // entry before last (just appended) entry
						}
					}
				})
				args := AppendEntriesArgs{
					Term:         r.term,
					Leader:       r.me,
					PrevTerm:     precedingEntry.Term,
					PrevIndex:    precedingEntry.Index,
					Entries:      entries,
					LeaderCommit: r.commitIndex,
				}
				for {
					if err := peerClient.Call("Raft.AppendEntries", args, &replyToChannel); err != nil {
						r.logger.Printf("Could not call AppendEntries on peer: [%s]. Error: [%s]. Requested reconnection", peer, err.Error())
						r.doReconnect(peer)
						return
					} else {
						if replyToChannel.Success {
							return
						}
						if replyToChannel.Term > r.term { // Observed another leader
							r.logger.Printf("AppendEntries to peer: [%s] failed. Observed higher peer term: [%d]", peer, replyToChannel.Term)
							return
						}
						r.logger.Println(replyToChannel)
						// AppendEntries fails because of log inconsistency
						r.logger.Printf("AppendEntries to peer: [%s] failed. Observed peer's log inconsistency. NextIndexHint: [%d]", peer, replyToChannel.NextIndexHint)
						var additionalEntries persistence.LogEntryPack
						for index := replyToChannel.NextIndexHint; index <= precedingEntry.Index; index++ { // Batch grab optimization?
							entry := <-infra.CombineAndGet(r.strand, func(replyChannel chan<- *persistence.LogEntry) { replyChannel <- r.log.At(index) })
							additionalEntries = append(additionalEntries, *entry)
						}
						newPrevTerm := <-infra.CombineAndGet(r.strand, func(replyChannel chan<- uint64) { replyChannel <- r.log.Term(replyToChannel.NextIndexHint - 1) })
						args.PrevTerm = newPrevTerm
						args.PrevIndex = replyToChannel.NextIndexHint - 1
						args.Entries = append(args.Entries, additionalEntries...)
					}
				}
			}()
		}
	}

	replyChannel := infra.CombineAndGet(r.strand, do)

	successCount := 0
	backoffTerm := uint64(0) // Highest term observed from slaves (if they really are...)
	newNextIndex := make(map[string]uint64)
	for range r.neighboursNum {
		doReply := <-replyChannel
		if doReply.Reply.Success {
			successCount++
			newNextIndex[doReply.Peer] = doReply.Reply.NextIndexHint
		} else if doReply.Reply.Term > backoffTerm {
			backoffTerm = doReply.Reply.Term
		}
	}
	if successCount >= r.quorum-1 {
		updatePeerState := func() {
			for peer, index := range newNextIndex {
				r.nextIndex[peer] = index
				r.matchIndex[peer] = index - 1
			}
		}
		r.strand.Combine(updatePeerState)
		return
	} else {
		backToFollower := func() {
			r.term = backoffTerm
			r.become(Follower) // Heartbeats will stop
		}
		r.strand.Combine(backToFollower)
	}
}
