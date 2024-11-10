package raft

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
		strand        infra.Strand // Synchronizes whole state below, except of one case inside Log
		me            string
		neighbours    map[string]*rpc.Client
		neighboursNum int
		electionTimer *time.Timer
		role          Role
		log           persistence.Log
		stateMachine  machine.StateMachine
		nextIndex     map[string]uint64
		matchIndex    map[string]uint64
		commitIndex   uint64
		lastApplied   uint64
		term          uint64
		leader        string
		votedFor      string
		quorum        int
		logger        *log.Logger
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
		strand:        infra.NewStrand(),
		me:            c.Me,
		neighbours:    make(map[string]*rpc.Client),
		neighboursNum: len(c.Neighbours),
		role:          Follower,
		log:           fileLog,
		term:          fileLog.LastEntry().Term, // No need in separate term in persistence??
		stateMachine:  machine.NewStateMachine(),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		quorum:        len(c.Neighbours)/2 + 1,
		logger:        log.New(os.Stdout, "INFO: ", log.Lmicroseconds|log.Lshortfile),
	}
	for _, n := range c.Neighbours {
		raft.neighbours[n] = nil
		raft.nextIndex[n] = 0  // Just store key
		raft.matchIndex[n] = 0 // Just store key
	}
	return raft
}

func (r *Raft) Run(ctx context.Context) {
	rpc.Register(r)
	rpc.HandleHTTP()

	wg := new(sync.WaitGroup)
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.logger.Println(http.ListenAndServe(r.me, nil))
	}()

	for peer := range r.neighbours {
		r.goReconnectBlocking(peer) // Init Connections
	}

	// First election
	// Timer is represented by rescheduling function that activates election process under Strand
	// Reseting this timer is the same as election postpone
	firstElection := func() {
		r.electionTimer = time.AfterFunc(timeout(), func() {
			r.goElection()
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

func (r *Raft) Park() {
	r.strand.Await()
}

func (r *Raft) resetTimer() {
	reset := func() {
		r.electionTimer.Reset(timeout()) // Safe, because that timer is created by AfterFunc
	}
	r.strand.Combine(reset)
}

func (r *Raft) goReconnectBlocking(peer string) {
	do := func() {
		for {
			client, err := rpc.DialHTTP("tcp", peer)
			if err != nil {
				r.logger.Printf("Could not reconnect to peer [%s].", peer)
				time.Sleep(1 * time.Second)
				continue
			}
			r.logger.Printf("Peer: [%s] connected!", peer)
			r.neighbours[peer] = client
			return
		}
	}
	r.strand.Combine(do)
}

func (r *Raft) goReconnect(peer string) {
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
	replyChannel := make(chan *RaftReply)
	do := func() {
		switch r.whoAmI() {
		case Follower: // Redirection to the leader
			go func() {
				var localReply RaftReply
				if err := r.neighbours[r.leader].Call("Raft.Apply", args, &localReply); err != nil {
					r.logger.Printf("Could not redirect request to [%s]. Error: [%s]", r.leader, err.Error())
					replyChannel <- &RaftReply{
						Error: err,
					}
				} else {
					replyChannel <- &localReply
				}
			}()
		case Candidate:
			replyChannel <- &RaftReply{
				Error: RetryableError{"wait election"},
			}
		case Leader:
			// TODO
		}
	}
	r.strand.Combine(do)
	realReply := <-replyChannel
	reply.Response = realReply.Response
	reply.Error = realReply.Error
	return nil
}

// Phase 1
// RPC
func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	awaitReply := make(chan *RequestVoteReply)
	do := func() {
		if args.Term > r.term {
			r.setTerm(args.Term)
			awaitReply <- &RequestVoteReply{
				Granted: true,
			}
			r.become(Follower)
			return
		}
		if args.Term < r.term {
			awaitReply <- &RequestVoteReply{
				Granted: false,
				Term:    r.term,
			}
			return
		}
		if r.votedFor == "" || r.votedFor == args.Candidate {
			lastEntry := r.log.LastEntry()
			if args.LastTerm >= lastEntry.Term {
				awaitReply <- &RequestVoteReply{
					Granted: true,
				}
				r.votedFor = args.Candidate
				return
			}
			if args.LastTerm == lastEntry.Term && args.LastIndex >= lastEntry.Index {
				awaitReply <- &RequestVoteReply{
					Granted: true,
				}
				return
			}
		}
		awaitReply <- &RequestVoteReply{
			Granted: false,
			Term:    r.term,
		}
	}
	r.strand.Combine(do)
	replyFromTask := <-awaitReply
	reply.Granted = replyFromTask.Granted
	reply.Term = replyFromTask.Term
	return nil
}

func (r *Raft) goElection() {
	replyChannel := make(chan *RequestVoteReply, r.neighboursNum)
	requestVote := func() {
		r.logger.Println("Election started!")
		r.become(Candidate)
		r.increaseTerm()
		r.votedFor = ""
		r.resetTimer()
		lastEntry := r.log.LastEntry()
		for peer, peerClient := range r.neighbours {
			args := &RequestVoteArgs{
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
					r.goReconnect(peer)
				} else {
					r.logger.Printf("RequestVoteReply from peer: [%s]. Granted: [%t]", peer, reply.Granted)
				}
			}()
		}
	}
	r.strand.Combine(requestVote)

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

	if votes >= r.quorum {
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
			r.goHeartbeat()
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
	replyChannel := make(chan *AppendEntriesReply)
	doAppend := func() {
		replyToChannel := &AppendEntriesReply{}
		defer func() {
			replyChannel <- replyToChannel
		}()
		replyToChannel.Term = r.term
		if r.term > args.Term {
			replyToChannel.Success = false
			return
		} else if r.term < args.Term {
			r.setTerm(args.Term)
		}
		localPrevTerm := r.log.At(args.PrevIndex).Term
		if localPrevTerm == 0 && args.PrevIndex != 0 { // Not Found
			replyToChannel.Success = false
			replyToChannel.NextIndexHint = r.log.LastEntry().Index + 1 // Lets help leader sent suitable start point
			return
		}
		r.resetTimer()
		if r.leader != args.Leader {
			r.leader = args.Leader
			r.logger.Printf("New Leader: [%s]", r.leader)
		}
		if localPrevTerm != args.PrevTerm {
			r.log.TrimS(args.PrevIndex)
		}
		r.log.Append(args.Entries, args.PrevIndex+1)
		replyToChannel.Success = true
		if args.LeaderCommit > r.commitIndex && len(args.Entries) == 0 {
			r.commitIndex = args.LeaderCommit
			return
		}
		if args.LeaderCommit > r.commitIndex && len(args.Entries) != 0 {
			r.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
			return
		}
		return
	}
	r.strand.Combine(doAppend)
	replyFromChannel := <-replyChannel
	reply.Success = replyFromChannel.Success
	reply.Term = replyFromChannel.Term
	reply.NextIndexHint = replyFromChannel.NextIndexHint
	return nil
}

func (r *Raft) goHeartbeat() {
	if r.whoAmI() != Leader {
		return
	}
	r.resetTimer()
	awaitPrevious := make(chan struct{})
	// No entries
	// Launch in separate goroutine to prevent strand recursive submit deadlock
	go func() {
		defer func() {
			awaitPrevious <- struct{}{}
		}()
		r.goAppendEntries(persistence.LogEntryPack{})
	}()
	time.AfterFunc(heartbeatBase*time.Millisecond, func() {
		<-awaitPrevious
		r.strand.Combine(r.goHeartbeat)
	})
}

func (r *Raft) goAppendEntries(entries persistence.LogEntryPack) {
	replyChannel := make(chan struct {
		Reply *AppendEntriesReply
		Peer  string
	}, r.neighboursNum)
	doRPC := func() {
		for peer, peerClient := range r.neighbours {
			go func() {
				var replyToChannel AppendEntriesReply
				defer func() {
					replyChannel <- struct {
						Reply *AppendEntriesReply
						Peer  string
					}{&replyToChannel, peer}
				}()
				prevIndex := r.nextIndex[peer] - 1
				args := &AppendEntriesArgs{
					Term:         r.term,
					Leader:       r.me,
					PrevTerm:     r.log.Term(prevIndex),
					PrevIndex:    prevIndex,
					Entries:      entries,
					LeaderCommit: r.commitIndex,
				}
				for {
					if err := peerClient.Call("Raft.AppendEntries", args, &replyToChannel); err != nil {
						r.logger.Printf("Could not call AppendEntries on peer: [%s]. Error: [%s]. Requested reconnection", peer, err.Error())
						r.goReconnect(peer)
						return
					} else {
						if replyToChannel.Success {
							return
						}
						if replyToChannel.Term > r.term { // Observed another leader
							r.logger.Printf("AppendEntries to peer: [%s] failed. Observed higher peer term: [%d]", peer, replyToChannel.Term)
							return
						}
						// AppendEntries fails because of log inconsistency
						r.logger.Printf("AppendEntries to peer: [%s] failed. Observed peer's log inconsistency. NextIndexHint: [%d]", peer, replyToChannel.NextIndexHint)
						var additionalEntries persistence.LogEntryPack
						for index := replyToChannel.NextIndexHint; index <= prevIndex; index++ { // Batch grab optimization?
							additionalEntries = append(additionalEntries, r.log.At(index))
						}
						args.PrevTerm = r.log.Term(replyToChannel.NextIndexHint - 1)
						args.PrevIndex = replyToChannel.NextIndexHint - 1
						args.Entries = append(args.Entries, additionalEntries...)
					}
				}
			}()
		}
	}
	r.strand.Combine(doRPC)

	successCount := 0
	backoffTerm := uint64(0) // Highest term observed from slaves (if they really are...)
	newNextIndex := make(map[string]uint64)
	for range r.neighboursNum {
		replyFromChannel := <-replyChannel
		if replyFromChannel.Reply.Success {
			successCount++
			newNextIndex[replyFromChannel.Peer] = replyFromChannel.Reply.NextIndexHint
		} else if replyFromChannel.Reply.Term > backoffTerm {
			backoffTerm = replyFromChannel.Reply.Term
		}
	}

	if successCount >= r.quorum {
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
