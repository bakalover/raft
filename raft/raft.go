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
		Ctx        context.Context
		LogKey     string
		Me         string
		Neighbours []string
	}
)

func NewRaft(c *Config) *Raft {
	fileLog := persistence.NewFileLog(c.LogKey)
	raft := &Raft{
		strand:        infra.NewStrand(c.Ctx),
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

// RPC
func (r *Raft) Apply(args *machine.RSMcmd, reply *RaftReply) error {
	return nil
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
					Term:    r.term,
				}
				r.votedFor = args.Candidate
				return
			}
			if args.LastTerm == lastEntry.Term && args.LastIndex >= lastEntry.Index {
				awaitReply <- &RequestVoteReply{
					Granted: true,
					Term:    r.term,
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

// Phase 2
// RPC
func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	reply.Success = true
	r.resetTimer()
	return nil
}

func (r *Raft) goHeartbeat() {
	if r.whoAmI() != Leader {
		return
	}
	r.resetTimer()
	r.goAppendEntries([]machine.RSMcmd{}) // No entries
	time.AfterFunc(heartbeatBase*time.Millisecond, func() {
		r.goHeartbeat()
	})
}

func (r *Raft) goAppendEntries(entries []machine.RSMcmd) {
	do := func() {
		replyChannel := make(chan *AppendEntriesReply, len(r.neighbours))
		for peer, peerClient := range r.neighbours {
			go func() {
				reply := &AppendEntriesReply{}
				defer func() {
					replyChannel <- reply
				}()
				prevIndex := r.nextIndex[peer]
				args := &AppendEntriesArgs{
					Term:         r.term,
					Leader:       r.me,
					PrevTerm:     r.log.Term(prevIndex),
					PrevIndex:    prevIndex,
					Entries:      entries,
					LeaderCommit: r.commitIndex,
				}
				for {
					if err := peerClient.Call("Raft.AppendEntries", args, reply); err != nil {
						r.logger.Printf("Could not call AppendEntries on peer: [%s]. Error: [%s]. Requested reconnection", peer, err.Error())
						r.goReconnect(peer)
						return
					} else {
						if reply.Success {
							return
						}
						if reply.Term > r.term {
							r.logger.Printf("RequestVoteReply from peer: [%s] failed. Observed higher peer term: [%d]", peer, reply.Term)
							return // Observed another leader
						}
						// AppendEntries fails because of log inconsistency
						r.logger.Printf("RequestVoteReply from peer: [%s] failed. Observed peer's log inconsistency. NextIndexHint: [%d]", peer, reply.NextIndexHint)
						var additionalEntries []machine.RSMcmd
						for index := reply.NextIndexHint; index < args.PrevIndex; index++ { // Batch grab optimization?
							additionalEntries = append(additionalEntries, *r.log.At(index).RSMCmd)
						}
						args.PrevTerm = r.log.Term(reply.NextIndexHint)
						args.PrevIndex = reply.NextIndexHint
						args.Entries = append(args.Entries, additionalEntries...)
					}
				}
			}()
		}

		successCount := 0
		for range len(r.neighbours) {
			if reply := <-replyChannel; reply.Success {
				if reply.Term > r.term {
					r.term = reply.Term
					r.become(Follower) // Heartbeats will stop
				}
				successCount++
			}
		}
		if successCount >= r.quorum {
			return
		}
	}
	r.strand.Combine(do)
}
