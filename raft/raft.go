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
	Raft interface {
		Run(context.Context)
		Park()
		Apply(args *machine.RSMcmd, reply *RaftReply) error
	}

	raftImpl struct {
		strand        infra.Strand // Synchronizes whole state below, except of one case inside Log 
		me            string
		neighbours    map[string]*rpc.Client
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
		logger        *log.Logger
	}

	Config struct {
		Ctx        context.Context
		LogKey     string
		Me         string
		Neighbours []string
	}
)

func NewRaft(c *Config) Raft {
	raft := &raftImpl{
		strand:       infra.NewStrand(c.Ctx),
		me:           c.Me,
		neighbours:   make(map[string]*rpc.Client),
		role:         Follower,
		log:          persistence.NewFileLog(c.LogKey),
		stateMachine: machine.NewStateMachine(),
		nextIndex:    make(map[string]uint64),
		matchIndex:   make(map[string]uint64),
		logger:       log.New(os.Stdout, "INFO", log.Lmicroseconds|log.Lshortfile),
	}
	for _, n := range c.Neighbours {
		raft.neighbours[n] = nil
		raft.nextIndex[n] = 0  // Just store key
		raft.matchIndex[n] = 0 // Just store key
	}
	return raft
}

// RPC
func (r *raftImpl) Apply(args *machine.RSMcmd, reply *RaftReply) error {
	return nil
}

func (r *raftImpl) Run(ctx context.Context) {
	rpc.Register(r)
	rpc.HandleHTTP()

	wg := new(sync.WaitGroup)
	defer wg.Wait()

	go func() {
		defer wg.Done()
		r.logger.Println(http.ListenAndServe(r.me, nil))
	}()

	// First election
	// Timer is represented by rescheduling function that activates election process under Strand
	// Reseting this timer is the same as election postpone
	r.electionTimer = time.AfterFunc(timeout(), func() {
		r.goElection()
	})
}

func (r *raftImpl) quorum() int {
	return len(r.neighbours)/2 + 1
}

// base - 2xbase ms random timeout
func timeout() time.Duration {
	return time.Duration((timeoutBase + rand.Intn(timeoutBase))) * time.Millisecond
}

func (r *raftImpl) become(role Role) {
	r.role = role
}

func (r *raftImpl) whoAmI() Role {
	return r.role
}

func (r *raftImpl) increaseTerm() {
	r.term++
}

func (r *raftImpl) Park() {
	r.strand.Await()
}

func (r *raftImpl) resetTimer() {
	r.electionTimer.Reset(timeout()) // Safe, because that timer is created by AfterFunc
}

// Phase 1
// RPC
func RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

}

func (r *raftImpl) goElection() {
	do := func(ctx context.Context) {
		r.logger.Println("Election started")
		r.become(Candidate)
		r.increaseTerm()
		r.resetTimer()
		replyChannel := make(chan *RequestVoteReply, len(r.neighbours))
		lastEntry := r.log.At(persistence.LastEntry)

		for peer, peerClient := range r.neighbours {
			if peerClient == nil { // Actually happens only at first election
				r.goReconnect(peer)
				continue
			}
			args := &RequestVoteArgs{
				Term:      r.term,
				Candidate: r.me,
				LastTerm:  lastEntry.Term,
				LastIndex: lastEntry.Index,
			}
			go func() {
				reply := &RequestVoteReply{}
				defer func() {
					replyChannel <- reply
				}()
				if err := peerClient.Call("raftImpl.RequestVote", args, reply); err != nil {
					r.logger.Printf("Could not call RequestVote on peer: [%s]. Error: [%s]. Requested reconnection", peer, err.Error())
					r.goReconnect(peer)
				} else {
					r.logger.Printf("RequestVoteReply from peer: [%s]. Granted: [%t]", peer, reply.Granted)
				}
			}()
		}

		votes := 0
		for range len(r.neighbours) {
			if reply := <-replyChannel; reply.Granted {
				if reply.Term > r.term {
					r.term = reply.Term
				}
				votes++
			}
		}

		if votes >= r.quorum() {
			r.become(Leader)
			for peer := range r.nextIndex {
				r.nextIndex[peer] = lastEntry.Index + 1
				r.matchIndex[peer] = 0
			}
			r.goHeartbeat()
		}
	}
	r.strand.Combine(do)
}

func (r *raftImpl) goReconnect(peer string) {
	do := func(ctx context.Context) {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			r.logger.Printf("Could not reconnect to peer [%s].", peer)
		}
		r.logger.Printf("Peer: [%s] connected!", peer)
		r.neighbours[peer] = client
	}
	r.strand.Combine(do)
}

// Phase 2
// RPC
func AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (r *raftImpl) goHeartbeat() {
	if r.whoAmI() != Leader {
		return
	}
	r.goAppendEntries([]machine.RSMcmd{}) // No entries
	time.AfterFunc(heartbeatBase*time.Millisecond, func() {
		r.goHeartbeat()
	})
}

func (r *raftImpl) goAppendEntries(entries []machine.RSMcmd) {
	do := func(ctx context.Context) {
		replyChannel := make(chan *AppendEntriesReply, len(r.neighbours))
		for peer, peerClient := range r.neighbours {
			if peerClient == nil {
				r.goReconnect(peer)
				continue
			}
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
					if err := peerClient.Call("raftImpl.AppendEntries", args, reply); err != nil {
						r.logger.Printf("Could not call AppendEntries on peer: [%s]. Error: [%s]. Requested reconnection", peer, err.Error())
						r.goReconnect(peer)
						return
					} else {
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
					return // Success
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
		if successCount >= r.quorum() {
			return
		}
	}
	r.strand.Combine(do)
}
