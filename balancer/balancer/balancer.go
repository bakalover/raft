package balancer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os/exec"
	"sync"

	"github.com/bakalover/raft/machine"
	"github.com/bakalover/raft/raft"
)

type (
	Strategy uint8

	IBalancer interface {
		Run()
		Balance(machine.RSMcmd) (error, string)
		Reconnect(peer string)
		SetStrategy(Strategy)
		SetAddrs(addrs []string)
		SetLeader(leader string)
		Destroy()
	}

	balancer struct {
		mu          sync.Mutex
		cmds        sync.WaitGroup
		cancel      context.CancelFunc
		leaderCache *rpc.Client
		peers       []*rpc.Client
		addrs       []string
		s           Strategy
		pos         int
	}
)

const (
	RoundRobin = Strategy(iota)
	LeaderRedirection
	Random
)

func NewBalancer() IBalancer {
	return &balancer{}
}

func (b *balancer) neighbours(addr string) []string {
	var result []string
	for _, check := range b.addrs {
		if addr != check {
			result = append(result, check)
		}
	}
	return result
}

func (b *balancer) Run() {
	b.mu.Lock()
	defer b.mu.Unlock()

	ctx, kill := context.WithCancel(context.Background())
	b.cancel = kill

	b.cmds.Add(len(b.addrs))
	for i, addr := range b.addrs {
		neighbours := b.neighbours(addr)
		args := []string{"run", fmt.Sprintf("%d", i+1), addr}
		args = append(args, neighbours...)
		cmd := exec.CommandContext(ctx, "go", args...)
		go func() {
			defer b.cmds.Done()
			log.Printf("Cmd done with: [%s]", cmd.Run().Error())
		}()
	}
}

func (b *balancer) Destroy() {
	if b.cancel != nil {
		b.cancel()
		b.cmds.Wait()
	}
}

func (b *balancer) Balance(request machine.RSMcmd) (error, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	reply := new(raft.RaftReply)

	var (
		peer *rpc.Client
	)

	switch b.s {
	case RoundRobin:
		peer = b.peers[b.pos]
		b.pos++
	case LeaderRedirection:
		if b.leaderCache == nil {
			peer = b.peers[b.pos]
			b.pos++
		} else {
			peer = b.leaderCache
		}
	case Random:
		peer = b.peers[rand.Intn(len(b.peers))]
	}
	log.Printf("%+v", reply)
	return peer.Call("Raft.Apply", request, reply), reply.Leader
}

func (b *balancer) SetStrategy(s Strategy) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.s = s
}

func (b *balancer) SetAddrs(addrs []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.Destroy()

	b.addrs = addrs
	b.peers = make([]*rpc.Client, len(addrs))
	for i, peer := range addrs {
		b.connect(i, peer)
	}
}

func (b *balancer) connect(pos int, addr string) {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Printf("Could not connect addr [%s]. Error: [%s]", addr, err.Error())
		return
	}
	b.peers[pos] = client
}

func (b *balancer) SetLeader(leader string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, addr := range b.addrs {
		if addr == leader {
			b.leaderCache = b.peers[i]
		}
	}
}

func (b *balancer) Reconnect(peer string) {
	for i, addr := range b.addrs {
		if peer == addr {
			b.connect(i, addr)
		}
	}
}
