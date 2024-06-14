package node

import "sync/atomic"

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type RoleStateMachine struct {
	state atomic.Uint32
}

func (t *RoleStateMachine) TransitTo(to uint32) {
	t.state.Swap(to)
}

func (t *RoleStateMachine) Whoami() uint32 {
	return t.state.Load()
}
