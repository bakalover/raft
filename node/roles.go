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

func (t *RoleStateMachine) Exchange(to uint32) {
	t.state.Swap(to)
}

func (t *RoleStateMachine) Load() uint32 {
	return t.state.Load()
}
