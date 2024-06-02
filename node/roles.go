package node

import "sync/atomic"

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

type RoleStateMachine struct {
	state atomic.Uint32
}

func (t *RoleStateMachine) Exchange(to uint32) {
	t.state.Swap(to)
}
