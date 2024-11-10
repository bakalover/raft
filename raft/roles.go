package raft

type Role int

const (
	Follower = Role(iota)
	Candidate
	Leader
)

func (r Role) Repr() string {
	switch r {
	case 0:
		return "Follower"
	case 1:
		return "Candidate"
	case 2:
		return "Leader"
	}
	panic("Undefined role")
}
