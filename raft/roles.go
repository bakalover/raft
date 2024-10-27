package raft

type Role int

const (
	Follower = Role(iota)
	Candidate
	Leader
)
