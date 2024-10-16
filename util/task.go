package util

import "context"

type (
	Task = func(context.Context)
	Node struct {
		t    Task
		c    context.Context
		Prev *Node
	}

	Queue struct {
		Head *Node
		// TODO SYNC
	}
)

func (n *Node) Run() {
	n.t(n.c)
}

func (q *Queue) Append(n *Node) {
	//TODO
}

func (q *Queue) GrabAll() []*Node {
	//TODO
	return nil
}
