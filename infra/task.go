package infra

import (
	"sync/atomic"
)

type (
	Task          = func()
	Batch         struct{ head *Node }
	Node          struct {
		t    Task
		Next *Node
	}

	Queue struct {
		head atomic.Pointer[Node]
	}
)

func FormNode(t Task, next *Node) *Node {
	return &Node{t, next}
}

func (n *Node) Run() {
	n.t()
}

func (b *Batch) IsNotEmpty() bool {
	return b.head != nil
}

func (b *Batch) Pop() *Node {
	ret := b.head
	b.head = b.head.Next
	return ret
}

func (q *Queue) Append(n *Node) {
	for {
		n.Next = q.head.Load()
		if q.head.CompareAndSwap(n.Next, n) {
			return
		}
	}
}

func (q *Queue) GrabAll() Batch {
	head := q.head.Swap(nil)
	if head == nil {
		return Batch{}
	}
	return Batch{q.reverse(head)}
}

func (q *Queue) reverse(head *Node) *Node {
	ret := FormNode(head.t, nil)
	head = head.Next
	for head != nil {
		ret = FormNode(head.t, ret)
		head = head.Next
	}
	return ret
}
