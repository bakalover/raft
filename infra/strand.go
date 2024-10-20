package infra

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

type (
	Strand interface {
		Combine(f Task)
		Await()
	}
	strandImpl struct {
		q    *Queue
		refs sync.WaitGroup
		ctx  context.Context
		c    atomic.Int64
	}
)

func NewStrand(ctx context.Context) Strand {
	return &strandImpl{
		q:   &Queue{},
		ctx: ctx,
	}
}

func (s *strandImpl) Combine(t Task) {
	s.q.Append(FormNode(t, nil))
	if s.c.Add(1) == 1 {
		s.goSelf()
	}
}

func (s *strandImpl) Await() {
	s.refs.Wait()
}

func (s *strandImpl) runBatch() {
	done := s.runBlockingCPU(s.q.GrabAll())
	left := s.c.Add(-done)
	if left > 0 {
		s.goSelf()
	}

}

func (s *strandImpl) runBlockingCPU(b Batch) int64 {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	count := int64(0)
	for b.IsNotEmpty() {
		b.Pop().Run(s.ctx) // All tasks run under Strand context
		count++
	}
	return count
}

func (s *strandImpl) goSelf() {
	s.refs.Add(1)
	go func() {
		defer s.refs.Done()
		s.runBatch()
	}()
}
