package infra

import (
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
		c    atomic.Int64
	}
)

func NewStrand() Strand {
	return &strandImpl{
		q: &Queue{},
	}
}

func CombineAndGet[ResultType any](s Strand, f func(chan<- ResultType)) <-chan ResultType {
	resultChannel := make(chan ResultType)
	wrappedTask := func() {
		defer func() {
			f(resultChannel)
		}()
	}
	s.Combine(wrappedTask)
	return resultChannel
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
	runtime.LockOSThread() // Turn off goroutine preemption to keep caches hot
	defer runtime.UnlockOSThread()
	count := int64(0)
	for b.IsNotEmpty() {
		b.Pop().Run() // All tasks run under Strand context
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
