package util

import (
	"context"
	"sync/atomic"
)

type (
	Strand interface {
		BlockExecute(c context.Context)
		Combine(c context.Context, f Task)
	}
	strandImpl struct {
		//TODO
		q       *Queue
		counter atomic.Uint64
	}
)

func NewStrand() Strand {
	return &strandImpl{
		//TODO
	}
}

func (s *strandImpl) BlockExecute(c context.Context) {
	//TODO
}

func (s *strandImpl) Combine(c context.Context, t Task) {
	//TODO
}
