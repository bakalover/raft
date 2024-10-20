package infra_test

import (
	"context"
	"sync"
	"testing"

	"github.com/bakalover/raft/infra"
	"github.com/stretchr/testify/assert"
)

const (
	goGroups   = 100
	gosInGroup = 65536
)

func TestQueue(t *testing.T) {
	t.Run("Just Work", func(t *testing.T) {
		q := &infra.Queue{}
		counter := 0
		q.Append(infra.FormNode(func(ctx context.Context) {
			counter++
		}, nil))
		q.Append(infra.FormNode(func(ctx context.Context) {
			counter++
		}, nil))
		q.Append(infra.FormNode(func(ctx context.Context) {
			counter++
		}, nil))
		b := q.GrabAll()
		for b.IsNotEmpty() {
			b.Pop().Run(context.Background())
		}
		assert.Equal(t, 3, counter)
	})

	// TSAN required
	t.Run("Concurrent writes", func(t *testing.T) {
		q := &infra.Queue{}
		count := 0
		var wg sync.WaitGroup
		for goGroup := 0; goGroup < goGroups; goGroup++ {
			wg.Add(gosInGroup)
			for gos := 0; gos < gosInGroup; gos++ {
				go func() {
					defer wg.Done()
					q.Append(infra.FormNode(func(ctx context.Context) {
						count++
					}, nil))
				}()
			}
			wg.Wait()
			b := q.GrabAll()
			for b.IsNotEmpty() {
				b.Pop().Run(context.Background())
			}
			assert.Equal(t, gosInGroup, count)
			count = 0
		}
	})

	// TSAN required
	t.Run("Concurrent reads/writes", func(t *testing.T) {
		q := &infra.Queue{}
		count := 0
		sig := make(chan struct{})
		sigMain := make(chan struct{})
		for goGroup := 0; goGroup < goGroups; goGroup++ {
			for gos := 0; gos < gosInGroup; gos++ {
				go func() {
					defer func() {
						sig <- struct{}{}
					}()
					q.Append(infra.FormNode(func(ctx context.Context) {
						count++
					}, nil))
				}()
			}
			go func() {
				defer func() {
					sigMain <- struct{}{}
				}()
				done := 0
			loop:
				for {
					select {
					case <-sig:
						done++
						if done == gosInGroup {
							break loop
						}
					default:
					}
					b := q.GrabAll()
					for b.IsNotEmpty() {
						b.Pop().Run(context.Background())
					}

				}
				assert.Equal(t, gosInGroup, count)
				count = 0
			}()
			<-sigMain
		}
	})
}

// TSAN required
func BenchmarkQueue(b *testing.B) {
	q := &infra.Queue{}
	count := 0
	sig := make(chan struct{})
	sigMain := make(chan struct{})
	for gos := 0; gos < gosInGroup; gos++ {
		go func() {
			defer func() {
				sig <- struct{}{}
			}()
			for i := 0; i < b.N; i++ {
				q.Append(infra.FormNode(func(ctx context.Context) {
					count++
				}, nil))
			}
		}()
	}
	go func() {
		defer func() {
			sigMain <- struct{}{}
		}()
		done := 0
	loop:
		for {
			select {
			case <-sig:
				done++
				if done == gosInGroup {
					break loop
				}
			default:
			}
			b := q.GrabAll()
			for b.IsNotEmpty() {
				b.Pop().Run(context.Background())
			}

		}
		assert.Equal(b, gosInGroup*b.N, count)
		count = 0
	}()
	<-sigMain
}
