package infra_test

import (
	"sync"
	"testing"

	"github.com/bakalover/raft/infra"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

const (
	goGroups   = 100
	gosInGroup = 65536
)

func TestQueue(t *testing.T) {
	t.Run("Just Work", func(t *testing.T) {
		q := &infra.Queue{}
		counter := 0
		q.Append(infra.FormNode(func() {
			counter++
		}, nil))
		q.Append(infra.FormNode(func() {
			counter++
		}, nil))
		q.Append(infra.FormNode(func() {
			counter++
		}, nil))
		b := q.GrabAll()
		for b.IsNotEmpty() {
			b.Pop().Run()
		}
		assert.Equal(t, 3, counter)
	})

	// TSAN required
	t.Run("Concurrent writes", func(t *testing.T) {
		q := &infra.Queue{}
		count := 0
		var wg sync.WaitGroup
		for range goGroups {
			wg.Add(gosInGroup)
			for range gosInGroup {
				go func() {
					defer wg.Done()
					q.Append(infra.FormNode(func() {
						count++
					}, nil))
				}()
			}
			wg.Wait()
			b := q.GrabAll()
			for b.IsNotEmpty() {
				b.Pop().Run()
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
		for range goGroups {
			for range gosInGroup {
				go func() {
					defer func() {
						sig <- struct{}{}
					}()
					q.Append(infra.FormNode(func() {
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
						b.Pop().Run()
					}

				}
				assert.Equal(t, gosInGroup, count)
				count = 0
			}()
			<-sigMain
		}
	})
}

func BenchmarkQueue(b *testing.B) {
	q := &infra.Queue{}
	count := 0
	sig := make(chan struct{})
	sigMain := make(chan struct{})
	for range gosInGroup {
		go func() {
			defer func() {
				sig <- struct{}{}
			}()
			for range b.N {
				q.Append(infra.FormNode(func() {
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
				b.Pop().Run()
			}

		}
		assert.Equal(b, gosInGroup*b.N, count)
		count = 0
	}()
	<-sigMain
}

func TestStrand(t *testing.T) {
	t.Run("Just Work", func(t *testing.T) {
		s := infra.NewStrand()
		crits := 0
		s.Combine(func() {
			crits++
		})
		s.Combine(func() {
			crits++
		})
		s.Combine(func() {
			crits++
		})
		s.Await()
		goleak.VerifyNone(t)
		assert.Equal(t, 3, crits)
	})

	// TSAN required
	t.Run("Highload", func(t *testing.T) {
		s := infra.NewStrand()
		crits := 0
		for range goGroups {
			for range gosInGroup {
				s.Combine(func() {
					crits++
				})
			}
			s.Await()
			goleak.VerifyNone(t)
			assert.Equal(t, gosInGroup, crits)
			crits = 0
		}
	})
}

func BenchmarkStrand(b *testing.B) {
	s := infra.NewStrand()
	crits := 0
	for range b.N {
		s.Combine(func() {
			crits++
		})
	}
	s.Await()
	assert.Equal(b, b.N, crits)
	crits = 0
}
