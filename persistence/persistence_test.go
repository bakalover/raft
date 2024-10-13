package persistence_test

import (
	"testing"

	"github.com/bakalover/raft/persistence"
	"github.com/bakalover/raft/rsm"
	"github.com/stretchr/testify/assert"
)

func TestFileLog(t *testing.T) {
	t.Run("Just Work", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		assert.Zero(t, log.Size())
		log.Append([]*persistence.LogEntry{
			{
				Term: 69,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
		}, 1)
		assert.Equal(t, log.Size(), uint64(1))
		assert.Equal(t, log.At(1).RSMCmd.CMDType, rsm.Add)
		assert.Equal(t, log.At(persistence.LastEntry).RSMCmd.CMDType, rsm.Add)
		assert.Equal(t, log.Term(1), uint64(69))
		assert.Equal(t, log.LastTerm(), uint64(69))
	})

	t.Run("Batch Append", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		assert.Zero(t, log.Size())
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.Append([]*persistence.LogEntry{
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 444,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 3)
		assert.Equal(t, log.Size(), uint64(4))
		assert.Equal(t, log.At(1).Term, uint64(111))
		assert.Equal(t, log.At(2).Term, uint64(222))
		assert.Equal(t, log.At(3).Term, uint64(333))
		assert.Equal(t, log.At(4).Term, uint64(444))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(444))
		assert.Equal(t, log.LastTerm(), uint64(444))
		assert.Equal(t, log.Term(1), uint64(111))
		assert.Equal(t, log.Term(2), uint64(222))
		assert.Equal(t, log.Term(3), uint64(333))
		assert.Equal(t, log.Term(4), uint64(444))
	})

	t.Run("Batch Append With Intersection", func(t *testing.T) {
		//Note: Raft does not have that semantic
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		assert.Zero(t, log.Size())
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.Append([]*persistence.LogEntry{
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 444,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 2)
		assert.Equal(t, log.Size(), uint64(3))
		assert.Equal(t, log.At(1).Term, uint64(111))
		assert.Equal(t, log.At(2).Term, uint64(333))
		assert.Equal(t, log.At(3).Term, uint64(444))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(444))
		assert.Equal(t, log.LastTerm(), uint64(444))
		assert.Equal(t, log.Term(1), uint64(111))
		assert.Equal(t, log.Term(2), uint64(333))
		assert.Equal(t, log.Term(3), uint64(444))
	})

	t.Run("Election", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		assert.Equal(t, log.LastTerm(), uint64(0))
	})

	t.Run("TrimP empty", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		assert.Zero(t, log.Size())
		log.TrimP(0)
		assert.Zero(t, log.Size())
	})

	t.Run("TrimP", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 444,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimP(2)
		assert.Equal(t, log.Size(), uint64(2))
		assert.Equal(t, log.At(1).Term, uint64(333))
		assert.Equal(t, log.At(2).Term, uint64(444))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(444))
		assert.Equal(t, log.LastTerm(), uint64(444))
		assert.Equal(t, log.Term(1), uint64(333))
		assert.Equal(t, log.Term(2), uint64(444))
	})

	t.Run("TrimP Append TrimP", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimP(1)
		log.Append([]*persistence.LogEntry{
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 444,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 2)
		assert.Equal(t, log.Size(), uint64(3))
		assert.Equal(t, log.At(1).Term, uint64(222))
		assert.Equal(t, log.At(2).Term, uint64(333))
		assert.Equal(t, log.At(3).Term, uint64(444))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(444))
		assert.Equal(t, log.LastTerm(), uint64(444))
		assert.Equal(t, log.Term(1), uint64(222))
		assert.Equal(t, log.Term(2), uint64(333))
		assert.Equal(t, log.Term(3), uint64(444))
	})

	t.Run("TrimP x2", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 444,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimP(1)
		log.TrimP(1)
		assert.Equal(t, log.Size(), uint64(2))
		assert.Equal(t, log.At(1).Term, uint64(333))
		assert.Equal(t, log.At(2).Term, uint64(444))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(444))
		assert.Equal(t, log.LastTerm(), uint64(444))
		assert.Equal(t, log.Term(1), uint64(333))
		assert.Equal(t, log.Term(2), uint64(444))
	})

	t.Run("TrimP All", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 444,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimP(4)
		assert.Equal(t, log.Size(), uint64(0))
		assert.Equal(t, log.LastTerm(), uint64(0))
	})

	t.Run("TrimS", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimS(2)
		assert.Equal(t, log.Size(), uint64(1))
		assert.Equal(t, log.At(1).Term, uint64(111))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(111))
		assert.Equal(t, log.LastTerm(), uint64(111))
		assert.Equal(t, log.Term(1), uint64(111))
	})

	t.Run("TrimS x2", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimS(3)
		log.TrimS(2)
		assert.Equal(t, log.Size(), uint64(1))
		assert.Equal(t, log.At(1).Term, uint64(111))
		assert.Equal(t, log.At(persistence.LastEntry).Term, uint64(111))
		assert.Equal(t, log.LastTerm(), uint64(111))
		assert.Equal(t, log.Term(1), uint64(111))
	})

	t.Run("TrimS All", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.Append([]*persistence.LogEntry{
			{
				Term: 111,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Add,
					Xid: rsm.Xid{
						Client: "1",
						Index:  0,
					},
				},
			},
			{
				Term: 222,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
			{
				Term: 333,
				RSMCmd: &rsm.RSMCmd{
					CMDType: rsm.Sub,
					Xid: rsm.Xid{
						Client: "2",
						Index:  0,
					},
				},
			},
		}, 1)
		log.TrimS(1)
		assert.Equal(t, log.Size(), uint64(0))
		assert.Equal(t, log.LastTerm(), uint64(0))
	})

	t.Run("TrimS on Empty log", func(t *testing.T) {
		log := persistence.NewFileLog("./test")
		defer log.Destroy()
		log.TrimS(1)
		assert.Equal(t, log.Size(), uint64(0))
		assert.Equal(t, log.LastTerm(), uint64(0))
	})
}

func BenchmarkAppend(t *testing.B) {
	log := persistence.NewFileLog("./test")
	defer log.Destroy()
	e := &persistence.LogEntry{
		Term: 111,
		RSMCmd: &rsm.RSMCmd{
			CMDType: rsm.Add,
			Xid: rsm.Xid{
				Client: "1",
				Index:  0,
			},
		},
	}

	for i := 0; i < t.N; i++ {
		log.Append([]*persistence.LogEntry{e}, uint64(i+1))
	}
}

func BenchmarkAppendTrimP(b *testing.B) {
	log := persistence.NewFileLog("./test")
	defer log.Destroy()
	e := &persistence.LogEntry{
		Term: 111,
		RSMCmd: &rsm.RSMCmd{
			CMDType: rsm.Add,
			Xid: rsm.Xid{
				Client: "1",
				Index:  0,
			},
		},
	}

	for i := 0; i < b.N; i++ {
		log.Append([]*persistence.LogEntry{e}, uint64(i+1))
		log.TrimP(uint64(b.N + 1))
	}
}

func BenchmarkAppendTrimS(b *testing.B) {
	log := persistence.NewFileLog("./test")
	defer log.Destroy()
	e := &persistence.LogEntry{
		Term: 111,
		RSMCmd: &rsm.RSMCmd{
			CMDType: rsm.Add,
			Xid: rsm.Xid{
				Client: "1",
				Index:  0,
			},
		},
	}

	for i := 0; i < b.N; i++ {
		log.Append([]*persistence.LogEntry{e}, uint64(i+1))
		log.TrimS(uint64(1))
	}
}

func BenchmarkAppendAccess(b *testing.B) {
	log := persistence.NewFileLog("./test")
	defer log.Destroy()
	e := &persistence.LogEntry{
		Term: 111,
		RSMCmd: &rsm.RSMCmd{
			CMDType: rsm.Add,
			Xid: rsm.Xid{
				Client: "1",
				Index:  0,
			},
		},
	}

	for i := 0; i < b.N; i++ {
		log.Append([]*persistence.LogEntry{e}, uint64(i+1))
	}
	for i := 0; i < b.N; i++ {
		log.At(uint64(i + 1))
	}
}
