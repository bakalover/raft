package node

import (
	"gorm.io/gorm"
)

type LogEntry struct {
	Index   uint64 `gorm:"primaryKey"`
	Term    uint64
	Command string
}

type TermState struct {
	CurrentTerm uint64
	VotedFor    string
}

type PersistentState struct {
	db *gorm.DB
}

func NewPersistentState(db *gorm.DB) *PersistentState {
	state := &PersistentState{db}
	state.Init()
	return state
}

func (p *PersistentState) Init() {
	if !p.db.Migrator().HasTable(&LogEntry{}) {
		p.db.AutoMigrate(&LogEntry{})
	}
	if !p.db.Migrator().HasTable(&TermState{}) {
		p.db.AutoMigrate(&TermState{})
	}
}

func (p *PersistentState) ShutDown() {
	p.db.Migrator().DropTable(&LogEntry{}, &TermState{})
}

func (p *PersistentState) CurrentTerm() uint64 {
	var el TermState
	p.db.Model(&TermState{}).First(&el)
	return el.CurrentTerm
}

func (p *PersistentState) SetTerm(term uint64) {
	var t TermState
	p.db.Model(&TermState{}).First(&t)
	t.CurrentTerm = term
	p.db.Model(&TermState{}).Save(&t)
}

func (p *PersistentState) NthEntry(n uint64) *LogEntry {
	var l LogEntry

	r := p.db.
		Model(&LogEntry{}).
		Where("index = ?", n).
		First(&l)

	if r.Error != nil {
		return nil
	} else {
		return &l
	}
}

func (p *PersistentState) ClearAbove(index uint64) {
	p.db.
		Model(&LogEntry{}).
		Where("index > ?", index).
		Delete(&LogEntry{})
}

func (p *PersistentState) Append(term uint64, lIndex uint64, entries []string) {
	for _, entry := range entries {
		p.db.
			Model(&LogEntry{}).
			Create(&LogEntry{Index: lIndex, Term: term, Command: entry})
		lIndex++
	}
}

type State struct {
	persistentState *PersistentState
	commitIndex     uint64
	lastApplied     uint64
	nextIndex       []uint64
	matchIndex      []uint64
}
