package node

import (
	"fmt"
	"log"
	"os"

	"gorm.io/driver/postgres"
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

func NewPersistentState() *PersistentState {
	dsn := fmt.Sprintf(
		"host=%v user=%v password=%v dbname=%v port=%v sslmode=disable",
		os.Getenv("PG_HOST"),
		os.Getenv("PG_USER"),
		os.Getenv("PG_PASS"),
		os.Getenv("PG_DB"),
		os.Getenv("PG_PORT"),
	)
	conn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("cannot establish connection to database: %v", err)
	}
	state := &PersistentState{conn}
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
	p.Set(0, NULL_CANDIDATE_ID)
}

func (p *PersistentState) ShutDown() {
	p.db.Migrator().DropTable(&LogEntry{}, &TermState{})
}

func (p *PersistentState) CurrentTerm() uint64 {
	var el TermState
	p.db.Model(&TermState{}).First(&el)
	return el.CurrentTerm
}

func (p *PersistentState) IncrementAndFetchTerm() uint64 {
	var el TermState
	txn := p.db.Begin()
	txn.First(&el)
	el.CurrentTerm++
	txn.Save(&el)
	txn.Commit()
	return el.CurrentTerm
}

func (p *PersistentState) VotedFor() string {
	var el TermState
	p.db.Model(&TermState{}).First(&el)
	return el.VotedFor
}

func (p *PersistentState) SetTerm(term uint64) {
	var t TermState
	txn := p.db.Begin()
	txn.Model(&TermState{}).First(&t)
	t.CurrentTerm = term
	txn.Model(&TermState{}).Save(&t)
	txn.Commit()
}

func (p *PersistentState) SetVotedFor(votedFor string) {
	var t TermState
	txn := p.db.Begin()
	txn.Model(&TermState{}).First(&t)
	t.VotedFor = votedFor
	txn.Model(&TermState{}).Save(&t)
	txn.Commit()
}

func (p *PersistentState) Set(term uint64, votedFor string) {
	var t TermState
	txn := p.db.Begin()
	txn.Model(&TermState{}).First(&t)
	t.VotedFor = votedFor
	t.CurrentTerm = term
	txn.Model(&TermState{}).Save(&t)
	txn.Commit()
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

func (p *PersistentState) LastEntry() *LogEntry {
	le := new(LogEntry)

	// If there is no log entry -> "zero value" is already here
	p.db.
		Model(&LogEntry{}).
		Last(&le)

	return le
}

func (p *PersistentState) ClearAbove(index uint64) {
	p.db.
		Model(&LogEntry{}).
		Where("index > ?", index).
		Delete(&LogEntry{})
}

func (p *PersistentState) AppendToLog(term uint64, lIndex uint64, entries []string) {
	// Raft insures safety even if node dies while appending
	for _, entry := range entries {
		p.db.
			Model(&LogEntry{}).
			Create(&LogEntry{Index: lIndex, Term: term, Command: entry})
		lIndex++
	}
}
