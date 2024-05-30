package node

import (
	"io"
	"log"
	"os"
)

type Log struct {
	persistentLog *os.File
}

func NewLog(name string) *Log {
	_, err := os.Stat(name)

	var file *os.File

	if err != nil {
		file, err = os.Create(name)
		if err != nil {
			log.Fatalf("Cannot create log file: %s", name)
		}
	} else {
		file, err = os.OpenFile(name, os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Cannot open log file: %s", name)
		}
	}
	file.Seek(0, io.SeekEnd)
	return &Log{persistentLog: file}
}

type TermEntry struct {
	VotedFor    string
	CurrentTerm uint64
}

type TermState struct {
	persistentTermState *os.File
}


func NewTermState(name string) *TermState {
	_, err := os.Stat(name)

	var file *os.File

	if err != nil {
		file, err = os.Create(name)
		if err != nil {
			log.Fatalf("Cannot create term_state file: %s", name)
		}
	} else {
		file, err = os.OpenFile(name, os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Cannot open term_state file: %s", name)
		}
	}
	return &TermState{persistentTermState: file}
}

type State struct {
	termState   TermState
	log         Log
	commitIndex uint64
	lastApplied uint64
	nextIndex   []uint64
	matchIndex  []uint64
}
