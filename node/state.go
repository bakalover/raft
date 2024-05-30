package node

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
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

func (l *Log) NthEntry(index uint64) (uint64, string, error) {
	scanner := bufio.NewScanner(l.persistentLog)
	currIndex := uint64(0)

	for scanner.Scan() {
		currIndex++
		if currIndex == index {
			entry := strings.Split(scanner.Text(), " ")
			term, _ := strconv.ParseUint(entry[0], 10, 64)
			command := entry[1]
			return term, command, nil
		}
	}
	return 0, "", fmt.Errorf("no log entry found at index: %v", index)
}

type TermState struct {
	persistentTermState *os.File
}

func (t *TermState) CurrentTerm() uint64 {
	scanner := bufio.NewScanner(t.persistentTermState)
	// Skip the first line
	if scanner.Scan() {
		// Read the second line
		if scanner.Scan() {
			currentTerm, err := strconv.ParseUint(scanner.Text(), 10, 64)
			if err != nil {
				log.Fatalf("Error converting line to integer: %v", err)
			}
			return currentTerm
		} else {
			panic("Inconsistent term file!")
		}
	}
	panic("Inconsistent term file!")
}

func NewTermState(name string) *TermState {
	_, err := os.Stat(name)

	var file *os.File

	if err != nil {
		file, err = os.Create(name)
		if err != nil {
			log.Fatalf("Cannot create term_state file: %s", name)
		}
		file.WriteString("NULL")
		file.WriteString(strconv.FormatUint(0, 10))
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
