package persistence

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sync"
)

var (
	logFileSuffix         = "_log"
	logFileTransferSuffix = "_log.swap"
)

// Simple implementation of crash-tolerant append-only log using just one file
type fileLog struct {
	mu  sync.Mutex // Protection from concurrent compaction
	db  *os.File
	key string
}

func NewFileLog(key string) Log {
	return &fileLog{db: openFile(key + logFileSuffix), key: key}
}

func (f *fileLog) Destroy() {
	f.mu.Lock()
	defer f.mu.Unlock()
	check(f.db.Close())
	check(os.Remove(f.key + logFileSuffix))
}

func openFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	check(err)
	return file
}

func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func (f *fileLog) persist() {
	check(f.db.Sync())
}

func (f *fileLog) calculateOffest(line uint64) uint64 {
	br := bufio.NewScanner(f.db)
	var offset uint64
	for i := uint64(0); i < line; i++ {
		br.Scan()
		offset += uint64(len(br.Bytes()) + 1) // +1  - new line char
	}
	return offset
}

func (f *fileLog) gotoLine(line uint64) {
	f.gotoPos(f.calculateOffest(line))
}

func (f *fileLog) gotoPos(offset uint64) {
	_, err := f.db.Seek(int64(offset), io.SeekStart)
	check(err)
}

func (f *fileLog) gotoStart() {
	f.gotoPos(0)
}

func (f *fileLog) Append(es LogEntryPack, offset uint64) {
	f.mu.Lock()
	defer func() {
		f.gotoStart()
		f.persist()
		f.mu.Unlock()
	}()
	f.gotoLine(offset - 1)
	bw := bufio.NewWriter(f.db)
	for _, e := range es {
		serialized, err := json.Marshal(e)
		check(err)
		_, err = bw.WriteString(string(serialized) + "\n")
		check(err)
	}
	check(bw.Flush())
}

func (f *fileLog) At(index uint64) *LogEntry {
	f.mu.Lock()
	defer func() {
		f.gotoStart()
		f.mu.Unlock()
	}()
	br := bufio.NewScanner(f.db)
	var l LogEntry
	kLine := uint64(1)
	var bytes []byte
	for br.Scan() {
		bytes = br.Bytes()
		if kLine == index {
			break // Never yield if index == 0 aka Last Entry
		}
		kLine++
	}
	check(br.Err())
	check(json.Unmarshal(bytes, &l))
	return &l
}

func (f *fileLog) Term(index uint64) uint64 {
	return f.At(index).Term
}

func (f *fileLog) LastTerm() uint64 {
	// Non-atomic, but ok
	if size := f.Size(); size == 0 {
		return 0
	}
	return f.Term(LastEntry)
}

func (f *fileLog) Size() uint64 {
	f.mu.Lock()
	defer func() {
		f.gotoStart()
		f.mu.Unlock()
	}()
	br := bufio.NewScanner(f.db)
	size := uint64(0)
	for br.Scan() {
		size += 1
	}
	check(br.Err())
	return size
}

// Most dangerous operation on file system
// There is no way to truncate file from begin
// So we will going to perform swap
// First we will create new file, then fill it with values, and at the end - atomically swap log files
// So this operation works like CAS on your file system
func (f *fileLog) TrimP(border uint64) {
	f.mu.Lock()
	defer func() {
		f.gotoStart()
		f.persist()
		f.mu.Unlock()
	}()
	br := bufio.NewScanner(f.db)
	f.gotoLine(border)
	// On crash during transfer Open() will retry with truncation
	newLog := openFile(f.key + logFileTransferSuffix)
	bw := bufio.NewWriter(newLog)
	for br.Scan() {
		bw.WriteString(br.Text() + "\n")
	}
	check(bw.Flush())
	check(br.Err())
	check(f.db.Close()) // Close before CAS, if crash, it will reopen in NewFileLog()

	// Atomic swap ONLY on UNIX/LINUX !!!
	// See: man rename
	os.Rename(f.key+logFileTransferSuffix, f.key+logFileSuffix)

	f.db = openFile(f.key + logFileSuffix)
}

func (f *fileLog) TrimS(border uint64) {
	f.mu.Lock()
	defer func() {
		f.gotoStart()
		f.mu.Unlock()
	}()
	// Atomic
	// See: man ftruncate
	check(os.Truncate(f.key+logFileSuffix, int64(f.calculateOffest(border-1))))
}
