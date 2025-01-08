package persistence

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

var (
	logFileSuffix         = "_log.txt"
	logFileTransferSuffix = "_log.swap.txt"
)

// Simple implementation of crash-tolerant append-only log using just one file
type fileLog struct {
	db  *os.File
	key string
}

func NewFileLog(key string) Log {
	return &fileLog{db: openFile(key + logFileSuffix), key: key}
}

func (f *fileLog) Destroy() {
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

func (f *fileLog) gotoEnd() {
	_, err := f.db.Seek(0, io.SeekEnd)
	check(err)
}

func (f *fileLog) Append(es LogEntryPack, offset uint64) {

	defer func() {
		f.gotoStart()
		f.persist()
	}()
	f.gotoEnd()
	bw := bufio.NewWriter(f.db)
	for i, e := range es {
		e.Index = offset + uint64(i)
		serialized, err := json.Marshal(e)
		check(err)
		_, err = bw.WriteString(string(serialized) + "\n")
		check(err)
	}
	check(bw.Flush())
}

func (f *fileLog) At(index uint64) *LogEntry {

	defer func() {
		f.gotoStart()
	}()
	br := bufio.NewScanner(f.db)
	l := &LogEntry{}
	var bytes []byte
	for br.Scan() {
		bytes = br.Bytes()
		check(json.Unmarshal(bytes, l)) // Optimize via file seek
		if l.Index == index {
			return l
		}
	}
	check(br.Err())
	if index == LastEntry {
		return l
	}
	return &LogEntry{}
}

func (f *fileLog) Term(index uint64) uint64 {

	return f.At(index).Term
}

func (f *fileLog) LastEntry() *LogEntry {
	if size := f.Size(); size == 0 {
		return &LogEntry{} // Zero index and term
	}
	return f.At(LastEntry)
}

func (f *fileLog) Size() uint64 {
	defer func() {
		f.gotoStart()
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

	defer func() {
		f.gotoStart()
		f.persist()
	}()
	br := bufio.NewScanner(f.db)
	var l LogEntry
	var bytes []byte
	for br.Scan() {
		bytes = br.Bytes()
		check(json.Unmarshal(bytes, &l))
		if l.Index == border {
			break
		}
	}
	check(br.Err())
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

	defer func() {
		f.gotoStart()
	}()
	// Atomic
	// See: man ftruncate
	check(os.Truncate(f.key+logFileSuffix, int64(f.calculateOffest(border-1))))
}
