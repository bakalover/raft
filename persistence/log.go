package persistence

import "github.com/bakalover/raft/machine"

const (
	LastEntry = uint64(0) // First entry has index == 1
)

type (
	LogEntry struct {
		Term   uint64         `json:"term"`
		Index  uint64         `json:"index"` // Index is virtual (because of possible compaction)
		RSMCmd machine.RSMcmd `json:"rsm"`
	}

	LogEntryPack = []LogEntry

	// Append-only log with compaction abillity
	// There is no errors in interface, caller should provide correct call semantic
	Log interface {
		Append(es LogEntryPack, offset uint64)

		Size() uint64

		// Requested entry At index position
		// nil == absent
		At(index uint64) *LogEntry

		// Blocks until all entries under border are deleted
		TrimP(border uint64)

		// Blocks until all entries above border are deleted
		TrimS(border uint64)

		// Provide term of command at specified index
		Term(index uint64) uint64

		// Leader election
		LastEntry() *LogEntry

		Destroy()
	}
)

func EmptyPack() LogEntryPack {
	return LogEntryPack{}
}
