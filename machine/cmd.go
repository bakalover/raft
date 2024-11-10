package machine

// Allowed oprations over RSM
const (
	CAS CMDtype = iota
	Add
	Mul
	Get
)

type (
	CMDtype     = int
	MachineType = int

	// Exactly once semantic support
	Xid struct {
		Client string `json:"client"`
		Index  uint64 `json:"index"`
	}

	RSMcmd struct {
		CMD CMDtype     `json:"cmd"`
		Xid Xid         `json:"xid"`
		Arg MachineType `json:"arg"`
	}
)
