package rsm

// Allowed oprations over RSM
const (
	CAS CMDType = iota
	Add
	Sub
	Mul
)

type (
	// Exactly one semantic support
	Xid struct {
		Client string `json:"client"`
		Index  uint64 `json:"index"`
	}
	CMDType = int
	RSMCmd  struct {
		CMDType CMDType `json:"cmd"`
		Xid     Xid     `json:"xid"`
		Arg     int     `json:"arg"`
	}
)
