package machine

type (
	StateMachine interface {
		Apply(RSMcmd) MachineType
	}

	machineImpl struct {
		state MachineType
	}
)

func NewStateMachine() StateMachine {
	return &machineImpl{}
}

func (m *machineImpl) Apply(cmd RSMcmd) MachineType {
	switch cmd.CMD {
	case CAS:
		m.state = cmd.Arg
	case Add:
		m.state += cmd.Arg
	case Mul:
		m.state *= cmd.Arg
	}
	return m.state
}
