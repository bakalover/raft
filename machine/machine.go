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
		ret := m.state
		m.state = cmd.Arg
		return ret
	case Add:
		m.state += cmd.Arg
	case Mul:
		m.state *= cmd.Arg
	case Get:
		return m.state
	}
	return 0
}
