package machine_test

import (
	"testing"

	"github.com/bakalover/raft/machine"
	"github.com/stretchr/testify/assert"
)

func TestMachine(t *testing.T) {
	t.Run("Just Work", func(t *testing.T) {
		m := machine.NewStateMachine()
		assert.Zero(t, m.Apply(machine.RSMcmd{
			CMD: machine.Get,
		}))
		m.Apply(machine.RSMcmd{
			CMD: machine.CAS,
			Arg: 69,
		})
		assert.Equal(t, 69, m.Apply(machine.RSMcmd{
			CMD: machine.Get,
		}))
		m.Apply(machine.RSMcmd{
			CMD: machine.CAS,
			Arg: 1,
		})
		m.Apply(machine.RSMcmd{
			CMD: machine.Add,
			Arg: 1,
		})
		m.Apply(machine.RSMcmd{
			CMD: machine.Mul,
			Arg: 3,
		})
		assert.Equal(t, 6, m.Apply(machine.RSMcmd{
			CMD: machine.Get,
		}))
	})
}
