package main

import (
	"os"
	"strconv"

	"github.com/bakalover/raft/node"
)

func main() {
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic("id argument must be an integer!")
	}
	ids, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic("ids argument must be an integer!")
	}
	node.NewNode(id, ids).BootRun()
}
