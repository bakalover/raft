package main

import (
	"log"
	"os"

	"github.com/bakalover/raft/raft"
)

func main() {
	log.Println(os.Args)
	config := raft.Config{
		LogKey:     os.Args[1],
		Me:         os.Args[2],
		Neighbours: os.Args[3:],
	}
	instance := raft.NewRaft(&config)
	instance.Run()
}
