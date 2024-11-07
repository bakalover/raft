package main

import (
	"context"
	"log"
	"os"

	"github.com/bakalover/raft/raft"
)

func main() {
	log.Println(os.Args)
	config := raft.Config{
		Ctx:        context.Background(),
		LogKey:     os.Args[1],
		Me:         os.Args[2],
		Neighbours: os.Args[3:],
	}
	raft.NewRaft(&config).Run(context.Background())
}