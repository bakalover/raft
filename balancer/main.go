package main

import (
	"log"
	"net/rpc"

	"github.com/bakalover/raft/machine"
	"github.com/bakalover/raft/raft"
)

func main() {
	client, err := rpc.DialHTTP("tcp", ":8081")
	if err != nil {
		panic(err.Error())
	}
	args := machine.RSMcmd{
		CMD: machine.Get,
		Xid: machine.Xid{
			Client: "hiiim",
			Index:  1,
		},
		// Arg: 2,
	}
	reply := new(raft.RaftReply)
	err = client.Call("Raft.Apply", args, reply)
	log.Printf("%+v", reply)
	if err != nil {
		panic(err.Error())
	}
}
