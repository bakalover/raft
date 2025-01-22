package main

import "github.com/bakalover/raft/balancer/srv"

func main() {
	srv.NewServer().Start(":11111")
}
