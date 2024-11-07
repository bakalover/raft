# Raft

🥱 Toy RAFT consensus protocol implemetation, _Just for fun_ ♪

Note: refactor wip

---

### Features
- No dependencies, just std lib
- Frontend
  - Cluster -> RPC
  - Balancer -> HTTP
- Balancing Modes
  - Round-Robin
  - Random Choice
  - Leader redirection
- Reconfiguration?
- WIP...
---

### Testing & Metrics
WIP...
---

### How to run

1. Step into balancer cmd directory
2. `go run raft.go <node count>` 
   - [Why an odd number of cluster members?](https://etcd.io/docs/v3.3/faq/)
3. Send request!
  - WIP...

### Reference

- [Raft](https://raft.github.io/raft.pdf)
- [Eli Bendersky - Implementing Raft](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/)
