# Raft
🥱 Yet another boring raft implementation, just for fun.
___
### Features
- Frontend - HTTP
- Backend - intracluster RPC calls via Go's std library
- Fault tolerant load balancing using [HAProxy](https://www.haproxy.org/)
- PostgreSQL as persistent storage (had big troubles with simple text files)
- Reconfiguration (may be implemented in future)


___
### [Jmeter](https://jmeter.apache.org/) Metrics
---
### How to run
1) Set environmetal variables:
- `PG_HOST`
- `PG_USER`
- `PG_PASS`
- `PG_DB`
- `PG_PORT`
2) `go run main.go <id> <ids>` inside node/cmd directory.
Make sure that id is unique for each node and ids are equal!
___
### Reference
- [Raft](https://raft.github.io/raft.pdf)