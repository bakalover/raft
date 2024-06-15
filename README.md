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
2) `go run main.go <id> <ids>` inside node/cmd directory. Id stands for current node id, so it should be unique. Ids stand for node number in whole cluster, this number should be equal in all node's setups.
### Reference
- [Raft](https://raft.github.io/raft.pdf)