# Raft
🥱 Just for fun
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
    - Id stands for current node id, so it should be unique. Ids stand for node number in whole cluster, this number should be equal in all node's setups.
    - Ids should be odd. Why? Consider case when we have just 2 nodes.
    Each voted for another and both think it is a Leader (Pure brain split,
    I've observed 2 nodes sending heartbeats and accepting it from each other).
    We cannot make Quorum = 2 (because candidate is not set votedFor for itself).
    Maybe force turning to Follower inside HeartBeat RPC? It might work, but unstable and I am not sure, because it can lead to extra election iterations.
    This problem is the same for each even n.
### Reference
- [Raft](https://raft.github.io/raft.pdf)