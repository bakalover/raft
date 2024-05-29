# Raft
🥱 Semi-toy implenetation of RAFT consensus protocol.

*Just for fun*

Note: refactor wip

___
### Features
Note: Depreprecated
- Frontend - HTTP
- Backend - intracluster RPC calls via gRPC
- Fault tolerant load balancing using HAProxy
- PostgreSQL as persistent storage (had big troubles with simple text files)

___
### Testing & Metrics
---
### How to run
Note: Depreprecated
1) Set environmetal variables:
- `PG_HOST`
- `PG_USER`
- `PG_PASS`
- `PG_DB`
- `PG_PORT`
2) `go run main.go <id> <ids>` inside node/cmd directory. 
    - `<id>` stands for current node id, so it should be unique. Ids stand for node number in whole cluster, this number should be equal in all node's setups.
    - `<ids>` should be odd. Why? Consider case when we have just 2 nodes.
    Each voted for another and both think it is a Leader (Pure brain split,
    I've observed 2 nodes sending heartbeats and accepting it from each other).
    We cannot make Quorum = 2 (because candidate is not set votedFor for itself).
    Maybe force turning to Follower inside HeartBeat RPC? It might work, but unstable and I am not sure, because it can lead to extra election iterations.
    This problem is the same for each even n.
3) `curl  -L --post301 -i -d 'command=woopwoop' http://localhost:6070/replicate`
### Reference
- [Raft](https://raft.github.io/raft.pdf)
