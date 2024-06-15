# Raft
🥱 Yet another boring raft implementation, just for fun.
___
### Features
- Frontend - HTTP
- Backend - intracluster RPC calls via Go's std library
- Fault tolerant load balancing using [HAProxy](https://www.haproxy.org/)
- Reconfiguration (may be implemented in future)
- PostgreSQL as persistent storage (had big troubles with simple text files)

___
### [Jmeter](https://jmeter.apache.org/) Metrics
___
### Reference
- [Raft](https://raft.github.io/raft.pdf)