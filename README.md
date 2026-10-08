# mimic-mqp26

Weekly Go exercises for the Mimic MQP-26 cohort.

- **Week 2:** in-memory key-value store with a TCP server and client.
- **Week 3:** made it a distributed, leaderless store with tunable quorums (Dynamo-style).

## Architecture (Week 3)

```
              client (GET / SET / DEL)
                       │  any node
                       ▼
   ┌──────────┐   PUT / READ   ┌──────────┐
   │  node 1  │◄──────────────►│  node 2  │
   │  :9090   │                │  :9091   │
   └────▲─────┘                └─────▲────┘
        │         ┌──────────┐       │
        └────────►│  node 3  │◄──────┘
                  │  :9092   │
                  └──────────┘
```

- Every node runs the same code and keeps a **full copy** of the data. There is no leader.
- The node a client talks to is the **coordinator** for that request.
  - **SET / DEL:** sends the write to all N nodes and replies `OK` once **W** have acked.
  - **GET:** asks all N nodes, waits for **R** answers, returns the newest value.
- Nodes talk to each other over the same text protocol as clients, with two internal commands:
  - `PUT key time node deleted [value]` stores an entry if it is newer.
  - `READ key` returns the node's local entry.

| Package | What it does |
|---|---|
| `store/` | Map of key → entry (value + version + deleted flag), guarded by an `RWMutex`. `Apply` only keeps newer versions. |
| `clock/` | Lamport clock: `Tick` for a new write, `Observe` when seeing another node's time. |
| `cmd/server/` | `main.go`: flags, TCP server, command dispatch. `node.go`: quorum reads/writes, read repair. |
| `cmd/client/` | Sends one command to the node given by `-addr`. |

## Key design decisions and why

1. **Quorums, W + R > N.** Any W nodes that acked a write and any R nodes that answered a read share at least one node, so a read sees the latest acked write. W=1, R=1 breaks the overlap, and stale reads appear.
2. **Lamport timestamps as versions.** With no leader, nodes need to agree which of two values is newer. Machine wall clocks drift, so each value carries a logical counter instead.
3. **Tie-break by node ID.** Two writes can get the same time. Comparing node IDs makes every node pick the same winner, so replicas converge.
4. **Observe before writing.** Before a write, the coordinator reads the key's current version from R nodes and moves its clock past it. This guarantees a write that starts after another write was acked always wins. (`TestWriteAfterAckedWriteWins` fails without this.)
5. **Tombstones for delete.** DEL stores "deleted at time t" instead of removing the key. Otherwise a node that missed the delete could hand the old value back.
6. **Read repair.** When a GET sees a replica with an older value, it sends that replica the newest one. This is how a node that was down catches up.
7. **Mutex in store and clock.** Many connections are handled concurrently. `Apply` does read-compare-write, and the lock makes that one atomic step.

## CAP / PACELC in this system

W and R are the knob:

| Setting | Partition (P): what happens | Else (E): normal case |
|---|---|---|
| W=2, R=2 (W+R>N) | Minority side refuses requests. Chooses **C** | Waits for 2 replicas: higher latency, fresh reads (**PC/EC**-leaning) |
| W=1, R=1 | Every reachable node keeps answering. Chooses **A** | Fastest, may read stale (**PA/EL**) |

## Run it

```sh
go build -o server.exe ./cmd/server
go build -o client.exe ./cmd/client

# three terminals
./server.exe -id 1 -w 2 -r 2
./server.exe -id 2 -w 2 -r 2
./server.exe -id 3 -w 2 -r 2

./client.exe -addr localhost:9090 set x 5
./client.exe -addr localhost:9092 get x      # VALUE 5
```

Default `-nodes` is `localhost:9090,localhost:9091,localhost:9092`, and `-id` picks which one this process is.

Demo script:

1. **W=2, R=2:** set on node 1, get on node 3 → new value.
2. **Kill node 3** (Ctrl+C): writes and reads still work (2 of 3 up).
3. **Kill node 2 too:** `ERROR only 1 of 2 required replicas acked`. The system chose consistency.
4. **Restart all with W=1, R=1.** Kill node 3, set x on node 1, restart node 3, run `get x` on node 3 a few times. The answers flip between `VALUE 5` and `NOT_FOUND`: stale reads, and a monotonic-reads violation.

Tests: `go test ./...` (store and clock rules, plus a real 3-node cluster in `cmd/server/cluster_test.go`).

## Known limits (on purpose, for discussion)

- **Concurrent writes lose data.** Last-write-wins keeps one of two simultaneous writes and drops the other silently. Vector clocks or CRDTs would keep both.
- **Not linearizable, even with W+R>N.** For example, two reads during an in-flight write can return new then old.
- **A failed write may still be partly applied.** `ERROR` means "fewer than W acked", not "nothing was written".
- **Repair happens only on read.** A key that's never read stays stale on a node that missed it. There's no background anti-entropy.
- **Tombstones are never cleaned up.**
- **No persistence.** A restarted node starts empty and catches up through read repair.
- **Single-word values only.** The text protocol splits on spaces (same as Week 2).
