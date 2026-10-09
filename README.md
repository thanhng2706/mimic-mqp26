# mimic-mqp26

Weekly Go exercises for the Mimic MQP-26 cohort.

- **Week 2:** in-memory key-value store with a TCP server and client.
- **Week 3:** made it a distributed, leaderless store with tunable quorums (Dynamo-style).
- **Week 4:** CRDTs. Each key is now a state-based **MV-Register** (vector clocks, concurrent writes are kept, not dropped), plus background anti-entropy.

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
  - **GET:** asks all N nodes, waits for **R** answers, merges them, and returns the value (or `VALUES a b` if there are concurrent writes).
- Nodes talk to each other over the same text protocol as clients, with two internal commands:
  - `PUT key clock/deleted/value ...` merges values into the node's copy, e.g. `PUT x 1:2,3:1/0/cat`.
  - `READ key` returns the node's local values.

| Package | What it does |
|---|---|
| `store/` | Map of key → list of values (value + vector clock + deleted flag), guarded by an `RWMutex`. `Merge` / `Apply` drop values another value has seen. |
| `clock/` | Counts this node's own writes: its slot in every vector clock. |
| `cmd/server/` | `main.go`: flags, TCP server, command dispatch, anti-entropy timer. `node.go`: quorum reads/writes, read repair, anti-entropy. |
| `cmd/client/` | Sends one command, as typed, to the node given by `-addr`. |

## Key design decisions and why

1. **Quorums, W + R > N.** Any W nodes that acked a write and any R nodes that answered a read share at least one node, so a read sees the latest acked write. W=1, R=1 breaks the overlap, and stale reads appear.
2. **Vector clocks as versions (Week 4, replaced Lamport timestamps).** Each value carries one counter per node: which writes it has seen. If one clock is ≥ the other in every slot, that value is newer and replaces the other. If each is bigger somewhere, the writes were **concurrent** and both are kept. A single Lamport number can't tell those two cases apart, so Week 3 silently dropped one of two concurrent writes.
3. **Each key is an MV-Register CRDT.** Merge = keep every value no other value has seen. It ignores order, grouping and duplicates, so all replicas converge. A GET that sees concurrent values returns `VALUES a b`; the next SET has seen both and replaces them.
4. **Read before writing.** Before a write, the coordinator reads the key from R nodes and makes the new clock cover everything it saw, so it replaces those values. (`TestWriteAfterAckedWriteWins` fails without this.)
5. **Tombstones for delete.** DEL stores a "deleted" value with its own clock instead of removing the key. A concurrent SET and DEL keep both, and GET shows the SET. Otherwise a node that missed the delete could hand the old value back.
6. **Read repair.** A GET sends the merged values back to the nodes that answered; `Apply` ignores what they already have.
7. **Mutex in store and clock.** Many connections are handled concurrently. `Apply` does read-compare-write, and the lock makes that one atomic step.
8. **Anti-entropy (Week 4).** Every 2 seconds each node sends all its entries to every other node as normal `PUT`s, and `Apply` merges them in. Read repair only fixes keys someone reads; this makes sure a node that was down catches up on *every* key, which a CRDT needs to guarantee convergence.

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
4. **Restart all with W=1, R=1.** Kill node 3, set x on node 1, restart node 3, and quickly run `get x` on node 3 a few times. The answers flip between `VALUE 5` and `NOT_FOUND`: stale reads, and a monotonic-reads violation.
5. **Anti-entropy:** wait 2 seconds and `get x` on node 3 again. It now always says `VALUE 5`, because node 3 caught up in the background without anyone reading through it.
6. **Concurrent writes are kept:** `go test ./cmd/server -run TestConcurrentWritesAreBothKept -v`. It splits the network, writes `a` on one side and `b` on the other, heals, and `GET` returns `VALUES a b`; a following `SET x c` returns `VALUE c`. (This runs as a test because killing a real server erases its memory, so a live split can't be shown that way.)

Tests: `go test ./...` (merge rules, plus a real 3-node cluster in `cmd/server/cluster_test.go`).

## Known limits (on purpose, for discussion)

- **The client must resolve concurrent values.** No write is lost, but `VALUES a b` is left for the client to pick from.
- **Not linearizable, even with W+R>N.** For example, two reads during an in-flight write can return new then old.
- **A failed write may still be partly applied.** `ERROR` means "fewer than W acked", not "nothing was written".
- **Anti-entropy sends everything, every time.** Fine for a demo; real systems (Dynamo, Cassandra) compare hashes (Merkle trees) and send only what differs.
- **Tombstones are never cleaned up.**
- **No persistence.** A restarted node starts empty and catches up through anti-entropy and read repair.
- **Single-word values only.** The text protocol splits on spaces (same as Week 2).
