# kvstore

A leaderless, replicated key-value store in Go, in the style of Amazon Dynamo.

Every node holds a full copy of the data and any node can serve any request. Each key is a state-based CRDT (MV-Register) tracked with dotted version vectors, so replicas always converge and concurrent writes are kept instead of silently dropped.

> **Status:** experimental, built to explore replication and CRDTs. Not for production use.

## Features

- **No leader:** any node coordinates any request.
- **Tunable quorums:** with `W + R > N`, reads see the latest acknowledged write; with `W=1, R=1`, the store stays available during failures.
- **No lost writes:** concurrent writes are returned together (`VALUES a b`), and the next write replaces them.
- **Self-healing:** read repair, plus background anti-entropy every 2 seconds.

## Quick start

Requires Go 1.27+. Start three nodes, one per terminal:

```sh
go run ./cmd/server -id 1
go run ./cmd/server -id 2
go run ./cmd/server -id 3
```

Then talk to any node:

```sh
go run ./cmd/client -addr localhost:9090 set user alice   # OK
go run ./cmd/client -addr localhost:9092 get user         # VALUE alice CONTEXT 1:1
go run ./cmd/client -addr localhost:9091 del user         # OK
```

Commands: `SET key value [context]`, `GET key`, `DEL key [context]`. Keys and values are single words.

`GET` also returns a **context**: what that read saw. Pass it to your next `SET`/`DEL` so it replaces only the values you actually read. Without it, a write replaces whatever the server currently has.

Server flags: `-id` (this node's 1-based position in `-nodes`), `-nodes` (comma-separated addresses of all nodes, default localhost ports 9090–9092), `-w` and `-r` (write/read quorums, default 2).

## Walkthrough

A session on a fresh 3-node cluster (`N=3, W=2, R=2`); `client` is short for `go run ./cmd/client`. Context slot order may vary between runs.

**Concurrent writes become siblings.** Two clients read the same version, then write through different coordinators. Neither context covers the other's dot, so both values are kept:

```sh
client -addr localhost:9090 set city paris        # OK
client -addr localhost:9092 get city              # VALUE paris CONTEXT 1:1
client -addr localhost:9090 set city rome 1:1     # OK
client -addr localhost:9091 set city oslo 1:1     # OK
client -addr localhost:9092 get city              # VALUES oslo rome CONTEXT 1:2,2:1
```

**A write with the merged context resolves them:**

```sh
client -addr localhost:9091 set city lima 1:2,2:1 # OK
client -addr localhost:9090 get city              # VALUE lima CONTEXT 1:2,2:2
```

**Deletes are tombstones.** `DEL` is a write with its own dot, so it supersedes only what its context covers:

```sh
client -addr localhost:9092 del city 1:2,2:2      # OK
client -addr localhost:9090 get city              # NOT_FOUND
```

A `DEL` (or `SET`) with an outdated context leaves unseen values in place as siblings rather than discarding them.

**Fault tolerance.** Stop node 3: quorums of 2 are still reachable.

```sh
client -addr localhost:9090 set lang go           # OK
client -addr localhost:9091 get lang              # VALUE go CONTEXT 1:3
```

Stop node 2 as well: only one replica is left, so quorum operations are refused.

```sh
client -addr localhost:9090 set lang rust         # ERROR only 1 of 2 required replicas acked
client -addr localhost:9090 get lang              # ERROR only 1 of 2 required replicas answered
```

**Anti-entropy heals restarted nodes.** Restart nodes 2 and 3 (empty, since storage is in-memory). Within one sync interval they receive node 1's state:

```sh
client -addr localhost:9092 get lang              # VALUE rust CONTEXT 1:4
```

Note `rust` appears: a write that fails its quorum is **not rolled back** on the replicas that did apply it. The error means "not durably acknowledged", not "did not happen".

## How it works

```
   client ──► any node (coordinator)
                  │  PUT / READ
        ┌─────────┼─────────┐
      node 1    node 2    node 3     (full replicas)
```

**Write path (SET / DEL).** The coordinator reads the key from R replicas, tags the new value with a dot and causal context, sends it to all replicas, and acknowledges after a write quorum of W.

**Read path (GET).** The coordinator queries all replicas, merges the first R responses, and returns the result with its causal context. Stale replicas are updated through **read repair**.

**Dotted version vectors.** Each value carries a **dot**, a unique write ID (`node:counter`), and a **causal context**, the dots it had seen. A value is superseded only by a value whose context includes its dot; otherwise the two are concurrent and kept as **siblings**:

| Value | Dot | Context | Result |
|---|---|---|---|
| `a` | `1:1` | — | |
| `b` | `1:2` | `1:1` | supersedes `a` |
| `c` | `2:1` | `1:1` | concurrent with `b`: `GET` returns `VALUES b c` |

Clients pass the context from `GET` to the next `SET`/`DEL`, so a write supersedes only the values the client actually read.

**MV-Register merge.** Replicas merge by keeping every value not covered by another value's context. The merge is commutative, associative and idempotent, so this is a **state-based CRDT**: replicas converge regardless of message order or duplication.

**Tombstones.** DEL writes a deletion marker with its own dot, so a stale replica can't resurrect the value.

**Anti-entropy.** Every 2 seconds, each node pushes its full state to every other node, guaranteeing convergence for keys that are never read.

| Path | Contents |
|---|---|
| `store/` | Dots, causal contexts, MV-Register merge, thread-safe storage |
| `clock/` | Per-node write counter used to assign dots |
| `cmd/server/` | TCP server, quorum reads/writes, read repair, anti-entropy |
| `cmd/client/` | Command-line client |

## Wire protocol

Plain text over TCP, one line per request and one per reply. Clients and nodes use the same port.

**Client commands** (the receiving node becomes the coordinator):

| Request | Replies |
|---|---|
| `GET key` | `VALUE v CONTEXT c`, `VALUES v1 v2 … CONTEXT c` (siblings), `NOT_FOUND`, `ERROR …` |
| `SET key value [context]` | `OK`, `ERROR …` |
| `DEL key [context]` | `OK`, `ERROR …` |

**Replica messages** (node to node, local store only):

| Request | Replies | Used by |
|---|---|---|
| `READ key` | `ENTRY e1 e2 …`, `NONE` | read path, write path (fetch current context) |
| `PUT key e1 e2 …` | `ACK` | write replication, read repair, anti-entropy |

A version vector is written `node:counter,…` (e.g. `1:2,2:1`). An entry is `dot|context/deleted/value`, e.g. `1:3|1:1/0/cat`: node 1's 3rd write, whose context had seen node 1's 1st write, not a tombstone, value `cat`. `PUT` merges the entries into the replica's state, so it is idempotent and safe to resend.
