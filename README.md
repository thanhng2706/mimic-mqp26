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
