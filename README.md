# kvstore

A leaderless, replicated key-value store in Go, in the style of Amazon Dynamo.

Every node holds a full copy of the data and any node can serve any request. Each key is a state-based CRDT (MV-Register) tracked with vector clocks, so replicas always converge and concurrent writes are kept instead of silently dropped.

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
go run ./cmd/client -addr localhost:9092 get user         # VALUE alice
go run ./cmd/client -addr localhost:9091 del user         # OK
```

Commands: `SET key value`, `GET key`, `DEL key`. Keys and values are single words.

Server flags: `-id` (this node's 1-based position in `-nodes`), `-nodes` (comma-separated addresses of all nodes, default localhost ports 9090–9092), `-w` and `-r` (write/read quorums, default 2).

## How it works

```
   client ──► any node (coordinator)
                  │  PUT / READ
        ┌─────────┼─────────┐
      node 1    node 2    node 3
```

- **Write:** the coordinator reads the key from R nodes, creates a value whose vector clock covers everything it saw, sends it to all nodes, and replies after W acknowledgements.
- **Read:** the coordinator asks all nodes, merges the first R answers, and sends the merged result back to those nodes (read repair).
- **Vector clocks:** if one value's clock is ≥ another's in every slot, it replaces it. Otherwise the writes were concurrent, and both are kept.
- **Merge:** keeps every value that no other value has seen. It's commutative, associative and idempotent, so replicas converge regardless of message order or duplicates.
- **Deletes:** stored as tombstones, so a stale replica can't bring a value back.
- **Anti-entropy:** every node periodically pushes all its entries to the others, so a node that was down catches up on every key.

| Path | Contents |
|---|---|
| `store/` | Vector clocks, MV-Register merge, thread-safe key → values map |
| `clock/` | Counter for this node's own writes |
| `cmd/server/` | TCP server, quorum reads/writes, read repair, anti-entropy |
| `cmd/client/` | Command-line client |
