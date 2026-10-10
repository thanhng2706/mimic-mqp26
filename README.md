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
   client ──► any node (it coordinates the request)
                  │
        ┌─────────┼─────────┐
      node 1    node 2    node 3     (each holds a full copy)
```

**Writing (SET / DEL).** The node you contact first asks the others what they have for the key, then sends the new value to every node. It replies `OK` once W nodes have saved it.

**Reading (GET).** The node asks every node for the key and combines the first R answers. If some nodes had an older copy, it sends them the up-to-date one.

**Detecting concurrent writes.** Each value records two things: its own ID (which node wrote it, and that node's write number, e.g. `1:1`) and the IDs of the writes it had seen. A value is replaced only by a value that had seen it:

| Value | ID | Had seen | Result |
|---|---|---|---|
| `a` | `1:1` | nothing | |
| `b`, written after reading `a` | `1:2` | `1:1` | `b` replaces `a` |
| `c`, written by someone who never saw `b` | `2:1` | `1:1` | `b` and `c` are both kept: `GET` returns `VALUES b c` |

A write made with the context of `VALUES b c` has seen both, so it replaces them.

**Combining copies.** When two copies of a key meet, the result keeps every value that no other value has seen. Order and duplicate messages don't change the result, so all nodes end up identical (this is what makes it a CRDT).

**Deletes.** DEL saves a "deleted" marker instead of removing the key, so a node with an old copy can't bring the value back.

**Catching up.** Every 2 seconds, each node sends all its data to the others, so a node that was down gets every key back.

| Folder | What's in it |
|---|---|
| `store/` | The data, write IDs and contexts, and the rule for combining copies |
| `clock/` | Counts this node's own writes |
| `cmd/server/` | The server: networking, reads, writes, catching up |
| `cmd/client/` | Command-line client |
