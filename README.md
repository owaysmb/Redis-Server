# Redis-Inspired In-Memory Data Store

A Redis-inspired server implemented in **C++23**. The project uses TCP sockets and the RESP (Redis Serialization Protocol) to communicate with clients, and explores in-memory data structures, concurrency, transactions, streams, and basic replication.

**Repository:** https://github.com/owaysmb/Redis-Server

## Features

### Networking and protocol
- TCP server built with POSIX sockets.
- RESP command parsing and response encoding.
- Concurrent client handling with a thread-per-client model.
- Per-client state for queued commands and watched keys.

### Data types and commands

The implementation supports a subset of Redis-style commands, including:

| Area | Commands / behavior |
| --- | --- |
| Connection and strings | `PING`, `ECHO`, `SET`, `GET`, `INCR`, `TYPE` |
| Key expiration | `SET` with `EX` / `PX` expiration options |
| Lists | `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP` |
| Streams | `XADD`, `XRANGE`, `XREAD`, blocking stream reads |
| Transactions | `MULTI`, `EXEC`, `DISCARD` |
| Optimistic locking | `WATCH`, `UNWATCH` |
| Replication | `REPLCONF`, `PSYNC`, `WAIT`, replication information |
| Configuration / keys | `CONFIG GET`, `KEYS` |

This is a partial implementation for learning purposes; it does not aim to provide full Redis compatibility.

### Concurrency
- A dedicated thread handles each connected client.
- Mutexes protect shared data structures and client/replica state.
- Condition variables support blocking list and stream operations.

### Transactions and replication
- Commands can be queued between `MULTI` and `EXEC`.
- `WATCH` / `UNWATCH` provide optimistic key-change tracking.
- A replica can connect to a master over TCP and perform a basic synchronization handshake.
- The master propagates supported write commands to connected replicas.

### RDB loading
- Reads an RDB file from a configured directory and filename during startup.
- Handles string keys and values, including expiration metadata, in the supported RDB loading path.
- This project currently documents **RDB loading**; it should not be treated as a complete Redis persistence implementation.

## Tech stack

- C++23
- POSIX sockets / TCP
- Multithreading, mutexes, and condition variables
- CMake
- vcpkg (ASIO and pthreads dependencies)

## Build requirements

- A C++ compiler with C++23 support
- CMake
- [vcpkg](https://github.com/microsoft/vcpkg)
- The dependencies listed in `vcpkg.json`

## Build and run

The repository's local runner uses vcpkg through the `VCPKG_ROOT` environment variable.

```bash
# Set this to your local vcpkg installation
export VCPKG_ROOT=/path/to/vcpkg

# Build and start the server (default port: 6379)
./your_program.sh
```

To choose another port:

```bash
./your_program.sh --port 6380
```

The script configures the CMake build, compiles the executable, and launches the server.

## Try it with redis-cli

If you have `redis-cli` installed, connect to the server:

```bash
redis-cli -p 6379
```

Then try a few supported commands:

```text
PING
SET language C++
GET language
RPUSH tasks build test
LRANGE tasks 0 -1
MULTI
SET mode development
INCR visits
EXEC
```

The exact behavior is limited to the commands and argument forms implemented in this project.

## Running a replica

Start a master on one port:

```bash
./your_program.sh --port 6379
```

Start another instance configured as a replica of that master:

```bash
./your_program.sh --port 6380 --replicaof "127.0.0.1 6379"
```

This demonstrates the project's basic master-replica connection and command-propagation path. It is not a drop-in replacement for Redis replication.

## Loading an RDB file

The server accepts a data directory and RDB filename:

```bash
./your_program.sh --dir ./data --dbfilename dump.rdb
```

At startup, the server attempts to load the specified file if it exists.

## Project layout

```text
Redis-Server/
├── src/
│   ├── main.cpp       # TCP server, client threads, replica connection
│   ├── Storage.cpp    # Command handling, data structures, replication, RDB loading
│   ├── Storage.h      # Storage and client-state declarations
│   └── RespParser.h   # RESP parser declarations
├── CMakeLists.txt
├── vcpkg.json
└── your_program.sh
```

## Scope

The goal of this project is to understand how a Redis-like server works internally: client/server communication, protocol parsing, concurrent access to shared state, data structures, blocking operations, transactions, replication, and reading persistence files.

It is an educational implementation and should not be used as a production Redis replacement.
