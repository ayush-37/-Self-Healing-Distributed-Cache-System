# Distributed Cache

A small Node.js learning project that explores a UDP based cache cluster. It combines a consistent hash ring, replicated key/value operations, gossip membership, basic failure detection, TTL expiry, and a Docker Compose development cluster.

## Requirements

- Node.js 20 or newer
- npm
- Docker Desktop with Docker Compose (optional, for the containerized cluster)

## Setup

```sh
npm install
```

## Run locally

Start each node in a separate terminal from the repository root. The peer list is JSON and includes the other nodes' IDs and ports.

```sh
node node/gossipNode.js A 6001 '[{"id":"B","port":6002},{"id":"C","port":6003},{"id":"D","port":6004},{"id":"E","port":6005}]'
node node/gossipNode.js B 6002 '[{"id":"A","port":6001},{"id":"C","port":6003},{"id":"D","port":6004},{"id":"E","port":6005}]'
node node/gossipNode.js C 6003 '[{"id":"A","port":6001},{"id":"B","port":6002},{"id":"D","port":6004},{"id":"E","port":6005}]'
node node/gossipNode.js D 6004 '[{"id":"A","port":6001},{"id":"B","port":6002},{"id":"C","port":6003},{"id":"E","port":6005}]'
node node/gossipNode.js E 6005 '[{"id":"A","port":6001},{"id":"B","port":6002},{"id":"C","port":6003},{"id":"D","port":6004}]'
```

In another terminal, start the HTTP API:

```sh
node api.js
```

The API listens on `http://localhost:3000`. Example requests:

```sh
curl -X POST http://localhost:3000/set -H "Content-Type: application/json" -d '{"key":"foo","value":"bar"}'
curl http://localhost:3000/get/foo
curl -X POST http://localhost:3000/set -H "Content-Type: application/json" -d '{"key":"session","value":"abc","ttl":5}'
curl -X DELETE http://localhost:3000/del/foo
```

For a one node dynamic join experiment after starting the cluster:

```sh
node node/gossipNode.js F 6006 '[{"id":"A","port":6001}]'
```

## Run with Docker Compose

```sh
docker compose up --build
```

The Compose file starts five gossip nodes and the API. Stop the cluster with `Ctrl+C` or `docker compose down`.

## Architecture

```text
HTTP client
    |
    v
api.js  -- UDP requests -->  gossip nodes (one process per node)
                                  |-- membershipHandlers.js
                                  |-- failureDetection.js
                                  |-- storeHandlers.js
                                  |-- hashRing.js --> hash.js
```

- `api.js` exposes set, get, and delete endpoints and routes requests to replica nodes.
- `node/gossipNode.js` is the node process entry point. It reads CLI configuration, constructs shared state, wires UDP message handlers, and starts periodic jobs.
- `node/membershipHandlers.js` handles joining, membership exchange, gossip merge, and data resynchronization.
- `node/failureDetection.js` handles direct and indirect probes and periodic gossip/probe cycles.
- `node/storeHandlers.js` handles local cache reads, writes, deletes, TTL cleanup, and capacity eviction.
- `node/hashRing.js` and `node/hash.js` implement consistent hashing and the hash function.
- `node/tcp*.js` and `node/udp*.js` are standalone networking examples.
- `chaos/chaosTest.js` generates load and simulates node failure.
- `test/hashRing.test.js` contains hash ring experiments and benchmarks. These are currently runnable scripts rather than a configured automated test suite.

## Node configuration

```text
node node/gossipNode.js <id> <port> '<peers-json>' [host] [max-keys] [lru|lfu]
```

Each peer object has an `id`, `port`, and optional `host`. Defaults are `localhost`, 50 keys, and the `lru` policy. The API uses `HOST_A` through `HOST_E` and `API_HOST` environment variables when running across Docker hosts.

## Known limitations

- This is an educational prototype, not a production cache. It has no authentication, encryption, persistence, or formal consistency guarantees.
- UDP messages are not validated or authenticated; malformed input can terminate a handler, and packet loss or reordering is not comprehensively handled.
- Failure detection is timing based and can mistake a slow or partitioned node for a failed node.
- Resynchronization sends a full local-store snapshot, which will become expensive as stores grow.
- The configured eviction policy implementation needs review: the `lru` branch currently selects by frequency, while the other branch selects the oldest map entry. This is retained from the original code during the module split.
- `npm test` is currently a placeholder. The scripts in `test/` are exploratory output/benchmark scripts, not assertions run by a test framework.
- The current API initializes a fixed set of node IDs and ports; dynamic membership is only partially reflected in that setup.
