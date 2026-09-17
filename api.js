import express from 'express';
import dgram from 'dgram';
import { HashRing } from './node/hashRing.js';

const app = express();
app.use(express.json());

const ring = new HashRing(150);
['A', 'B', 'C', 'D', 'E'].forEach(n => ring.addNode(n));

const NODE_PORTS = {
    A: 6001, B: 6002, C: 6003, D: 6004, E: 6005,
};

// api.js's own lightweight view of cluster membership, kept fresh by periodic queries
let knownMembership = new Map();

const API_PORT = 7000;
const socket = dgram.createSocket('udp4');
socket.bind(API_PORT);

const pendingRequests = new Map();

socket.on('message', (msg) => {
    const message = JSON.parse(msg.toString());

    if (message.type === 'membership-reply') {
        knownMembership = new Map(Object.entries(message.membership));

        for (const [id, info] of knownMembership) {
            if (!NODE_PORTS[id] && info.port) {
                NODE_PORTS[id] = info.port;
                if (!ring.hasNode(id)) {
                    ring.addNode(id);
                    console.log(`[api] learned of new node ${id} on port ${info.port}, added to ring`);
                }
            }
        }
        return;
    }

    const pending = pendingRequests.get(message.key + ':' + message.type);
    if (pending) {
        clearTimeout(pending.timeoutHandle);
        pendingRequests.delete(message.key + ':' + message.type);
        pending.resolve(message);
    }
});

function sendAndWait(nodeId, payload, timeoutMs = 500) {
    return new Promise((resolve, reject) => {
        const port = NODE_PORTS[nodeId];

        const replyTypeMap = {
            'store-set': 'store-set-ack',
            'store-get': 'store-get-reply',
            'store-delete': 'store-delete-ack',
        };
        const requestKey = payload.key + ':' + replyTypeMap[payload.type];

        const timeoutHandle = setTimeout(() => {
            pendingRequests.delete(requestKey);
            reject(new Error(`Timeout waiting for ${nodeId}`));
        }, timeoutMs);

        pendingRequests.set(requestKey, { resolve, timeoutHandle });

        const message = JSON.stringify({ ...payload, fromPort: API_PORT });
        socket.send(message, port, 'localhost');
    });
}


// exist on HashRing, only getNodeForKey (singular). Fixed below.
function getReadyOwners(key, count) {
    const candidates = ring.getNodeForKey(key, Object.keys(NODE_PORTS).length);
    const ready = candidates.filter(id => {
        const info = knownMembership.get(id);
        return !info || (info.status !== 'dead' && info.status !== 'joining');
    });
    return ready.slice(0, count);
}

app.post('/set', async (req, res) => {
    const { key, value, ttl } = req.body;
    const expiresAt = ttl ? Date.now() + (ttl * 1000) : null;

    // Bug fix: was ring.getNodesForKey (undefined method) — corrected to getNodeForKey.
    const owners = ring.getNodeForKey(key, 2);
    const readyTargets = getReadyOwners(key, 2);
    const joiningOwners = owners.filter(id => knownMembership.get(id)?.status === 'joining');

    // Dual-write: a joining node still gets the write (fire-and-forget,
    // not counted toward success) so it isn't stale for the whole handoff
    // window, only for whatever happened before it announced itself.
    joiningOwners.forEach(id => {
        sendAndWait(id, { type: 'store-set', key, value, expiresAt }).catch(() => {});
    });

    const results = await Promise.allSettled(
        readyTargets.map(id => sendAndWait(id, { type: 'store-set', key, value, expiresAt }))
    );

    const anySucceeded = results.some(r => r.status === 'fulfilled');
    res.json({
        status: anySucceeded ? 'ok' : 'failed',
        key, value, expiresAt,
        routedTo: readyTargets,
        handoffTo: joiningOwners.length ? joiningOwners : undefined,
    });
});

app.get('/get/:key', async (req, res) => {
    const { key } = req.params;

    // Bug fix: primary/replica were referenced without ever being declared.
    const [primary, replica] = getReadyOwners(key, 2);
    if (!primary) return res.status(503).json({ status: 'no ready node for key' });

    try {
        const result = await sendAndWait(primary, { type: 'store-get', key });
        if (result.found) return res.json({ status: 'ok', key, value: result.value, servedBy: primary });
        if (!replica) return res.status(404).json({ status: 'not found' });
    } catch (err) {
        if (!replica) return res.status(503).json({ status: 'primary unreachable, no replica' });
        console.log(`[api] primary ${primary} timed out, falling back to replica ${replica}`);
    }

    try {
        const result = await sendAndWait(replica, { type: 'store-get', key });
        if (result.found) return res.json({ status: 'ok', key, value: result.value, servedBy: replica, fallback: true });
        return res.status(404).json({ status: 'not found' });
    } catch (err2) {
        return res.status(503).json({ status: 'both primary and replica unreachable' });
    }
});

app.delete('/del/:key', async (req, res) => {
    const { key } = req.params;
    if (!key) return res.status(400).json({ status: 'bad request' });

    // Bug fix: primary/replica were referenced without ever being declared.
    const [primary, replica] = getReadyOwners(key, 2);

    const results = await Promise.allSettled([
        primary ? sendAndWait(primary, { type: 'store-delete', key }) : Promise.reject(),
        replica ? sendAndWait(replica, { type: 'store-delete', key }) : Promise.reject(),
    ]);

    const anySucceeded = results.some(r => r.status === 'fulfilled');
    res.json({
        status: anySucceeded ? 'ok' : 'failed',
        key,
        routedTo: [primary, replica].filter(Boolean),
    });
});

function refreshMembership() {
    const ids = Object.keys(NODE_PORTS);
    const randomNodeId = ids[Math.floor(Math.random() * ids.length)];
    const query = JSON.stringify({ type: 'membership-query', fromPort: API_PORT });
    socket.send(query, NODE_PORTS[randomNodeId], 'localhost');
}
setInterval(refreshMembership, 3000);

app.listen(3000, () => console.log('Cache API listening on port 3000'));