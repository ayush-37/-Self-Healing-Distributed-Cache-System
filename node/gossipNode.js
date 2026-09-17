import dgram from 'dgram';
import { HashRing } from './hashRing.js';


// --- Command-line args when you launch this file ---
//  process.argv = ["node", "script.js", "arg1", "arg2", ...]

const MY_ID = process.argv[2];
const MY_PORT = parseInt(process.argv[3]);
const PEERS = JSON.parse(process.argv[4]);

// --- Failure detection ---
const pendingProbes = new Map();

const PROBE_TIMEOUT_MS = 500;
const INDIRECT_TIMEOUT_MS = 700;
const INDIRECT_PROBE_COUNT = 2;

// --- Membership list ---
// status: 'joining' | 'alive' | 'suspected' | 'dead'
// incarnation: version number — higher incarnation always wins when merging
const membership = new Map();
membership.set(MY_ID, { status: 'joining', incarnation: 0, lastUpdated: Date.now(), port: MY_PORT });
PEERS.forEach(p => {
    membership.set(p.id, { status: 'alive', incarnation: 0, lastUpdated: Date.now(), port: p.port });
});

// Bug fix: every other membership.set() call site used to write a bare
// {status, incarnation, lastUpdated} object with no `port` field, silently
// erasing it on every suspected/dead/alive-via-ack/self-refute transition.
// All transitions now go through this helper so `port` (and anything else
// already known about the node) survives every update.
function updateMembership(id, patch) {
    const existing = membership.get(id) || {};
    membership.set(id, { ...existing, ...patch, lastUpdated: Date.now() });
}

const joinMsg = JSON.stringify({ type: 'join', id: MY_ID, port: MY_PORT, status: 'joining' });

const ring = new HashRing(150);
[MY_ID, ...PEERS.map(p => p.id)].sort().forEach(n => ring.addNode(n));

// --- Local key-value store for THIS node ---
// Values are stored as { value, expiresAt } where expiresAt is an absolute
// epoch ms timestamp (or null for no TTL) computed once by api.js and sent
// identically to every replica, so expiry is consistent across copies.
const localStore = new Map();

const socket = dgram.createSocket('udp4');

socket.on('message', (msg, rinfo) => {
    const message = JSON.parse(msg.toString());

    if (message.type === 'gossip') {
        handleGossip(message);
    } else if (message.type === 'probe') {
        handleProbe(message);
    } else if (message.type === 'ack') {
        handleAck(message);
    } else if (message.type === 'indirectProbe') {
        handleIndirectProbe(message);
    } else if (message.type === 'indirectAck') {
        handleIndirectAck(message);
    } else if (message.type === 'store-set') {
        handleStoreSet(message, rinfo);
    } else if (message.type === 'store-get') {
        handleStoreGet(message, rinfo);
    } else if (message.type === 'store-delete') {
        handleStoreDelete(message, rinfo);
    } else if (message.type === 'membership-query') {
        handleMembershipQuery(message);
    } else if (message.type === 'resync-request') {
        handleResyncRequest(message);
    } else if (message.type === 'resync-response') {
        handleResyncResponse(message);
    } else if (message.type === 'join') {
        handleJoin(message);
    }
});

function learnNewNode(nodeId, port) {
    if (nodeId === MY_ID || !port) return;
    if (!PEERS.some(p => p.id === nodeId)) {
        PEERS.push({ id: nodeId, port });
    }
    if (!ring.hasNode(nodeId)) {
        ring.addNode(nodeId);
        console.log(`[${MY_ID}] added ${nodeId} to local ring`);
    }
}

function handleJoin({ id, port, status }) {
    learnNewNode(id, port);
    if (!membership.has(id)) {
        membership.set(id, { status: status || 'joining', incarnation: 0, lastUpdated: Date.now(), port });
    }
    console.log(`[${MY_ID}] processed join from ${id} (status: ${status || 'joining'})`);
}

function handleResyncRequest({ from, fromPort }) {
    // Someone (from) is recovering/joining and wants any data we hold for them.
    // Simple approach: send back our ENTIRE local store; the requester filters what it needs.
    const snapshot = JSON.stringify({
        type: 'resync-response',
        from: MY_ID,
        data: Object.fromEntries(localStore),
    });
    socket.send(snapshot, fromPort, 'localhost');
    console.log(`[${MY_ID}] sent resync response to ${from}`);
}

function handleResyncResponse({ from, data }) {
    let adopted = 0;
    const now = Date.now();
    for (const [key, entry] of Object.entries(data)) {
        // Don't resurrect a key that already expired before we could adopt it.
        if (entry && entry.expiresAt && now > entry.expiresAt) continue;

        const owners = ring.getNodeForKey(key, 2);
        if (owners.includes(MY_ID) && !localStore.has(key)) {
            localStore.set(key, entry);
            adopted++;
        }
    }
    console.log(`[${MY_ID}] resync from ${from}: adopted ${adopted} key(s)`);
}

let resyncAttempts = 0;

function attemptResync() {
    resyncAttempts++;
    requestResync();

    if (resyncAttempts < 3) {
        setTimeout(attemptResync, 3000);
    } else {
        setTimeout(completeJoin, 1000);
    }
}

function completeJoin() {
    const info = membership.get(MY_ID);
    if (info.status !== 'joining') return; // already completed, or refuted back to alive
    updateMembership(MY_ID, { status: 'alive', incarnation: info.incarnation + 1 });
    console.log(`[${MY_ID}] finished joining — now alive, incarnation ${info.incarnation + 1}`);
}

function requestResync() {
    const request = JSON.stringify({ type: 'resync-request', from: MY_ID, fromPort: MY_PORT });
    PEERS.forEach(p => socket.send(request, p.port, 'localhost'));
}

function handleMembershipQuery({ fromPort }) {
    const snapshot = JSON.stringify({
        type: 'membership-reply',
        membership: Object.fromEntries(membership),
    });
    socket.send(snapshot, fromPort, 'localhost');
}

function handleStoreSet({ key, value, expiresAt, fromPort }) {
    localStore.set(key, { value, expiresAt: expiresAt ?? null });
    console.log(`[${MY_ID}] stored key "${key}"${expiresAt ? ` (expires ${new Date(expiresAt).toLocaleTimeString()})` : ''}`);
    const ack = JSON.stringify({ type: 'store-set-ack', key });
    socket.send(ack, fromPort, 'localhost');
}

function handleStoreGet({ key, fromPort }) {
    const entry = localStore.get(key);
    let value = null, found = false;

    if (entry) {
        if (entry.expiresAt && Date.now() > entry.expiresAt) {
            localStore.delete(key);
            console.log(`[${MY_ID}] key "${key}" expired on read, deleted`);
        } else {
            value = entry.value;
            found = true;
        }
    }

    const reply = JSON.stringify({ type: 'store-get-reply', key, value, found });
    socket.send(reply, fromPort, 'localhost');
}

function handleStoreDelete({ key, fromPort }) {
    const existed = localStore.has(key);
    localStore.delete(key);
    console.log(`[${MY_ID}] deleted key "${key}" (existed: ${existed})`);
    const ack = JSON.stringify({ type: 'store-delete-ack', key, existed });
    socket.send(ack, fromPort, 'localhost');
}

function sweepExpiredKeys() {
    const now = Date.now();
    for (const [key, entry] of localStore) {
        if (entry && entry.expiresAt && now > entry.expiresAt) {
            localStore.delete(key);
            console.log(`[${MY_ID}] swept expired key "${key}"`);
        }
    }
}

function handleGossip({ from, gossip }) {
    for (const [nodeId, info] of Object.entries(gossip)) {

        if (nodeId === MY_ID) {
            const myInfo = membership.get(MY_ID);
            if (info.status !== 'alive' && info.incarnation >= myInfo.incarnation) {
                const newIncarnation = info.incarnation + 1;
                updateMembership(MY_ID, { status: 'alive', incarnation: newIncarnation });
                console.log(`[${MY_ID}] refuting false suspicion -- bumping own incarnation to ${newIncarnation}`);
            }
            continue; // never apply incoming info about ourselves -- we are the authority on our own status
        }

        const existing = membership.get(nodeId);
        if (!existing || info.incarnation > existing.incarnation || (info.incarnation === existing.incarnation && info.lastUpdated > existing.lastUpdated)) {
            membership.set(nodeId, info);
            if (!existing) {
                learnNewNode(nodeId, info.port);
                console.log(`[${MY_ID}] learned of new node ${nodeId} via gossip`);
            }
        }
    }
    console.log(`[${MY_ID}] received gossip from ${from}`);
}

function handleProbe({ from, fromPort }) {
    const ack = JSON.stringify({ type: 'ack', from: MY_ID });
    socket.send(ack, fromPort, 'localhost');
}

function handleAck({ from }) {
    // Case 1: this ack answers OUR OWN direct probe (from probeCycle)
    const pending = pendingProbes.get(from);
    if (pending) {
        clearTimeout(pending.timeoutHandle);
        pendingProbes.delete(from);

        const existing = membership.get(from);
        if (existing && existing.status !== 'alive') {
            updateMembership(from, { status: 'alive' });
            console.log(`[${MY_ID}] ${from} confirmed alive (direct ack)`);
        }
    }

    // Case 2: this ack answers a probe WE sent on behalf of someone else (indirect relay)
    const relayPending = pendingProbes.get(`relay-${from}`);
    if (relayPending) {
        clearTimeout(relayPending.timeoutHandle);
        pendingProbes.delete(`relay-${from}`);
        const { from: askerId, fromPort: askerPort } = relayPending.relayFor;
        reportIndirectResult(askerId, askerPort, from, true);
    }
}

function handleIndirectProbe({ from, fromPort, target, targetPort }) {
    const ack = JSON.stringify({ type: 'probe', from: MY_ID, fromPort: MY_PORT });
    socket.send(ack, targetPort, 'localhost');

    const relayTimeout = setTimeout(() => {
        reportIndirectResult(from, fromPort, target, false);
    }, PROBE_TIMEOUT_MS);

    pendingProbes.set(`relay-${target}`, {
        timeoutHandle: relayTimeout,
        relayFor: { from, fromPort },
    });
}

function reportIndirectResult(from, fromPort, target, isAlive) {
    const result = JSON.stringify({ type: 'indirectAck', from: MY_ID, target, isAlive });
    socket.send(result, fromPort, 'localhost');
}

function handleIndirectAck({ from, target, isAlive }) {
    const pending = pendingProbes.get(target);
    if (!pending) return;

    if (isAlive) {
        clearTimeout(pending.timeoutHandle);
        pendingProbes.delete(target);
        updateMembership(target, { status: 'alive' });
        console.log(`[${MY_ID}] ${target} confirmed alive (indirect via ${from})`);
    } else {
        pending.indirectResults.push(false);
    }
}

socket.bind(MY_PORT, () => {
    console.log(`[${MY_ID}] gossip node listening on port ${MY_PORT}`);
    PEERS.forEach(p => socket.send(joinMsg, p.port, 'localhost'));
    console.log(`[${MY_ID}] announced join to ${PEERS.length} peer(s)`);

    setTimeout(attemptResync, 3000);
});

function gossipCycle() {
    const alivePeers = PEERS.filter(p => membership.get(p.id)?.status !== 'dead');
    const deadPeers = PEERS.filter(p => membership.get(p.id)?.status === 'dead');

    const targets = [];
    if (alivePeers.length > 0) {
        targets.push(alivePeers[Math.floor(Math.random() * alivePeers.length)]);
    }
    if (deadPeers.length > 0 && Math.random() < 0.2) {
        targets.push(deadPeers[Math.floor(Math.random() * deadPeers.length)]);
    }
    if (targets.length === 0) return;

    const payload = JSON.stringify({
        type: 'gossip',
        from: MY_ID,
        gossip: Object.fromEntries(membership),
    });

    targets.forEach(target => {
        socket.send(payload, target.port, 'localhost', () => {
            console.log(`[${MY_ID}] gossiped to ${target.id}`);
        });
    });
}

function probeCycle() {
    const candidates = [...PEERS].filter(
        p => p.id !== MY_ID && membership.get(p.id)?.status !== 'dead'
    );
    if (candidates.length === 0) return;

    const target = candidates[Math.floor(Math.random() * candidates.length)];
    const probe = JSON.stringify({ type: 'probe', from: MY_ID, fromPort: MY_PORT });
    socket.send(probe, target.port, 'localhost');

    const timeoutHandle = setTimeout(() => {
        handleProbeTimeout(target);
    }, PROBE_TIMEOUT_MS);

    pendingProbes.set(target.id, { timeoutHandle, indirectResults: [] });
}

function handleProbeTimeout(target) {
    console.log(`[${MY_ID}] no ack from ${target.id} - marking suspected, asking for help`);

    const existing = membership.get(target.id);
    updateMembership(target.id, { status: 'suspected', incarnation: existing.incarnation });

    const helpers = PEERS
        .filter(p => p.id !== target.id && p.id !== MY_ID)
        .sort(() => 0.5 - Math.random())
        .slice(0, INDIRECT_PROBE_COUNT);

    helpers.forEach(helper => {
        const req = JSON.stringify({
            type: 'indirectProbe',
            from: MY_ID,
            fromPort: MY_PORT,
            target: target.id,
            targetPort: target.port,
        });
        socket.send(req, helper.port, 'localhost');
    });

    setTimeout(() => {
        const pending = pendingProbes.get(target.id);
        if (!pending) return;

        const anyAlive = pending.indirectResults.includes(true);
        if (!anyAlive) {
            updateMembership(target.id, { status: 'dead', incarnation: existing.incarnation + 1 });
            console.log(`[${MY_ID}] ${target.id} marked DEAD - no direct or indirect ack`);
        }
        pendingProbes.delete(target.id);
    }, INDIRECT_TIMEOUT_MS);
}

setInterval(gossipCycle, 1000);
setInterval(probeCycle, 1000);
setInterval(sweepExpiredKeys, 2000);
setInterval(() => {
    console.log(`[${MY_ID}] membership view:`, Object.fromEntries(membership));
}, 5000);

/*
command to run the gossip node in 5 terminals:

Terminal 1 — Node A:
node node/gossipNode.js A 6001 "[{\"id\":\"B\",\"port\":6002},{\"id\":\"C\",\"port\":6003},{\"id\":\"D\",\"port\":6004},{\"id\":\"E\",\"port\":6005}]"

Terminal 2 — Node B:
node node/gossipNode.js B 6002 "[{\"id\":\"A\",\"port\":6001},{\"id\":\"C\",\"port\":6003},{\"id\":\"D\",\"port\":6004},{\"id\":\"E\",\"port\":6005}]"

Terminal 3 — Node C:
node node/gossipNode.js C 6003 "[{\"id\":\"A\",\"port\":6001},{\"id\":\"B\",\"port\":6002},{\"id\":\"D\",\"port\":6004},{\"id\":\"E\",\"port\":6005}]"

Terminal 4 — Node D:
node node/gossipNode.js D 6004 "[{\"id\":\"A\",\"port\":6001},{\"id\":\"B\",\"port\":6002},{\"id\":\"C\",\"port\":6003},{\"id\":\"E\",\"port\":6005}]"

Terminal 5 — Node E:
node node/gossipNode.js E 6005 "[{\"id\":\"A\",\"port\":6001},{\"id\":\"B\",\"port\":6002},{\"id\":\"C\",\"port\":6003},{\"id\":\"D\",\"port\":6004}]"

Terminal 6 — API server:
node api.js

Terminal 7 — test commands:
curl -X POST http://localhost:3000/set -H "Content-Type: application/json" -d '{"key":"foo","value":"bar"}'
curl http://localhost:3000/get/foo
curl -X POST http://localhost:3000/set -H "Content-Type: application/json" -d '{"key":"session","value":"abc","ttl":5}'
curl -X DELETE http://localhost:3000/del/foo
curl http://localhost:3000/get/foo

Dynamic join test:
node node/gossipNode.js F 6006 "[{\"id\":\"A\",\"port\":6001}]"
*/