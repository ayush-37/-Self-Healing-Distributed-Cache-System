// Node process entry point: parse configuration, build shared state, and wire modules.
import dgram from 'dgram';
import { HashRing } from './hashRing.js';
import { createFailureDetection } from './failureDetection.js';
import { createMembershipHandlers } from './membershipHandlers.js';
import { createStoreHandlers } from './storeHandlers.js';

const MY_ID = process.argv[2];
const MY_PORT = parseInt(process.argv[3]);
const PEERS = JSON.parse(process.argv[4]);
const MY_HOST = process.argv[5] || 'localhost';
PEERS.forEach(p => { if (!p.host) p.host = 'localhost'; });

const MAX_KEYS = parseInt(process.argv[6]) || 50;
const EVICTION_POLICY = (process.argv[7] || 'lru').toLowerCase();
const PROBE_TIMEOUT_MS = 500;

const pendingProbes = new Map();
const membership = new Map();
membership.set(MY_ID, { status: 'joining', incarnation: 0, lastUpdated: Date.now(), port: MY_PORT, host: MY_HOST });
PEERS.forEach(p => membership.set(p.id, { status: 'alive', incarnation: 0, lastUpdated: Date.now(), port: p.port, host: p.host }));

// Preserve known endpoint details across status changes.
function updateMembership(id, patch) {
    const existing = membership.get(id) || {};
    membership.set(id, { ...existing, ...patch, lastUpdated: Date.now() });
}

const joinMsg = JSON.stringify({ type: 'join', id: MY_ID, port: MY_PORT, host: MY_HOST, status: 'joining' });
const ring = new HashRing(150);
[MY_ID, ...PEERS.map(p => p.id)].sort().forEach(n => ring.addNode(n));

// Values use an absolute expiry timestamp so replicas expire consistently.
const localStore = new Map();
const socket = dgram.createSocket('udp4');

const store = createStoreHandlers({ localStore, socket, myId: MY_ID, maxKeys: MAX_KEYS, evictionPolicy: EVICTION_POLICY });
const membershipHandlers = createMembershipHandlers({
    myId: MY_ID, myPort: MY_PORT, myHost: MY_HOST, peers: PEERS, membership, ring, localStore, socket,
    updateMembership, evictIfNeeded: store.evictIfNeeded,
});
const failureDetection = createFailureDetection({
    myId: MY_ID, myPort: MY_PORT, myHost: MY_HOST, peers: PEERS, membership, pendingProbes, socket, updateMembership,
});

const messageHandlers = {
    gossip: membershipHandlers.handleGossip,
    probe: failureDetection.handleProbe,
    ack: failureDetection.handleAck,
    indirectProbe: failureDetection.handleIndirectProbe,
    indirectAck: failureDetection.handleIndirectAck,
    'store-set': store.handleStoreSet,
    'store-get': store.handleStoreGet,
    'store-delete': store.handleStoreDelete,
    'membership-query': membershipHandlers.handleMembershipQuery,
    'resync-request': membershipHandlers.handleResyncRequest,
    'resync-response': membershipHandlers.handleResyncResponse,
    join: membershipHandlers.handleJoin,
};

socket.on('message', (msg, rinfo) => {
    const message = JSON.parse(msg.toString());
    const handler = messageHandlers[message.type];
    if (!handler) return;
    if (message.type === 'store-set' || message.type === 'store-get' || message.type === 'store-delete') handler(message, rinfo);
    else handler(message);
});

socket.on('error', err => {
    console.error(`[${MY_ID}] UDP socket error (non-fatal, continuing):`, err.message);
});

socket.bind(MY_PORT, () => {
    console.log(`[${MY_ID}] gossip node listening on port ${MY_PORT}`);
    PEERS.forEach(p => socket.send(joinMsg, p.port, p.host));
    console.log(`[${MY_ID}] announced join to ${PEERS.length} peer(s)`);
    setTimeout(membershipHandlers.attemptResync, 3000);
});

setInterval(failureDetection.gossipCycle, 1000);
setInterval(failureDetection.probeCycle, 1000);
setInterval(store.sweepExpiredKeys, 2000);
setInterval(() => console.log(`[${MY_ID}] membership view:`, Object.fromEntries(membership)), 5000);
