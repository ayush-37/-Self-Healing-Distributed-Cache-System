import express from 'express';
import dgram from 'dgram';
import { HashRing } from './node/hashRing.js';

const app = express();
app.use(express.json());

const ring = new HashRing(150);
['A', 'B', 'C','D', 'E'].forEach(n => ring.addNode(n));

const NODE_PORTS = {
    A: 6001, B: 6002, C: 6003, D: 6004, E: 6005,
};

// api.js's own lightweight view of cluster membership, kept fresh by periodic queries
let knownMembership = new Map();
const ALL_NODE_IDS = Object.keys(NODE_PORTS);

const API_PORT = 7000; // this API process's own UDP port, for receiving replies
const socket = dgram.createSocket('udp4');
socket.bind(API_PORT);

// Track requests waiting for a reply, keyed by a unique request id
const pendingRequests = new Map();
let requestCounter = 0;

// s

socket.on('message', (msg) => {
    const message = JSON.parse(msg.toString());

    if (message.type === 'membership-reply') {
    knownMembership = new Map(Object.entries(message.membership));

    for(const [id, info] of knownMembership){
        if(!NODE_PORTS[id] && info.port){
            NODE_PORTS[id] = info.port;
            if(!ring.hasNode(id)){
                ring.addNode(id);
                console.log(`[api] learned of new node ${id} on port ${info.port}, added to ring`);
            }
        }
    }
    return;
  }

    const pending = pendingRequests.get(message.key + ':' + message.type);  // fixed: ':' not '.'
    if (pending) {
        clearTimeout(pending.timeoutHandle);
        pendingRequests.delete(message.key + ':' + message.type);            // fixed here too
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


app.post('/set', async (req, res) => {
    const { key, value } = req.body;
    const [primary, replica] = ring.getNodeForKey(key, 2);

    const targets = [primary, replica].filter(nodeId => {
    const info = knownMembership.get(nodeId);
        return !info || info.status !== 'dead'; // skip known-dead, allow unknown (fail open)
    });

    const results = await Promise.allSettled(
        targets.map(nodeId => sendAndWait(nodeId, {type: 'store-set', key, value}))
    );

    const anySucceeded = results.some(r => r.status === 'fulfilled');
    res.json({
        status: anySucceeded ? 'ok' : 'failed',
        key, value, 
        routedTo: [primary, replica],
    });
});

app.get('/get/:key', async (req, res) => {
    const { key } = req.params;
    const [primary, replica] = ring.getNodeForKey(key, 2);

    const primaryInfo = knownMembership.get(primary);
    const primaryKnownDead = primaryInfo && primaryInfo.status === 'dead';

    if(!primaryKnownDead){
        try{
            const result = await sendAndWait(primary, {type: 'store-get', key});
            if(result.found){
                return res.json({status: 'ok', key, value: result.value, servedBy: primary});
            }
            return res.status(404).json({status: 'not found'});
        } catch(err){
            console.log(`[api] primary ${primary} timed out, falling back to replica ${replica}`);
        }
    } else {
        console.log(`[api] primary ${primary} known dead, going straight to replica ${replica}`);
    }

    try{
        const result = await sendAndWait(replica, {type: 'store-get', key});
        if(result.found){
            return res.json({status: 'ok', key, value: result.value, servedBy: replica});
        }

        return res.status(404).json({status: 'not found'});
    } catch(err2){
            return res.status(503).json({status: 'both primary and replica unreachable'});
    }
});

app.delete('/del/:key', async (req, res) => {
  const { key } = req.params;
  const [primary, replica] = ring.getNodeForKey(key, 2);

  const results = await Promise.allSettled([
    sendAndWait(primary, { type: 'store-delete', key }),
    sendAndWait(replica, { type: 'store-delete', key }),
  ]);

  const anySucceeded = results.some(r => r.status === 'fulfilled');
  res.json({
    status: anySucceeded ? 'ok' : 'failed',
    key,
    routedTo: [primary, replica],
  });
});

function refreshMembership(){
    const ids = Object.keys(NODE_PORTS);
    const randomNodeId = ids[Math.floor(Math.random() * ids.length)];
    const query = JSON.stringify({type: 'membership-query', fromPort: API_PORT});
    socket.send(query, NODE_PORTS[randomNodeId], 'localhost');
}
setInterval(refreshMembership, 3000); // refresh every 3 seconds


app.listen(3000, () => console.log('Cache API listening on port 3000'));

