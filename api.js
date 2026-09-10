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

const API_PORT = 7000; // this API process's own UDP port, for receiving replies
const socket = dgram.createSocket('udp4');
socket.bind(API_PORT);

// Track requests waiting for a reply, keyed by a unique request id
const pendingRequests = new Map();
let requestCounter = 0;

// socket.on('message', (msg) =>{
//     const message = JSON.parse(msg.toString());
//     console.log(`[api] received message: ${JSON.stringify(message)}`);
//     const pending = pendingRequests.get(message.key + '.' + message.type);
//     if(pending){
//         clearTimeout(pending.timeoutHandle);
//         pendingRequests.delete(message.key + '.' + message.type);
//         pending.resolve(message);
//     }
// });

socket.on('message', (msg) => {
    const message = JSON.parse(msg.toString());
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

    const results = await Promise.allSettled([
        sendAndWait(primary, {type: 'store-set', key, value}),
        sendAndWait(replica, {type: 'store-set', key, value})
    ]);

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

    try{
        const result = await sendAndWait(primary, {type: 'store-get', key});
        if(result.found){
            return res.json({status: 'ok', key, value: result.value, servedBy: primary});
        }
        return res.status(404).json({status: 'not found'});
    } catch(err){
        // primary timed out — fall back to replica
        console.log(`[api] primary ${primary} timed out, falling back to replica ${replica}`);
        try{
            const result = await sendAndWait(replica, {type: 'store-get', key});
            if(result.found){
                return res.json({status: 'ok', key, value: result.value, servedBy: replica});
            }

            return res.status(404).json({status: 'not found'});
        } catch(err){
            return res.status(503).json({status: 'both primary and replica unreachable'});
        }
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


app.listen(3000, () => console.log('Cache API listening on port 3000'));

