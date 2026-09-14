import dgram from 'dgram';
import { HashRing } from './hashRing.js';


// --- Command-line args when you launch this file ---
//  process.argv = ["node", "script.js", "arg1", "arg2", ...]
//

const MY_ID = process.argv[2];  // Gets the 3rd thing you typed
const MY_PORT = parseInt(process.argv[3]);  // Gets the 4th thing you typed
const PEERS = JSON.parse(process.argv[4]);

// --- Failure detection ---
// Tracks probes we've sent out and are waiting on
const pendingProbes = new Map();
// key: nodeId being probed, value:{timeoutHandle, indirectResults:[]}

const PROBE_TIMEOUT_MS = 500;      // how long to wait for a direct ack
const INDIRECT_TIMEOUT_MS = 700;   // how long to wait for indirect probes to report back
const INDIRECT_PROBE_COUNT = 2;    // how many peers to ask for indirect help

// --- Membership list ---
// Each node keeps a local map of everyone it knows about, including itself.
// status: 'alive' | 'suspected' | 'dead'  (suspected/dead logic comes Day 5)
// incarnation: version number — higher incarnation always wins when merging
const membership = new Map();
membership.set(MY_ID, {status: 'alive', incarnation: 0, lastUpdated: Date.now(), port: MY_PORT});
PEERS.forEach(p =>{
    membership.set(p.id, {status: 'alive', incarnation: 0, lastUpdated: Date.now(), port: p.port});       
});

const ring = new HashRing(150);
[MY_ID, ...PEERS.map(p =>p.id)].sort().forEach(n => ring.addNode(n));

// --- Local key-value store for THIS node ---
const localStore = new Map();


const socket = dgram.createSocket('udp4');

// When a gossip message arrives from any peer, merge its view into our own
socket.on('message', (msg, rinfo) =>{
    const message = JSON.parse(msg.toString()); // turns the incoming binary data to the message (toString()) and then parse it to a JS object
    
    if(message.type === 'gossip'){
        handleGossip(message);
    } else if(message.type === 'probe'){
        handleProbe(message);
    } else if(message.type === 'ack'){
        handleAck(message);
    } else if(message.type === 'indirectProbe'){
        handleIndirectProbe(message);
    } else if(message.type === 'indirectAck'){
        handleIndirectAck(message);
    } else if (message.type === 'store-set') {
      handleStoreSet(message, rinfo);
    } else if (message.type === 'store-get') {
      handleStoreGet(message, rinfo);
    } else if (message.type === 'store-delete') {
      handleStoreDelete(message, rinfo);
    } else if(message.type === 'membership-query'){
      handleMembershipQuery(message);
    } else if (message.type === 'resync-request') {
        handleResyncRequest(message);
    } else if (message.type === 'resync-response') {
        handleResyncResponse(message);
    } else if (message.type === 'join') {
        handleJoin(message);
    }

});

function learnNewNode(nodeId, port){
  if(nodeId === MY_ID || !port)return ;
  if(!PEERS.some(p => p.id === nodeId)){
    PEERS.push({id: nodeId, port});
  }
  if(!ring.hasNode(nodeId)){
    ring.addNode(nodeId);
    console.log(`[${MY_ID}] added ${nodeId} to local ring`);
  }
}

function handleJoin({id, port}){
  learnNewNode(id, port);
  if(!membership.has(id)){
    membership.set(id, {status: 'alive', incarnation: 0, lastUpdated: Date.now(), port});

  }
  console.log(`[${MY_ID}] processed join request from ${id}`);
}

function handleResyncRequest({ from, fromPort }){
    // Someone (from) is recovering and wants any data we hold for them.
    // Simple approach: send back our ENTIRE local store; the requester filters what it needs.
    const snapshot = JSON.stringify({
      type: 'resync-response',
      from: MY_ID,
      data: Object.fromEntries(localStore),
    });

    socket.send(snapshot, fromPort, 'localhost');
    console.log(`[${MY_ID}] sent resync response to ${from}`);
}

function handleResyncResponse({from, data}){
  let adopted = 0;
  for(const[key, value] of Object.entries(data)){
    const owners = ring.getNodeForKey(key, 2);
    if(owners.includes(MY_ID) && !localStore.has(key)){
      localStore.set(key, value);
      adopted++;
    }
  }
  console.log(`[${MY_ID}] resync from ${from}: adopted ${adopted} key(s)`);
}

function requestResync(){
  const requests = JSON.stringify({
    type: 'resync-request',
    from: MY_ID,
    fromPort: MY_PORT,
  });

  PEERS.forEach(p => {
    socket.send(requests, p.port, 'localhost',);
  })
}

function handleMembershipQuery({fromPort}){
  const snapshot = JSON.stringify({
    type: 'membership-reply',
    membership: Object.fromEntries(membership),
  });
  socket.send(snapshot, fromPort, 'localhost');
}

function handleStoreSet({ key, value, fromPort }) {
    localStore.set(key,value);
    console.log(`[${MY_ID}] stored key "${key}"`);
    const ack = JSON.stringify({ type: 'store-set-ack', key});
    socket.send(ack, fromPort, 'localhost');
}

function handleStoreGet({ key, fromPort }) {
  const value = localStore.has(key)? localStore.get(key) : null;
  const reply = JSON.stringify({type: 'store-get-reply', key, value, found: localStore.has(key)});
  socket.send(reply, fromPort, 'localhost');
}

function handleStoreDelete({ key, fromPort }) {
  const existed = localStore.has(key);
  localStore.delete(key);
  console.log(`[${MY_ID}] deleted key "${key}" (existed: ${existed})`);
  const ack = JSON.stringify({ type: 'store-delete-ack', key, existed });
  socket.send(ack, fromPort, 'localhost');
}

function handleGossip({from, gossip}){
    for(const [nodeId, info] of Object.entries(gossip)){

        if(nodeId === MY_ID){
          const myInfo = membership.get(MY_ID);
          if(info.status !== 'alive' && info.incarnation >= myInfo.incarnation){
            const newIncarnation = info.incarnation + 1;
            membership.set(MY_ID, {status: 'alive', incarnation: newIncarnation, lastUpdated: Date.now()});
            console.log(`[${MY_ID}] refuting false suspicion -- bumping own incarnation to ${newIncarnation}`);
          }
          continue; // never apply incoming info aout ourselves -- we are the authorityon our own stauts
        }

        const existing = membership.get(nodeId);
        if(!existing || info.incarnation > existing.incarnation || (info.incarnation === existing.incarnation && info.lastUpdated > existing.lastUpdated)){
            membership.set(nodeId, info);
            if(!existing){
                learnNewNode(nodeId, info.port);
                console.log(`[${MY_ID}] learned of new node ${nodeId} via gossip`);
            }
        }
    }
    console.log(`[${MY_ID}] received gossip from ${from}`);
}

function handleProbe({from, fromPort}){
    // Someone is checking if we're alive — reply immediately
    const ack = JSON.stringify({type: 'ack', from: MY_ID});
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
      membership.set(from, { status: 'alive', incarnation: existing.incarnation, lastUpdated: Date.now() });
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

function handleIndirectProbe({from, fromPort, target, targetPort}){
    // Someone (from) is asking us to check on `target` on their behalf
    const ack = JSON.stringify({type: 'probe', from: MY_ID, fromPort: MY_PORT});
    socket.send(ack, targetPort, 'localhost');
    
    // wait briefly for target to respond directly to us, then report back
    const relayTimeout = setTimeout(() =>{
        reportIndirectResult(from, fromPort, target, false);

    }, PROBE_TIMEOUT_MS);

    // temporary one-off listener isn't clean in this simple version —
    // instead, reuse handleAck's pendingProbes map with a special relay flag
    pendingProbes.set(`relay-${target}`, {
        timeoutHandle: relayTimeout,
        relayFor: {from, fromPort},
    });
}

function reportIndirectResult(from, fromPort, target, isAlive) {
  const result = JSON.stringify({ type: 'indirectAck', from: MY_ID, target, isAlive });
  socket.send(result, fromPort, 'localhost');
}

function handleIndirectAck({ from, target, isAlive }) {
  const pending = pendingProbes.get(target);   // plain key, not relay-prefixed
  if (!pending) return;

  if (isAlive) {
    clearTimeout(pending.timeoutHandle);
    pendingProbes.delete(target);
    const existing = membership.get(target);
    membership.set(target, { status: 'alive', incarnation: existing.incarnation, lastUpdated: Date.now() });
    console.log(`[${MY_ID}] ${target} confirmed alive (indirect via ${from})`);
  } else {
    pending.indirectResults.push(false);
  }
}


socket.bind(MY_PORT, () => {
  console.log(`[${MY_ID}] gossip node listening on port ${MY_PORT}`);
  const joinMsg = JSON.stringify({ type: 'join', id: MY_ID, port: MY_PORT});
  PEERS.forEach(p => socket.send(joinMsg, p.port, 'localhost'));
   console.log(`[${MY_ID}] announced join to ${PEERS.length} peer(s)`);
  
  setTimeout(requestResync, 3000); 
    /* Why 3 seconds?
        Give other nodes time to start (if cluster is booting)
        Give gossip a chance to establish membership
        Avoid "thundering herd" of resyncs 
    */
});

// --- The gossip cycle: runs once per second ---
// Picks ONE random peer (currently not marked dead) and sends our
// entire current membership view to them (piggybacking).

function gossipCycle(){
    const alivePeers = PEERS.filter(p => membership.get(p.id)?.status !== 'dead');
    const deadPeers = PEERS.filter(p => membership.get(p.id)?.status === 'dead');

    const targets = [];
    if (alivePeers.length > 0) {
        targets.push(alivePeers[Math.floor(Math.random() * alivePeers.length)]);
    }

    // Occasionally still gossip to nodes we think are dead. If they've actually
    // restarted, this is the only way they ever hear their own old incarnation
    // number and get a chance to self-refute (Day 8's fix can't fire on a
    // message that never arrives).
    if (deadPeers.length > 0 && Math.random() < 0.2) {
        targets.push(deadPeers[Math.floor(Math.random() * deadPeers.length)]);
    }

    if (targets.length === 0) return;

    const payload = JSON.stringify({
        type: 'gossip',
        from: MY_ID,
        gossip: Object.fromEntries(membership),
    });    // Turns a JavaScript object into a text string that can be sent over the network.

    targets.forEach(target => {
        socket.send(payload, target.port, 'localhost', () => {
            console.log(`[${MY_ID}] gossiped to ${target.id}`);
        });
    });
}

function probeCycle(){
    const candidates = [...PEERS].filter(
      p => p.id !== MY_ID && membership.get(p.id)?.status !== 'dead'
    );
    if(candidates.length === 0)return;

    const target = candidates[Math.floor(Math.random() * candidates.length)];
    const probe = JSON.stringify({
        type: 'probe',
        from: MY_ID,
        fromPort: MY_PORT,
    });

    socket.send(probe, target.port, 'localhost');

    const timeoutHandle = setTimeout(() =>{
        handleProbeTimeout(target);
    }, PROBE_TIMEOUT_MS);

    pendingProbes.set(target.id, {timeoutHandle, indirectResults: []});
}


function handleProbeTimeout(target) {
  console.log(`[${MY_ID}] no ack from ${target.id} - marking suspected, asking for help`);

  const existing = membership.get(target.id);
  membership.set(target.id, {
    status: 'suspected',
    incarnation: existing.incarnation,
    lastUpdated: Date.now(),
  });

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
      membership.set(target.id, {
        status: 'dead',
        incarnation: existing.incarnation + 1,
        lastUpdated: Date.now(),
      });
      console.log(`[${MY_ID}] ${target.id} marked DEAD - no direct or indirect ack`);
    }
    pendingProbes.delete(target.id);
  }, INDIRECT_TIMEOUT_MS);
}  


setInterval(gossipCycle, 1000); // one gossip round per second
setInterval(probeCycle, 1000);  // one probe round per second
setInterval(() => {
  console.log(`[${MY_ID}] membership view:`, Object.fromEntries(membership));
}, 5000);


/*
command to run the gossip node in 6 terminals:

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

Terminal 7 (or reuse any terminal once things are running) — test commands:
curl -X POST http://localhost:3000/set -H "Content-Type: application/json" -d '{"key":"foo","value":"bar"}'
curl http://localhost:3000/get/foo
curl -X DELETE http://localhost:3000/del/foo
curl http://localhost:3000/get/foo

*/