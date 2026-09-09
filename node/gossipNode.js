import dgram from 'dgram';

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
membership.set(MY_ID, {status: 'alive', incarnation: 0, lastUpdated: Date.now()});
PEERS.forEach(p =>{
    membership.set(p.id, {status: 'alive', incarnation: 0, lastUpdated: Date.now()});       
});


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
    }
});


function handleGossip({from, gossip}){
    for(const [nodeId, info] of Object.entries(gossip)){
        const existing = membership.get(nodeId);
        if(!existing || info.incarnation > existing.incarnation || (info.incarnation === existing.incarnation && info.lastUpdated > existing.lastUpdated)){
            membership.set(nodeId, info);
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
});

// --- The gossip cycle: runs once per second ---
// Picks ONE random peer (currently not marked dead) and sends our
// entire current membership view to them (piggybacking).

function gossipCycle(){
    const alivePeers = PEERS.filter(p => membership.get(p.id)?.status !== 'dead');
    if(alivePeers.length === 0) return;

    const target = alivePeers[Math.floor(Math.random() * alivePeers.length)];

    const payload = JSON.stringify({
        from: MY_ID,
        gossip: Object.fromEntries(membership),
    });    // Turns a JavaScript object into a text string that can be sent over the network.

    socket.send(payload, target.port, 'localhost', () =>{
        console.log(`[${MY_ID}] gossiped to ${target.id}`);
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
command to run the gossip node in three terminals:

node node/gossipNode.js A 6001 "[{\"id\":\"B\",\"port\":6002},{\"id\":\"C\",\"port\":6003}]" on one terminal
node node/gossipNode.js B 6002 "[{\"id\":\"A\",\"port\":6001},{\"id\":\"C\",\"port\":6003}]" on another terminal    
node node/gossipNode.js C 6003 "[{\"id\":\"A\",\"port\":6001},{\"id\":\"B\",\"port\":6002}]" on third terminal

*/