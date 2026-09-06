import dgram from 'dgram';

// --- Command-line args when you launch this file ---
//  process.argv = ["node", "script.js", "arg1", "arg2", ...]
//

const MY_ID = process.argv[2];  // Gets the 3rd thing you typed
const MY_PORT = parseInt(process.argv[3]);  // Gets the 4th thing you typed
const PEERS = JSON.parse(process.argv[4]);

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
socket.on('message', (msg) =>{
    const {from, gossip} = JSON.parse(msg.toString()); // turns the incoming binary data to the message (toString()) and then parse it to a JS object
    console.log(`[${MY_ID}] received message from ${from}:`, gossip);
    for(const [nodeId, info] of Object.entries(gossip)){
        const existing = membership.get(nodeId);
        info.lastUpdated = Date.now();

        // only accept the incoming info if it's newer (higher incarnation)
        if(!existing || info.incarnation > existing.incarnation){
            membership.set(nodeId, info);
        }
    }

    console.log(`[${MY_ID}] received gossip from ${from}`);
});


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
setInterval(gossipCycle, 1000); // one gossip round per second