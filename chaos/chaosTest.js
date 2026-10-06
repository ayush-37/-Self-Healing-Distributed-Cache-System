import { spawn } from 'child_process';

const NODES = [
    { id: 'A', port: 6001 },
    { id: 'B', port: 6002 },
    { id: 'C', port: 6003 },
    { id: 'D', port: 6004 },
    { id: 'E', port: 6005 },
];

const API_URL = 'http://localhost:3000';
const REQUEST_INTERVAL_MS = 100;   // open-loop: fire on a fixed clock, don't wait for prior request
const REQUEST_TIMEOUT_MS = 2000;   // generous — gives server-side primary->replica fallback room to finish
const RETRY_DELAY_MS = 200;
const CONVERGENCE_WAIT_MS = 12000; // all nodes start 'joining' — give them time to finish before trusting results
const KILL_AFTER_MS = 5000;        // how long into steady traffic before we strike
const TOTAL_RUN_MS = 30000;        // total load-test duration after convergence

const children = [];

function spawnNode({ id, port }) {
    const peers = NODES
        .filter(n => n.id !== id)
        .map(n => ({ id: n.id, port: n.port }));
    const args = ['node/gossipNode.js', id, String(port), JSON.stringify(peers)];
    const child = spawn('node', args, { stdio: 'inherit' });
    children.push({ id, child });
    return child;
}

function spawnApi() {
    const child = spawn('node', ['api.js'], { stdio: 'inherit' });
    children.push({ id: 'api', child });
    return child;
}

function killRandomNode() {
    const candidates = children.filter(c => c.id !== 'api');
    const victim = candidates[Math.floor(Math.random() * candidates.length)];
    console.log(`\n💥 CHAOS: killing node ${victim.id} (pid ${victim.child.pid}) — SIGKILL, no graceful shutdown\n`);
    victim.child.kill('SIGKILL'); // SIGKILL, not SIGTERM — a real crash gets no cleanup, and this shouldn't either
}

function shutdownAll() {
    children.forEach(({ child }) => {
        try { child.kill('SIGKILL'); } catch (e) {}
    });
}

// --- Load generation ---

const stats = { total: 0, cleanSuccess: 0, recoveredOnRetry: 0, permanentFailure: 0 };
let knownKeys = [];
let counter = 0;

async function request(path, options) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
        const res = await fetch(`${API_URL}${path}`, { ...options, signal: controller.signal });
        clearTimeout(timeout);
        if (!res.ok) throw new Error(`status ${res.status}`);
        return await res.json();
    } catch (err) {
        clearTimeout(timeout);
        throw err;
    }
}

async function fireOneRequest() {
    stats.total++;
    const doSet = knownKeys.length === 0 || Math.random() < 0.5;

    const attempt = async () => {
        if (doSet) {
            const key = `chaos-key-${counter++}`;
            const value = `v${Date.now()}`;
            await request('/set', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ key, value }),
            });
            knownKeys.push(key);
            if (knownKeys.length > 200) knownKeys.shift();
        } else {
            const key = knownKeys[Math.floor(Math.random() * knownKeys.length)];
            await request(`/get/${key}`);
        }
    };

    try {
        await attempt();
        stats.cleanSuccess++;
    } catch (err1) {
        await new Promise(r => setTimeout(r, RETRY_DELAY_MS));
        try {
            await attempt();
            stats.recoveredOnRetry++;
            console.log(`  ↻ recovered on retry`);
        } catch (err2) {
            stats.permanentFailure++;
            console.log(`  ❌ PERMANENT FAILURE: ${err2.message}`);
        }
    }
}

function printSummary() {
    console.log('\n===== CHAOS TEST SUMMARY =====');
    console.log(`Total requests:        ${stats.total}`);
    console.log(`Clean success:         ${stats.cleanSuccess}`);
    console.log(`Recovered on retry:    ${stats.recoveredOnRetry}`);
    console.log(`Permanent failures:    ${stats.permanentFailure}`);
    const failRate = ((stats.permanentFailure / stats.total) * 100).toFixed(2);
    console.log(`Permanent failure rate: ${failRate}%`);
    console.log('================================\n');
}

// --- Orchestration ---

async function main() {
    console.log('Spawning 5-node cluster + api.js...');
    NODES.forEach(spawnNode);
    spawnApi();

    console.log(`Waiting ${CONVERGENCE_WAIT_MS / 1000}s for join/convergence before starting load...`);
    await new Promise(r => setTimeout(r, CONVERGENCE_WAIT_MS));

    console.log('Starting load generation (open-loop, fixed interval)...');
    const loadInterval = setInterval(fireOneRequest, REQUEST_INTERVAL_MS);

    setTimeout(killRandomNode, KILL_AFTER_MS);

    setTimeout(() => {
        clearInterval(loadInterval);
        printSummary();
        shutdownAll();
        process.exit(0);
    }, TOTAL_RUN_MS);
}

process.on('SIGINT', () => {
    printSummary();
    shutdownAll();
    process.exit(0);
});

main();