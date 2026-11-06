// Direct and indirect probes plus periodic gossip/failure detection cycles.
export function createFailureDetection({ myId, myPort, myHost, peers, membership, pendingProbes, socket, updateMembership }) {
    const PROBE_TIMEOUT_MS = 500;
    const INDIRECT_TIMEOUT_MS = 700;
    const INDIRECT_PROBE_COUNT = 2;

    function handleProbe({ fromPort, fromHost }) {
        socket.send(JSON.stringify({ type: 'ack', from: myId }), fromPort, fromHost || 'localhost');
    }

    function handleAck({ from }) {
        const pending = pendingProbes.get(from);
        if (pending) {
            clearTimeout(pending.timeoutHandle);
            pendingProbes.delete(from);
            const existing = membership.get(from);
            if (existing && existing.status !== 'alive') {
                updateMembership(from, { status: 'alive' });
                console.log(`[${myId}] ${from} confirmed alive (direct ack)`);
            }
        }
        const relayPending = pendingProbes.get(`relay-${from}`);
        if (relayPending) {
            clearTimeout(relayPending.timeoutHandle);
            pendingProbes.delete(`relay-${from}`);
            const { from: askerId, fromPort: askerPort, fromHost: askerHost } = relayPending.relayFor;
            reportIndirectResult(askerId, askerPort, askerHost, from, true);
        }
    }

    function handleIndirectProbe({ from, fromPort, fromHost, target, targetPort, targetHost }) {
        socket.send(JSON.stringify({ type: 'probe', from: myId, fromPort: myPort, fromHost: myHost }), targetPort, targetHost || 'localhost');
        const relayTimeout = setTimeout(() => reportIndirectResult(from, fromPort, fromHost, target, false), PROBE_TIMEOUT_MS);
        pendingProbes.set(`relay-${target}`, { timeoutHandle: relayTimeout, relayFor: { from, fromPort, fromHost } });
    }

    function reportIndirectResult(from, fromPort, fromHost, target, isAlive) {
        socket.send(JSON.stringify({ type: 'indirectAck', from: myId, target, isAlive }), fromPort, fromHost || 'localhost');
    }

    function handleIndirectAck({ from, target, isAlive }) {
        const pending = pendingProbes.get(target);
        if (!pending) return;
        if (isAlive) {
            clearTimeout(pending.timeoutHandle);
            pendingProbes.delete(target);
            updateMembership(target, { status: 'alive' });
            console.log(`[${myId}] ${target} confirmed alive (indirect via ${from})`);
        } else {
            pending.indirectResults.push(false);
        }
    }

    function gossipCycle() {
        const alivePeers = peers.filter(p => membership.get(p.id)?.status !== 'dead');
        const deadPeers = peers.filter(p => membership.get(p.id)?.status === 'dead');
        const targets = [];
        if (alivePeers.length > 0) targets.push(alivePeers[Math.floor(Math.random() * alivePeers.length)]);
        if (deadPeers.length > 0 && Math.random() < 0.2) targets.push(deadPeers[Math.floor(Math.random() * deadPeers.length)]);
        if (targets.length === 0) return;
        const payload = JSON.stringify({ type: 'gossip', from: myId, gossip: Object.fromEntries(membership) });
        targets.forEach(target => socket.send(payload, target.port, target.host, () => console.log(`[${myId}] gossiped to ${target.id}`)));
    }

    function probeCycle() {
        const candidates = [...peers].filter(p => p.id !== myId && membership.get(p.id)?.status !== 'dead');
        if (candidates.length === 0) return;
        const target = candidates[Math.floor(Math.random() * candidates.length)];
        const probe = JSON.stringify({ type: 'probe', from: myId, fromPort: myPort, fromHost: myHost });
        socket.send(probe, target.port, target.host);
        const timeoutHandle = setTimeout(() => handleProbeTimeout(target), PROBE_TIMEOUT_MS);
        pendingProbes.set(target.id, { timeoutHandle, indirectResults: [] });
    }

    function handleProbeTimeout(target) {
        console.log(`[${myId}] no ack from ${target.id} - marking suspected, asking for help`);
        const existing = membership.get(target.id);
        updateMembership(target.id, { status: 'suspected', incarnation: existing.incarnation });
        const helpers = peers.filter(p => p.id !== target.id && p.id !== myId).sort(() => 0.5 - Math.random()).slice(0, INDIRECT_PROBE_COUNT);
        helpers.forEach(helper => {
            const req = JSON.stringify({ type: 'indirectProbe', from: myId, fromPort: myPort, fromHost: myHost, target: target.id, targetPort: target.port, targetHost: target.host });
            socket.send(req, helper.port, helper.host);
        });
        setTimeout(() => {
            const pending = pendingProbes.get(target.id);
            if (!pending) return;
            const anyAlive = pending.indirectResults.includes(true);
            if (!anyAlive) {
                updateMembership(target.id, { status: 'dead', incarnation: existing.incarnation + 1 });
                console.log(`[${myId}] ${target.id} marked DEAD - no direct or indirect ack`);
            }
            pendingProbes.delete(target.id);
        }, INDIRECT_TIMEOUT_MS);
    }

    return { handleProbe, handleAck, handleIndirectProbe, handleIndirectAck, gossipCycle, probeCycle };
}
