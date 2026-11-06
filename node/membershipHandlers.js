// Membership, joining, and state-transfer message handlers.
export function createMembershipHandlers({ myId, myPort, myHost, peers, membership, ring, localStore, socket, updateMembership, evictIfNeeded }) {
    function learnNewNode(nodeId, port, host) {
        if (nodeId === myId || !port) return;
        if (!peers.some(p => p.id === nodeId)) peers.push({ id: nodeId, port, host: host || 'localhost' });
        if (!ring.hasNode(nodeId)) {
            ring.addNode(nodeId);
            console.log(`[${myId}] added ${nodeId} to local ring`);
        }
    }

    function handleJoin({ id, port, host, status }) {
        learnNewNode(id, port, host);
        if (!membership.has(id)) membership.set(id, { status: status || 'joining', incarnation: 0, lastUpdated: Date.now(), port, host: host || 'localhost' });
        console.log(`[${myId}] processed join from ${id} (status: ${status || 'joining'})`);
    }

    function handleResyncRequest({ from, fromPort, fromHost }) {
        const snapshot = JSON.stringify({ type: 'resync-response', from: myId, fromPort: myPort, fromHost: myHost, data: Object.fromEntries(localStore) });
        socket.send(snapshot, fromPort, fromHost || 'localhost');
        console.log(`[${myId}] sent resync response to ${from}`);
    }

    function handleResyncResponse({ from, data }) {
        let adopted = 0;
        const now = Date.now();
        for (const [key, entry] of Object.entries(data)) {
            if (entry && entry.expiresAt && now > entry.expiresAt) continue;
            const owners = ring.getNodeForKey(key, 2);
            if (owners.includes(myId) && !localStore.has(key)) {
                evictIfNeeded();
                localStore.set(key, { ...entry, freq: entry.freq || 1 });
                adopted++;
            }
        }
        console.log(`[${myId}] resync from ${from}: adopted ${adopted} key(s)`);
    }

    let resyncAttempts = 0;
    function attemptResync() {
        resyncAttempts++;
        requestResync();
        if (resyncAttempts < 3) setTimeout(attemptResync, 3000);
        else setTimeout(completeJoin, 1000);
    }

    function completeJoin() {
        const info = membership.get(myId);
        if (info.status !== 'joining') return;
        updateMembership(myId, { status: 'alive', incarnation: info.incarnation + 1 });
        console.log(`[${myId}] finished joining — now alive, incarnation ${info.incarnation + 1}`);
    }

    function requestResync() {
        const request = JSON.stringify({ type: 'resync-request', from: myId, fromPort: myPort, fromHost: myHost });
        peers.forEach(p => socket.send(request, p.port, p.host));
    }

    function handleMembershipQuery({ fromPort, fromHost }) {
        const snapshot = JSON.stringify({ type: 'membership-reply', membership: Object.fromEntries(membership) });
        socket.send(snapshot, fromPort, fromHost || 'localhost');
    }

    function handleGossip({ from, gossip }) {
        for (const [nodeId, info] of Object.entries(gossip)) {
            if (nodeId === myId) {
                const myInfo = membership.get(myId);
                if (info.status !== 'alive' && info.incarnation >= myInfo.incarnation) {
                    const newIncarnation = info.incarnation + 1;
                    updateMembership(myId, { status: 'alive', incarnation: newIncarnation });
                    console.log(`[${myId}] refuting false suspicion -- bumping own incarnation to ${newIncarnation}`);
                }
                continue;
            }
            const existing = membership.get(nodeId);
            if (!existing || info.incarnation > existing.incarnation || (info.incarnation === existing.incarnation && info.lastUpdated > existing.lastUpdated)) {
                membership.set(nodeId, info);
                if (!existing) {
                    learnNewNode(nodeId, info.port, info.host);
                    console.log(`[${myId}] learned of new node ${nodeId} via gossip`);
                }
            }
        }
        console.log(`[${myId}] received gossip from ${from}`);
    }

    return { learnNewNode, handleJoin, handleResyncRequest, handleResyncResponse, attemptResync, handleMembershipQuery, handleGossip };
}
