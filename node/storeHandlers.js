// UDP handlers and maintenance for this node's local key-value store.
export function createStoreHandlers({ localStore, socket, myId, maxKeys, evictionPolicy }) {
    function evictIfNeeded() {
        if (localStore.size <= maxKeys) return;
        let victimKey = null;

        if (evictionPolicy === 'lru') {
            let minFreq = Infinity;
            for (const [key, entry] of localStore) {
                if (entry.freq < minFreq) {
                    minFreq = entry.freq;
                    victimKey = key;
                }
            }
        } else {
            // Map iteration order keeps the least-recently-used key first.
            victimKey = localStore.keys().next().value;
        }

        if (victimKey !== null) {
            localStore.delete(victimKey);
            console.log(`[${myId}] evicted key "${victimKey}" (${evictionPolicy.toUpperCase()}, at capacity ${maxKeys})`);
        }
    }

    function handleStoreSet({ key, value, expiresAt, fromPort, fromHost }) {
        const isNewKey = !localStore.has(key);
        if (isNewKey) evictIfNeeded();
        // Reinsert existing keys so Map iteration order continues to track LRU.
        if (localStore.has(key)) localStore.delete(key);
        localStore.set(key, { value, expiresAt: expiresAt ?? null });
        console.log(`[${myId}] stored key "${key}"${expiresAt ? ` (expires ${new Date(expiresAt).toLocaleTimeString()})` : ''}`);
        socket.send(JSON.stringify({ type: 'store-set-ack', key }), fromPort, fromHost || 'localhost');
    }

    function handleStoreGet({ key, fromPort, fromHost }) {
        const entry = localStore.get(key);
        let value = null, found = false;
        if (entry) {
            if (entry.expiresAt && Date.now() > entry.expiresAt) {
                localStore.delete(key);
                console.log(`[${myId}] key "${key}" expired on read, deleted`);
            } else {
                value = entry.value;
                found = true;
                entry.freq += 1;
                if (evictionPolicy === 'lru') {
                    localStore.delete(key);
                    localStore.set(key, entry);
                }
            }
        }
        socket.send(JSON.stringify({ type: 'store-get-reply', key, value, found }), fromPort, fromHost || 'localhost');
    }

    function handleStoreDelete({ key, fromPort, fromHost }) {
        const existed = localStore.has(key);
        localStore.delete(key);
        console.log(`[${myId}] deleted key "${key}" (existed: ${existed})`);
        socket.send(JSON.stringify({ type: 'store-delete-ack', key, existed }), fromPort, fromHost || 'localhost');
    }

    function sweepExpiredKeys() {
        const now = Date.now();
        for (const [key, entry] of localStore) {
            if (entry && entry.expiresAt && now > entry.expiresAt) {
                localStore.delete(key);
                console.log(`[${myId}] swept expired key "${key}"`);
            }
        }
    }

    return { handleStoreSet, handleStoreGet, handleStoreDelete, sweepExpiredKeys, evictIfNeeded };
}
