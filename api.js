import express from 'express';
import { HashRing } from './node/hashRing.js';

const app = express();
app.use(express.json());
const ring = new HashRing(150);
['A', 'B', 'C','D', 'E'].forEach(n => ring.addNode(n));

// In-memory store PER cache node (this file simulates the routing layer for now)
const store = new Map();

app.post('/set', (req, res) => {
    const { key, value } = req.body;
    if (!key || value === undefined) {
        return res.status(400).json({ 
            status: 'error', 
            message: 'Missing key or value' 
        });
    }

    const nodes = ring.getNodeForKey(key, 2); // [primary, replica]

    store.set(key, { value, nodes });
    console.log(`[SET] key="${key}", value="${value}", nodes=${nodes.join(', ')}`);

    res.json({status: 'ok', key, value, routeTo: nodes});
});

app.get('/get/:key', (req, res) => {
    const { key } = req.params;
    const entry = store.get(key);
    if (!entry) {
        console.log(`[GET] key="${key}" - NOT FOUND`);
        return res.status(404).json({ 
            status: 'not found', 
            key 
        });
    }
    console.log(`[GET] key="${key}" - nodes: ${entry.nodes.join(', ')}`);

    res.json({status: 'ok', key, value: entry.value, routedTo: entry.nodes});
});

app.delete('/del/:key', (req, res) => {
    store.delete(req.params.key);
    const existed = store.delete(key);
    res.json({ 
        status: existed ? 'deleted' : 'not found',
        key 
    });
});
app.listen(3000, () => console.log('Cache API listening on port 3000'));

