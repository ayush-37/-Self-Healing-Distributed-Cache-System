import { hashToInt } from "./hash.js";


export class HashRing{
    constructor(VIRTUAL_NODES = 150){
        this.VIRTUAL_NODES = VIRTUAL_NODES;
        this.ring = []; 
    }

    addNode(nodeId){
        for(let i = 0; i < this.VIRTUAL_NODES; i++){
            const point = hashToInt(`${nodeId}#${i}`);
            this.ring.push({point,nodeId});
        }
        this.ring.sort((a,b) => a.point - b.point);
    }

    removeNode(nodeId){
        this.ring = this.ring.filter(entry => entry.nodeId !== nodeId);
    }

    getNodeForKey(key, replicaCount = 2){
        if(this.ring.length === 0) return [];
        const keyHash = hashToInt(key);

        let lo = 0, hi = this.ring.length - 1, startIdx = 0;

        while(lo <= hi){
            const mid = Math.floor(lo + (hi - lo)/2);
            if(this.ring[mid].point >= keyHash){
                startIdx = mid;
                hi = mid - 1;
            } else{
                lo = mid+1;
            }
        }
        const resultNodes = [];
        const seen = new Set();
        let idx = startIdx;
        let attempts = 0;

        while(resultNodes.length < replicaCount && attempts < this.ring.length){
            const nodeId = this.ring[idx].nodeId;
            if(!seen.has(nodeId)){
                seen.add(nodeId);
                resultNodes.push(nodeId);
            }

            idx = (idx + 1) % this.ring.length;
            attempts++;
        }

        return resultNodes;
    }
}