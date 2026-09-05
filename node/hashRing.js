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

    getNodeForKey(key){
        if(this.ring.length === 0) return null;
        const keyHash = hashToInt(key);

        let lo = 0, hi = this.ring.length - 1, result = 0;

        while(lo <= hi){
            const mid = Math.floor(lo + (hi - lo)/2);
            if(this.ring[mid].point >= keyHash){
                result = mid;
                hi = mid - 1;
            } else{
                lo = mid+1;
            }
        }
        return this.ring[result].nodeId;
    }
}