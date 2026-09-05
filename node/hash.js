import crypto from "crypto";
export function hashToInt(str){
    const hash = crypto.createHash('md5').update(str).digest('hex');
    // take first 8 hex chars, convert to a plain integer
    return parseInt(hash.substring(0,8),16);
}