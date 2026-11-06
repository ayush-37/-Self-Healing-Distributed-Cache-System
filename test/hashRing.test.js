import { HashRing } from '../node/hashRing.js';

function testEvenDistribution() {
  const ring = new HashRing();
  ['A', 'B', 'C', 'D', 'E'].forEach(n => ring.addNode(n));

  const counts = {};
  for (let i = 0; i < 10000; i++) {
    const node = ring.getNodeForKey(`key-${i}`);
    counts[node] = (counts[node] || 0) + 1;
  }
  console.log('Distribution across 5 nodes:', counts);
  // Eyeball this: each should be roughly 2000, not wildly skewed
}

function testMinimalReshuffling() {
  const ring = new HashRing();
  ['A', 'B', 'C', 'D', 'E'].forEach(n => ring.addNode(n));

  const before = {};
  for (let i = 0; i < 10000; i++) {
    before[`key-${i}`] = ring.getNodeForKey(`key-${i}`);
  }

  ring.addNode('E'); // add a 5th node

  let moved = 0;
  for (let i = 0; i < 10000; i++) {
    const after = ring.getNodeForKey(`key-${i}`);
    if (after !== before[`key-${i}`]) moved++;
  }
  console.log(`Keys reshuffled after adding node E: ${moved} / 10000`);
  // Expect roughly 1/5 (~2000) moved — NOT close to 10000 (which modulo hashing would cause)
}

function testVirtualNodesImpact() {
  console.log('\n--- Virtual Nodes Impact on Distribution ---');
  
  const configs = [
    { name: '5 virtual nodes', count: 5 },
    { name: '50 virtual nodes', count: 50 },
    { name: '150 virtual nodes', count: 150 },
    { name: '500 virtual nodes', count: 500 }
  ];
  
  configs.forEach(config => {
    const ring = new HashRing(config.count);
    ['A', 'B', 'C'].forEach(n => ring.addNode(n));
    
    const counts = {};
    for (let i = 0; i < 10000; i++) {
      const node = ring.getNodeForKey(`key-${i}`);
      counts[node] = (counts[node] || 0) + 1;
    }
    
    const values = Object.values(counts);  //  Get the counts as an array
    // If counts = { A: 3350, B: 3325, C: 3325 }
    // values = [3350, 3325, 3325]
    
    const avg = values.reduce((a, b) => a + b, 0) / values.length;
    // reduce((a, b) => a + b, 0) adds all numbers: 3350 + 3325 + 3325 = 10000
    // Divide by number of values: 10000 / 3 = 3333.33
    // avg = 3333.33

    const maxDeviation = Math.max(...values.map(v => Math.abs(v - avg) / avg * 100));
    // For A: |3350 - 3333.33| / 3333.33 * 100 = 16.67 / 3333.33 * 100 = 0.5%
    // For B: |3325 - 3333.33| / 3333.33 * 100 = 8.33 / 3333.33 * 100 = 0.25%
    // For C: |3325 - 3333.33| / 3333.33 * 100 = 8.33 / 3333.33 * 100 = 0.25%
    // Result: [0.5, 0.25, 0.25]

    console.log(`${config.name}: Max deviation = ${maxDeviation.toFixed(2)}%`);
    // Math.max(0.5, 0.25, 0.25) = 0.5
    // maxDeviation = 0.5%
    
    // This tells us: The most unbalanced node (A) has 0.5% more keys than average. That's very balanced!
    console.log(`  Distribution: ${JSON.stringify(counts)}`);
  });
}

function testPerformance() {
  console.log('\n--- Performance Benchmark ---');
  
  const ring = new HashRing();
  const nodes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'];
  nodes.forEach(n => ring.addNode(n));
  
  // Warm up phase to ensure the Just-in-Time (JIT) compiler has optimized the function before I measure performance. This gives me accurate, real-world numbers.
  // Without warm-up: 100,000 lookups took 120ms  ← First run is slow
  // With warm-up: 100,000 lookups took 45ms   ← Accurate measurement of optimized code
  
  for (let i = 0; i < 1000; i++) {
    ring.getNodeForKey(`key-${i}`);
  }
  
  // Test
  const start = Date.now();
  const iterations = 100000;
  for (let i = 0; i < iterations; i++) {
    ring.getNodeForKey(`key-${i}`);
  }
  const end = Date.now();
 
/*
Why 100,000 iterations?
✅ Large enough to get accurate timing
✅ Small enough to run quickly (< 100ms)
✅ Smooths out random variations

Why 1,000 for warm-up but 100,000 for test?
Warm-up: Enough to trigger JIT optimization
Test: Large enough sample for statistical significance
*/ 


  const totalTime = end - start;
  const avgTimePerLookup = totalTime / iterations;
  
  console.log(`${iterations.toLocaleString()} lookups took ${totalTime}ms`);
  console.log(`Average time per lookup: ${avgTimePerLookup.toFixed(3)}ms`);
  console.log(`Lookups per second: ${Math.round(1000 / avgTimePerLookup)}`);
}


function testModuloComparison() {
  console.log('\n--- Consistent Hashing vs. Naive Modulo (adding a 6th node) ---');

  function simpleHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
    return hash;
  }

  // Consistent hashing side — your real ring, 5 nodes growing to 6
  const ring = new HashRing(150);
  ['A', 'B', 'C', 'D', 'E'].forEach(n => ring.addNode(n));

  const before = {};
  for (let i = 0; i < 10000; i++) before[`key-${i}`] = ring.getNodeForKey(`key-${i}`);

  ring.addNode('F');

  let ringMoved = 0;
  for (let i = 0; i < 10000; i++) {
    if (ring.getNodeForKey(`key-${i}`) !== before[`key-${i}`]) ringMoved++;
  }

  // Naive modulo side — same 10000 keys, same 5->6 node growth
  let moduloMoved = 0;
  for (let i = 0; i < 10000; i++) {
    const h = simpleHash(`key-${i}`);
    if (h % 5 !== h % 6) moduloMoved++;
  }

  console.log(`Consistent hashing: ${ringMoved}/10000 moved (${(ringMoved / 100).toFixed(2)}%)`);
  console.log(`Naive modulo:       ${moduloMoved}/10000 moved (${(moduloMoved / 100).toFixed(2)}%)`);
  console.log(`Theoretical ideal:  ~16.67% (1/6 of keys should land on the new node)`);
}

testEvenDistribution();

testMinimalReshuffling();

testVirtualNodesImpact();

testPerformance();

testModuloComparison();
