/**
 * Sheet Writer — pushes enrichment results to Google Sheet
 * Reads all worker progress files, writes scores to Sheet in batch.
 * 
 * Usage: node sheet-writer.js
 */

import { execSync } from 'child_process';
import { readFileSync, existsSync } from 'fs';

const SHEET_ID = '1w8yEzOVUbn8Td4jqmTx9ezLfKU-D1ESynfqBcjE8gX4';

// Collect all progress data
const allProgress = [];

// Original single-worker progress
const origFile = 'enrich-progress.json';
if (existsSync(origFile)) {
  try { allProgress.push(...JSON.parse(readFileSync(origFile, 'utf8'))); } catch {}
}

// Parallel worker progress
for (let w = 1; w <= 3; w++) {
  const f = `enrich-worker-${w}-progress.json`;
  if (existsSync(f)) {
    try { allProgress.push(...JSON.parse(readFileSync(f, 'utf8'))); } catch {}
  }
}

// Deduplicate by row
const seen = new Set();
const unique = allProgress.filter(p => {
  if (seen.has(p.row)) return false;
  seen.add(p.row);
  return true;
});

console.error(`Writing ${unique.length} enriched rows to Sheet...`);

// Sort by row number for orderly writes
unique.sort((a, b) => a.row - b.row);

let written = 0, failed = 0;

// Batch write in chunks of 5 (smaller batches = more reliable)
for (let i = 0; i < unique.length; i += 5) {
  const chunk = unique.slice(i, i + 5);
  
  for (const p of chunk) {
    const vals = JSON.stringify([[p.score, p.tier, p.triggers, p.peStatus, '']]);
    try {
      execSync(`gog sheets update "${SHEET_ID}" "L${p.row}:P${p.row}" --values-json '${vals.replace(/'/g, "'\\''")}' --input USER_ENTERED`, { encoding: 'utf8', timeout: 30000 });
      written++;
    } catch (err) {
      console.error(`  Row ${p.row} write failed: ${err.message}`);
      failed++;
    }
    // Small delay between writes
    await new Promise(r => setTimeout(r, 500));
  }
  
  // Longer delay between chunks
  await new Promise(r => setTimeout(r, 2000));
}

console.error(`\nSheet write complete: ${written} written, ${failed} failed`);

// Summary
const hot = unique.filter(p => p.tier === 'HOT').length;
const warm = unique.filter(p => p.tier === 'WARM').length;
const low = unique.filter(p => p.tier === 'LOW').length;
console.error(`\nTOTAL: ${unique.length} companies | HOT: ${hot} | WARM: ${warm} | LOW: ${low}`);

// Top 20
const sorted = [...unique].sort((a, b) => b.score - a.score);
console.error(`\n=== TOP 20 ===`);
sorted.slice(0, 20).forEach((p, i) => {
  console.error(`${i + 1}. [${p.tier}] ${p.score}pts — ${p.name} — ${p.triggers}`);
});
