/**
 * Enrichment Worker v3 — crash-safe, no shell spawns during loop
 * 
 * - Reads Sheet data ONCE at startup via gog CLI
 * - Saves all enrichment results to local JSON (no Sheet writes during loop)
 * - Separate `sheet-writer.js` pushes results to Sheet after workers finish
 * - Checkpoint: only marks row done AFTER progress is saved
 * - Failed rows are NOT checkpointed → resume will retry them
 * 
 * Usage: node enrich-worker.js <workerId> <startRow> <endRow>
 * Resume: node enrich-worker.js <workerId> <startRow> <endRow> --resume
 */

import { launch } from 'cloakbrowser';
import { randomInt } from 'crypto';
import { readFileSync, writeFileSync, existsSync } from 'fs';

const SHEET_ID = '1w8yEzOVUbn8Td4jqmTx9ezLfKU-D1ESynfqBcjE8gX4';
const WORKER_ID = process.argv[2] || '1';
const START_ROW = parseInt(process.argv[3]) || 2;
const END_ROW = parseInt(process.argv[4]) || 50;
const CHECKPOINT_FILE = `enrich-worker-${WORKER_ID}-checkpoint.json`;
const PROGRESS_FILE = `enrich-worker-${WORKER_ID}-progress.json`;

function loadJson(path, fallback) {
  if (existsSync(path)) { try { return JSON.parse(readFileSync(path, 'utf8')); } catch {} }
  return fallback;
}
function saveJson(path, data) { writeFileSync(path, JSON.stringify(data, null, 2)); }
function delay(min, max) { return new Promise(r => setTimeout(r, randomInt(min, max))); }

// ── Signal extraction (EP-aligned) ──
const SIGNALS = {
  pe_ownership:   { re: /private equity|pe[- ]backed|pe[- ]owned|acquired by|backed by|portfolio company|investment from|majority stake|buyout|venture capital|growth equity|investor group/i, pts: 25, label: 'PE ownership' },
  revenue_growth:  { re: /growth target|revenue growth|increase sales|expand pipeline|record (year|revenue|sales)|new deal|underperform|missed target|broker performance|sales target/i, pts: 15, label: 'Revenue/Growth' },
  org_change:      { re: /new head of sales|new sales director|new commercial director|appointed (sales|commercial|md|ceo)|hired (sales|commercial)|joins as (sales|commercial|md)|ceo transition|new md|new chief|leadership (change|appointment)|chief commercial officer|head of brokerage/i, pts: 15, label: 'Org change' },
  expansion:       { re: /new office|expanding (into|to)|opens in|launches (division|team|service)|new (asset class|service line|division)|enters? market|geographic expansion|international expansion|expands (into|to|presence)|new (yacht|aviation|property|art) (division|team|desk)/i, pts: 12, label: 'Expansion' },
  infra_invest:    { re: /crm|salesforce|hubspot|revops|marketing (director|hire|appointment)|head of (business development|marketing|growth)|sales enablement|data (investment|platform|infrastructure)|digital transformation|technology investment|sales (platform|tool|stack)|prop-?tech/i, pts: 10, label: 'Infra/CRM' },
  events:          { re: /mipim|monaco yacht show|ebace|frieze|art basel|top marques|goodwood|fort lauderdale (boat|yacht)|dubai air show|singapore yacht|art fair|auction season|mediterranean season/i, pts: 8, label: 'Event trigger' }
};

function extractSignals(text) {
  const found = [];
  for (const [key, cfg] of Object.entries(SIGNALS)) {
    if (cfg.re.test(text)) { found.push({ key, score: cfg.pts, label: cfg.label }); }
  }
  return found;
}

function fullScore(company, signals) {
  let score = 0, labels = [];
  for (const s of signals) { score += s.score; labels.push(s.label); }
  const emp = parseInt(String(company.employees || '0').replace(/[^0-9]/g, '')) || 0;
  if (emp >= 20 || String(company.employees || '').includes('+')) { score += 5; labels.push('Size fit'); }
  const vl = (company.vertical || '').toLowerCase();
  if (['superyacht','yacht','luxury real estate','property','private aviation','jet','charter','fine art','art advisory','auction','luxury automobile','supercar','classic car','luxury travel','expedition','high-end travel','bespoke travel'].some(v => vl.includes(v))) { score += 5; labels.push('Vertical fit'); }
  const cl = (company.hq_country || '').toLowerCase();
  if (['uk','united kingdom','us','united states','uae','dubai','monaco','switzerland','singapore'].some(g => cl.includes(g))) { score += 5; labels.push('Geo fit'); }
  score = Math.min(score, 100);
  let tier = 'LOW'; if (score >= 80) tier = 'HOT'; else if (score >= 50) tier = 'WARM';
  return { score, tier, triggers: labels.join('; ') };
}

// ── Browser search ──
async function searchSignals(browser, companyName) {
  const queries = [
    `"${companyName}" private equity OR acquired OR backed OR investment`,
    `"${companyName}" new sales director OR appointed OR hired OR new head`,
    `"${companyName}" expansion OR new office OR launches OR growing`,
  ];
  let allSignals = [];
  for (const query of queries) {
    const page = await browser.newPage();
    try {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto(`https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`, { waitUntil: 'domcontentloaded', timeout: 20000 });
      await delay(2000, 4000);
      const text = await page.evaluate(() => document.body?.innerText?.substring(0, 3000) || '');
      allSignals.push(...extractSignals(text));
      await page.close();
      await delay(8000, 15000);
    } catch { try { await page.close(); } catch {} await delay(5000, 10000); }
  }
  const seen = new Set();
  return allSignals.filter(s => { if (seen.has(s.key)) return false; seen.add(s.key); return true; });
}

async function scrapeSite(browser, url) {
  if (!url || url === 'Unknown') return '';
  const page = await browser.newPage();
  try {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`https://${url.replace(/^https?:\/\//, '')}`, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await delay(1500, 3000);
    const text = await page.evaluate(() => document.body?.innerText?.substring(0, 3000) || '');
    await page.close();
    return text;
  } catch { try { await page.close(); } catch {} return ''; }
}

// ── Main ──
async function main() {
  const checkpoint = loadJson(CHECKPOINT_FILE, { completed: [], lastRow: 0 });
  const progress = loadJson(PROGRESS_FILE, []);

  // Read Sheet ONCE at startup
  console.error(`[worker-${WORKER_ID}] Reading Sheet data...`);
  let allRows;
  try {
    const { execSync } = await import('child_process');
    const raw = execSync(`gog sheets get "${SHEET_ID}" "Sheet1!A2:K" --json`, { encoding: 'utf8', timeout: 60000 });
    allRows = JSON.parse(raw).values || [];
  } catch (err) {
    console.error(`[worker-${WORKER_ID}] FATAL: Cannot read Sheet: ${err.message}`);
    process.exit(1);
  }
  const rows = allRows.slice(START_ROW - 2, END_ROW - 1);
  console.error(`[worker-${WORKER_ID}] Rows ${START_ROW}-${END_ROW} | ${rows.length} companies | ${checkpoint.completed.length} already done`);

  let browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
  let queryCount = 0;
  let enriched = 0, errors = 0;

  for (let i = 0; i < rows.length; i++) {
    const rowNum = START_ROW + i;
    if (checkpoint.completed.includes(rowNum)) continue;
    const row = rows[i];
    if (!row || !row[0] || !row[0].trim()) continue;

    const [companyName, vertical, hqCity, hqCountry, website, employees, revenue, peOwned, dmName, dmTitle, notes] = row;

    console.error(`[worker-${WORKER_ID}] [${i + 1}/${rows.length}] ${companyName}`);

    // Browser rotation every 40 queries
    if (queryCount >= 40) {
      try { await browser.close(); } catch {}
      await delay(3000, 6000);
      browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
      queryCount = 0;
    }

    let companySignals = [];
    let peStatus = peOwned || 'Unknown';

    try {
      const found = await searchSignals(browser, companyName);
      companySignals.push(...found);
      queryCount += 3;
      if ((peStatus === 'Unknown' || peStatus === '') && found.some(s => s.key === 'pe_ownership')) peStatus = 'Yes (enriched)';

      if (website && website !== 'Unknown') {
        const siteText = await scrapeSite(browser, website);
        queryCount++;
        if (siteText) {
          companySignals.push(...extractSignals(siteText));
          if ((peStatus === 'Unknown' || peStatus === '') && /private equity|pe-backed|portfolio company/i.test(siteText)) peStatus = 'Yes (website)';
          else if ((peStatus === 'Unknown' || peStatus === '') && /family owned|independent|founder/i.test(siteText)) peStatus = 'No (independent)';
        }
      }

      const seenKeys = new Set();
      const unique = companySignals.filter(s => { if (seenKeys.has(s.key)) return false; seenKeys.add(s.key); return true; });
      const company = { company_name: companyName, vertical, hq_country: hqCountry, website, employees };
      const { score, tier, triggers } = fullScore(company, unique);

      console.error(`[worker-${WORKER_ID}]   ${score} [${tier}] — ${triggers}`);

      // Save progress (not Sheet — that's done separately)
      progress.push({ row: rowNum, name: companyName, score, tier, triggers, peStatus, signals: unique.map(s => s.key), vertical, hq_country: hqCountry, website });
      saveJson(PROGRESS_FILE, progress);

      // Only checkpoint AFTER successful save
      checkpoint.completed.push(rowNum);
      checkpoint.lastRow = rowNum;
      saveJson(CHECKPOINT_FILE, checkpoint);

      enriched++;
      await delay(3000, 6000);

    } catch (err) {
      console.error(`[worker-${WORKER_ID}]   ERROR: ${err.message}`);
      errors++;
      // Do NOT checkpoint failed rows — resume will retry
      try { await browser.close(); } catch {}
      await delay(5000, 10000);
      browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
      queryCount = 0;
    }
  }

  try { await browser.close(); } catch {}

  const hot = progress.filter(p => p.tier === 'HOT').length;
  const warm = progress.filter(p => p.tier === 'WARM').length;
  const low = progress.filter(p => p.tier === 'LOW').length;
  console.error(`\n[worker-${WORKER_ID}] ═══════════════════════════════`);
  console.error(`[worker-${WORKER_ID}] DONE — Enriched: ${enriched} | Errors: ${errors}`);
  console.error(`[worker-${WORKER_ID}] HOT: ${hot} | WARM: ${warm} | LOW: ${low}`);
  console.error(`[worker-${WORKER_ID}] ═══════════════════════════════`);
}

main().catch(err => { console.error(`[worker-${WORKER_ID}] Fatal: ${err.message}`); process.exit(1); });
