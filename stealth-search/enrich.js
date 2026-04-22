/**
 * Enrichment Pipeline — crash-safe, resumable
 * 
 * For each company: search for trigger signals, scrape website, update Sheet.
 * Saves progress after every company. Can resume from checkpoint.
 * 
 * EP-aligned trigger search:
 *   PE ownership (25), Revenue/Growth (15), Org Change (15),
 *   Expansion (12), Infra/CRM (10), Events (8)
 * 
 * Usage: node enrich.js [--resume]
 */

import { launch } from 'cloakbrowser';
import { randomInt } from 'crypto';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { execSync } from 'child_process';

const SHEET_ID = '1w8yEzOVUbn8Td4jqmTx9ezLfKU-D1ESynfqBcjE8gX4';
const CHECKPOINT_FILE = 'enrich-checkpoint.json';
const PROGRESS_FILE = 'enrich-progress.json';

// ── Crash-safe helpers ──
function loadCheckpoint() {
  if (existsSync(CHECKPOINT_FILE)) {
    try { return JSON.parse(readFileSync(CHECKPOINT_FILE, 'utf8')); } catch {}
  }
  return { completed: [], lastRow: 0 };
}

function saveCheckpoint(cp) {
  writeFileSync(CHECKPOINT_FILE, JSON.stringify(cp, null, 2));
}

function loadProgress() {
  if (existsSync(PROGRESS_FILE)) {
    try { return JSON.parse(readFileSync(PROGRESS_FILE, 'utf8')); } catch {}
  }
  return [];
}

function saveProgress(progress) {
  writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ── Signal extraction ──
const SIGNAL_PATTERNS = {
  pe_ownership: {
    patterns: [/private equity/i, /pe[- ]backed/i, /pe[- ]owned/i, /acquired by/i, /backed by/i, /portfolio company/i, /investment from/i, /majority stake/i, /buyout/i, /venture capital/i, /growth equity/i],
    score: 25, label: 'PE ownership'
  },
  revenue_growth: {
    patterns: [/growth target/i, /revenue growth/i, /increase sales/i, /expand pipeline/i, /record (year|revenue|sales)/i, /new deal/i, /underperform/i, /missed target/i, /broker performance/i, /sales target/i],
    score: 15, label: 'Revenue/Growth signal'
  },
  org_change: {
    patterns: [/new head of sales/i, /new sales director/i, /new commercial director/i, /appointed (sales|commercial|md|ceo)/i, /hired (sales|commercial)/i, /joins as (sales|commercial|md)/i, /ceo transition/i, /new md/i, /new chief/i, /leadership (change|appointment)/i, /promoted to (sales|commercial)/i],
    score: 15, label: 'Org change'
  },
  expansion: {
    patterns: [/new office/i, /expanding (into|to)/i, /opens in/i, /launches (division|team|service)/i, /new (asset class|service line|division)/i, /enters? market/i, /geographic expansion/i, /international expansion/i],
    score: 12, label: 'Expansion'
  },
  infra_invest: {
    patterns: [/crm/i, /salesforce/i, /hubspot/i, /revops/i, /marketing (director|hire|appointment)/i, /head of (business development|marketing|growth)/i, /sales enablement/i, /data (investment|platform|infrastructure)/i, /digital transformation/i, /technology investment/i, /sales (platform|tool|stack)/i],
    score: 10, label: 'Infra/CRM invest'
  },
  events: {
    patterns: [/mipim/i, /monaco yacht show/i, /ebace/i, /frieze/i, /art basel/i, /top marques/i, /goodwood/i, /fort lauderdale (boat|yacht)/i, /dubai air show/i, /singapore yacht/i, /art fair/i, /auction season/i, /mediterranean season/i],
    score: 8, label: 'Event trigger'
  }
};

function extractSignals(text) {
  const found = [];
  for (const [key, config] of Object.entries(SIGNAL_PATTERNS)) {
    for (const pattern of config.patterns) {
      if (pattern.test(text)) {
        found.push({ key, score: config.score, label: config.label, match: pattern.source });
        break; // One match per signal type
      }
    }
  }
  return found;
}

// ── Scoring ──
function fullScore(company, signals) {
  let score = 0;
  let triggerLabels = [];
  
  // Intent signals from enrichment
  for (const s of signals) {
    score += s.score;
    triggerLabels.push(s.label);
  }
  
  // Fit signals (from company data)
  const t = `${company.company_name||''} ${company.vertical||''} ${company.hq_country||''} ${company.website||''}`.toLowerCase();
  const emp = parseInt(String(company.employees||'0').replace(/[^0-9]/g,''))||0;
  if (emp >= 20 || String(company.employees||'').includes('+')) { score += 5; triggerLabels.push('Size fit'); }
  
  const vl = (company.vertical||'').toLowerCase();
  if (['superyacht','yacht','luxury real estate','property','private aviation','jet','charter','fine art','art advisory','auction','luxury automobile','supercar','classic car','luxury travel','expedition','high-end travel','bespoke travel'].some(v => vl.includes(v))) {
    score += 5; triggerLabels.push('Vertical fit');
  }
  
  const cl = (company.hq_country||'').toLowerCase();
  if (['uk','united kingdom','us','united states','uae','dubai','monaco','switzerland','singapore'].some(g => cl.includes(g))) {
    score += 5; triggerLabels.push('Geo fit');
  }
  
  score = Math.min(score, 100);
  let tier = 'LOW'; if (score >= 80) tier = 'HOT'; else if (score >= 50) tier = 'WARM';
  return { score, tier, triggers: triggerLabels.join('; ') };
}

// ── Browser search ──
function delay(min, max) { return new Promise(r => setTimeout(r, randomInt(min, max))); }

async function searchSignals(browser, companyName) {
  const queries = [
    `"${companyName}" private equity OR acquired OR backed OR investment`,
    `"${companyName}" new sales director OR appointed OR hired OR new head`,
    `"${companyName}" expansion OR new office OR launches OR growing`,
  ];
  
  let allText = '';
  let allSignals = [];
  
  for (const query of queries) {
    const page = await browser.newPage();
    try {
      await page.setViewportSize({ width: 1440, height: 900 });
      const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20000 });
      await delay(2000, 4000);
      
      const text = await page.evaluate(() => document.body?.innerText?.substring(0, 3000) || '');
      allText += ' ' + text;
      const signals = extractSignals(text);
      allSignals.push(...signals);
      
      await page.close();
      await delay(8000, 15000); // Respectful delay between queries
    } catch (err) {
      try { await page.close(); } catch {}
      // Don't crash — just skip this query
      await delay(5000, 10000);
    }
  }
  
  // Deduplicate signals by key
  const seen = new Set();
  const unique = allSignals.filter(s => {
    if (seen.has(s.key)) return false;
    seen.add(s.key);
    return true;
  });
  
  return { signals: unique, rawText: allText.substring(0, 500) };
}

async function scrapeCompanySite(browser, url) {
  if (!url || url === 'Unknown') return '';
  const page = await browser.newPage();
  try {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`https://${url.replace(/^https?:\/\//, '')}`, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await delay(1500, 3000);
    const text = await page.evaluate(() => document.body?.innerText?.substring(0, 3000) || '');
    await page.close();
    return text;
  } catch {
    try { await page.close(); } catch {}
    return '';
  }
}

// ── Sheet read/write ──
function readSheet() {
  try {
    const raw = execSync(`gog sheets get "${SHEET_ID}" "Sheet1!A2:K" --json`, { encoding: 'utf8', timeout: 30000 });
    return JSON.parse(raw).values || [];
  } catch (err) {
    console.error(`[enrich] Sheet read error: ${err.message}`);
    return [];
  }
}

function writeSheetRow(rowNum, score, tier, triggers, peOwned, notes) {
  try {
    const vals = JSON.stringify([[score, tier, triggers, peOwned, notes]]);
    execSync(`gog sheets update "${SHEET_ID}" "L${rowNum}:P${rowNum}" --values-json '${vals.replace(/'/g, "'\\''")}' --input USER_ENTERED`, { encoding: 'utf8', timeout: 15000 });
  } catch (err) {
    console.error(`[enrich] Sheet write error row ${rowNum}: ${err.message}`);
  }
}

// ── Main ──
async function main() {
  const resume = process.argv.includes('--resume');
  const checkpoint = resume ? loadCheckpoint() : { completed: [], lastRow: 0 };
  const progress = resume ? loadProgress() : [];
  
  console.error(`[enrich] ${resume ? 'Resuming' : 'Starting'} enrichment pipeline`);
  console.error(`[enrich] ${checkpoint.completed.length} companies already completed`);
  
  // Read companies from Sheet
  const rows = readSheet();
  console.error(`[enrich] ${rows.length} companies in Sheet`);
  
  let browser;
  let browserQueryCount = 0;
  
  async function getBrowser() {
    if (browser) {
      try { await browser.close(); } catch {}
    }
    console.error(`[enrich] Launching CloakBrowser...`);
    browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
    browserQueryCount = 0;
    return browser;
  }
  
  browser = await getBrowser();
  
  let enriched = 0;
  let errors = 0;
  
  for (let i = 0; i < rows.length; i++) {
    const rowNum = i + 2; // 1-indexed, header is row 1
    const row = rows[i];
    
    // Skip already completed
    if (checkpoint.completed.includes(rowNum)) continue;
    
    const [companyName, vertical, hqCity, hqCountry, website, employees, revenue, peOwned, dmName, dmTitle, notes] = row;
    if (!companyName || companyName.trim() === '') continue;
    
    console.error(`\n[enrich] [${i+1}/${rows.length}] ${companyName} (${vertical}, ${hqCountry})`);
    
    // Restart browser every 40 queries for fingerprint rotation
    if (browserQueryCount >= 40) {
      console.error(`[enrich] Browser rotation after ${browserQueryCount} queries`);
      browser = await getBrowser();
    }
    
    let companySignals = [];
    let peStatus = peOwned || 'Unknown';
    let enrichedNotes = notes || '';
    
    try {
      // 1. Search for trigger signals
      console.error(`[enrich]   Searching for signals...`);
      const { signals: foundSignals, rawText } = await searchSignals(browser, companyName);
      companySignals.push(...foundSignals);
      browserQueryCount += 3;
      
      // Check PE status from search results
      if (peStatus === 'Unknown' || peStatus === '') {
        if (foundSignals.some(s => s.key === 'pe_ownership')) {
          peStatus = 'Yes (enriched)';
        }
      }
      
      // 2. Scrape company website for additional signals
      if (website && website !== 'Unknown') {
        console.error(`[enrich]   Scraping ${website}...`);
        const siteText = await scrapeCompanySite(browser, website);
        browserQueryCount++;
        
        if (siteText) {
          const siteSignals = extractSignals(siteText);
          companySignals.push(...siteSignals);
          
          // Check PE from website
          if (peStatus === 'Unknown' || peStatus === '') {
            if (/private equity|pe-backed|portfolio company/i.test(siteText)) {
              peStatus = 'Yes (from website)';
            } else if (/family owned|independent|founder/i.test(siteText) && !/private equity|pe-backed/i.test(siteText)) {
              peStatus = 'No (independent)';
            }
          }
        }
      }
      
      // Deduplicate signals
      const seenKeys = new Set();
      const uniqueSignals = companySignals.filter(s => {
        if (seenKeys.has(s.key)) return false;
        seenKeys.add(s.key);
        return true;
      });
      
      // 3. Calculate full score
      const company = { company_name: companyName, vertical, hq_country: hqCountry, website, employees };
      const { score, tier, triggers } = fullScore(company, uniqueSignals);
      
      console.error(`[enrich]   Score: ${score} [${tier}] — ${triggers}`);
      if (uniqueSignals.length > 0) {
        console.error(`[enrich]   Signals: ${uniqueSignals.map(s => s.label + ' (+' + s.score + ')').join(', ')}`);
      }
      
      // 4. Write to Sheet
      writeSheetRow(rowNum, score, tier, triggers, peStatus, enrichedNotes);
      
      // 5. Save checkpoint
      checkpoint.completed.push(rowNum);
      checkpoint.lastRow = rowNum;
      saveCheckpoint(checkpoint);
      
      // 6. Save progress
      progress.push({ row: rowNum, name: companyName, score, tier, triggers, peStatus, signals: uniqueSignals.map(s => s.key) });
      saveProgress(progress);
      
      enriched++;
      
      // Delay between companies
      await delay(3000, 6000);
      
    } catch (err) {
      console.error(`[enrich]   ERROR: ${err.message}`);
      errors++;
      
      // Save checkpoint even on error
      checkpoint.completed.push(rowNum);
      checkpoint.lastRow = rowNum;
      saveCheckpoint(checkpoint);
      
      // Restart browser on error
      try { await browser.close(); } catch {}
      await delay(5000, 10000);
      browser = await getBrowser();
    }
  }
  
  try { await browser.close(); } catch {}
  
  // Summary
  const hot = progress.filter(p => p.tier === 'HOT').length;
  const warm = progress.filter(p => p.tier === 'WARM').length;
  const low = progress.filter(p => p.tier === 'LOW').length;
  
  console.error(`\n[enrich] ═══════════════════════════════`);
  console.error(`[enrich] ENRICHMENT COMPLETE`);
  console.error(`[enrich] Enriched: ${enriched} | Errors: ${errors}`);
  console.error(`[enrich] HOT: ${hot} | WARM: ${warm} | LOW: ${low}`);
  console.error(`[enrich] Progress saved to ${PROGRESS_FILE}`);
  console.error(`[enrich] ═══════════════════════════════`);
  
  // Print top companies
  const sorted = [...progress].sort((a, b) => b.score - a.score);
  console.error(`\n=== TOP 15 COMPANIES ===`);
  sorted.slice(0, 15).forEach((p, i) => {
    console.error(`${i+1}. [${p.tier}] ${p.score}pts — ${p.name} — ${p.triggers}`);
  });
}

main().catch(err => {
  console.error(`[enrich] Fatal: ${err.message}`);
  process.exit(1);
});
