/**
 * Batch People Finder v2 — EP-aligned, crash-safe, no shell spawns during loop
 * 
 * For each company in Sheet1:
 *   1. Try common team/leadership page paths on their website
 *   2. Scrape structured people cards only
 *   3. Classify contacts by EP tier
 *   4. Detect person-level triggers (new hire signals)
 *   5. Save to local JSON per company (checkpoint)
 * 
 * Usage: node batch-people-finder.js [--resume]
 */

import { launch } from 'cloakbrowser';
import { randomInt } from 'crypto';
import { readFileSync, writeFileSync, existsSync } from 'fs';

const SHEET_ID = '1w8yEzOVUbn8Td4jqmTx9ezLfKU-D1ESynfqBcjE8gX4';
const CHECKPOINT_FILE = 'people-checkpoint.json';
const PROGRESS_FILE = 'people-progress.json';

function loadJson(path, fallback) {
  if (existsSync(path)) { try { return JSON.parse(readFileSync(path, 'utf8')); } catch {} }
  return fallback;
}
function saveJson(path, data) { writeFileSync(path, JSON.stringify(data, null, 2)); }
function delay(min, max) { return new Promise(r => setTimeout(r, randomInt(min, max))); }

// ── EP Tier classification ──
const TIER_RE = {
  primary: /\b(head of sales|sales director|commercial director|director of sales|director of brokerage|vp of sales|vice president of sales|chief revenue officer|cro|chief commercial officer|cco|head of commercial|sales manager|head of business development|director of business development|managing broker|head of brokerage)\b/i,
  secondary: /\b(managing director|chief executive officer|ceo|managing partner|founding partner|co-founder|president|group ceo|principal|owner|chairman)\b/i,
  tertiary: /\b(chief operating officer|coo|head of strategy|strategy director|director of operations|head of growth|svp business development|vp business development|head of partnerships|director of partnerships|head of marketing)\b/i,
  financeApprover: /\b(financial director|finance director|cfo|chief financial officer)\b/i,
  adjacentUser: /\b(yacht sales|sales & purchase|sales operations|broker|charter|head of charter|brokerage|business development|account|sales consultant)\b/i,
};

function classify(title = '') {
  if (TIER_RE.primary.test(title)) return { tier: 'Primary', rationale: 'Sales/commercial leadership per EP' };
  if (TIER_RE.secondary.test(title)) return { tier: 'Secondary', rationale: 'Founder/firm leadership per EP' };
  if (TIER_RE.tertiary.test(title)) return { tier: 'Tertiary', rationale: 'Ops/strategy/growth per EP' };
  if (TIER_RE.financeApprover.test(title)) return { tier: 'Approver', rationale: 'Budget approver' };
  if (TIER_RE.adjacentUser.test(title)) return { tier: 'Adjacent User', rationale: 'Potential end user / influencer' };
  return { tier: 'Not Target', rationale: 'Outside EP hierarchy' };
}

// ── Person-level trigger detection ──
const NEW_HIRE_RE = /\b(appointed|joined|newly appointed|named|hired|promoted to|takes role|takes helm|new (head|director|ceo|md|vp|chief)|succeeds|replaces|announced as)\b/i;

function detectPersonTrigger(name, title, pageText) {
  // Check if the surrounding context suggests a recent appointment
  const context = pageText.substring(0, 5000).toLowerCase();
  const nameLower = name.toLowerCase();
  
  // Look for appointment language near this person's name
  const nameIdx = context.indexOf(nameLower);
  if (nameIdx >= 0) {
    const surrounding = context.substring(Math.max(0, nameIdx - 200), nameIdx + 200);
    if (NEW_HIRE_RE.test(surrounding)) {
      return { trigger: 'New hire / appointment', detail: 'Detected appointment language near name — verify via LinkedIn' };
    }
  }
  
  // Check title itself for "new" or appointment indicators
  if (/new (head|director|ceo|md|vp|chief|sales|commercial)/i.test(title)) {
    return { trigger: 'New hire / appointment', detail: 'Title suggests recent appointment — verify via LinkedIn' };
  }
  
  return { trigger: '', detail: '' };
}

// ── Team page discovery ──
const TEAM_PATHS = [
  '/team', '/leadership', '/people', '/our-team', '/meet-the-team', '/management', '/board', '/directors',
  '/team/directors', '/team/management', '/team/sales', '/team/charter', '/team/yacht-sales', '/team/yacht-charter'
];

async function findTeamPages(browser, domain) {
  const results = [];
  const page = await browser.newPage();

  for (const path of TEAM_PATHS) {
    try {
      await page.goto(`https://${domain}${path}`, { waitUntil: 'domcontentloaded', timeout: 10000 });
      await delay(1000, 2000);
      const url = page.url();
      if (url.includes('404') || url.endsWith(domain + '/') || url.endsWith(domain)) continue;

      const people = await page.evaluate(() => {
        const NAME_RE = /^[A-Z][A-Za-z'’.-]+(?: [A-Z][A-Za-z'’.-]+){1,3}$/;
        const TITLE_HINT_RE = /(director|managing|chief|head|sales|commercial|broker|charter|business development|operations|marketing|finance|financial|founder|owner|partner|president|chairman|coo|ceo|cfo|cro|cco)/i;
        const BAD_NAME_RE = /^(The|Our|About|Team|People|Directors|Management|Leadership|Company|Contact|Learn|More|View|Read|Discover|Explore|Services|Charter|Sales|Purchase|Yacht|Superyacht|Brokerage|International|Global|Monaco|London|Fort Lauderdale)$/i;

        const candidates = [];
        const sels = [
          '[class*=person]', '[class*=member]', '[class*=profile]', '[class*=staff]', '[class*=leader]',
          '[class*=bio]', '[class*=employee]', '[class*=teammate]', '[data-testid*=member]', '[data-testid*=person]',
          'article', 'li', 'section', 'div'
        ];

        for (const el of document.querySelectorAll(sels.join(','))) {
          const name = el.querySelector('h1,h2,h3,h4,h5,[class*=name]')?.textContent?.trim() || '';
          const roleNode = el.querySelector('[class*=title],[class*=role],[class*=position],[class*=job],p,small,span');
          const title = roleNode?.textContent?.trim() || '';

          if (!name || !NAME_RE.test(name) || BAD_NAME_RE.test(name)) continue;
          if (!title || title.length < 3 || title.length > 140) continue;
          if (!TITLE_HINT_RE.test(title)) continue;

          candidates.push({ name, title: title.substring(0, 120) });
        }

        const seen = new Set();
        return candidates.filter(p => {
          const k = `${p.name}|${p.title}`;
          if (seen.has(k)) return false;
          seen.add(k);
          return true;
        });
      });

      if (people.length >= 2) results.push({ url, people });
    } catch {}
  }

  await page.close();
  return results;
}

// ── Main ──
async function main() {
  const resume = process.argv.includes('--resume');
  const checkpoint = loadJson(CHECKPOINT_FILE, { completed: [] });
  const allPeople = loadJson(PROGRESS_FILE, []);
  
  console.error(`[people] ${resume ? 'Resuming' : 'Starting'} — ${checkpoint.completed.length} already done, ${allPeople.length} people found so far`);
  
  // Read Sheet1 company data (one-time, before loop)
  let companies;
  try {
    const { execSync } = await import('child_process');
    const raw = execSync(`gog sheets get "${SHEET_ID}" "Sheet1!A2:K" --json`, { encoding: 'utf8', timeout: 60000 });
    companies = JSON.parse(raw).values || [];
  } catch (err) {
    console.error(`[people] FATAL: Cannot read Sheet: ${err.message}`);
    process.exit(1);
  }
  console.error(`[people] ${companies.length} companies to process`);
  
  let browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
  let queryCount = 0;
  let processed = 0, errors = 0;
  
  for (let i = 0; i < companies.length; i++) {
    const rowNum = i + 2;
    if (checkpoint.completed.includes(rowNum)) continue;
    
    const row = companies[i];
    if (!row || !row[0] || !row[0].trim()) continue;
    
    const [companyName, vertical, hqCity, hqCountry, website] = row;
    const domain = (website || '').replace(/^https?:\/\//, '').replace(/\/.*$/, '').toLowerCase();
    
    if (!domain || domain === 'unknown' || domain.length < 3) {
      checkpoint.completed.push(rowNum);
      saveJson(CHECKPOINT_FILE, checkpoint);
      continue;
    }
    
    console.error(`[people] [${i+1}/${companies.length}] ${companyName} (${domain})`);
    
    // Browser rotation
    if (queryCount >= 30) {
      try { await browser.close(); } catch {}
      await delay(3000, 6000);
      browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
      queryCount = 0;
    }
    
    let companyPeople = [];
    
    try {
      // 1. Try website team pages
      if (domain) {
        console.error(`[people]   Scraping ${domain} team pages...`);
        const teamPages = await findTeamPages(browser, domain);
        queryCount += teamPages.length;
        
        for (const tp of teamPages) {
          console.error(`[people]   Found ${tp.people.length} people on ${tp.url}`);
          for (const p of tp.people) {
            const { tier, rationale } = classify(p.title);
            const location = (p.title.split('|')[1] || '').trim();
            companyPeople.push({
              company: companyName,
              sheet_row: rowNum,
              person_name: p.name,
              job_title: p.title,
              location,
              ep_tier: tier,
              tier_rationale: rationale,
              person_trigger: '',
              trigger_detail: '',
              linkedin: '',
              source: tp.url,
              notes: '',
            });
          }
        }
      }
      
      // 2. Detect person-level triggers
      for (const p of companyPeople) {
        if (['Primary','Secondary','Tertiary'].includes(p.ep_tier)) {
          const { trigger, detail } = detectPersonTrigger(p.person_name, p.job_title, p.job_title);
          p.person_trigger = trigger;
          p.trigger_detail = detail;
        }
      }
      
      // 4. Deduplicate by name within company
      const seen = new Set();
      companyPeople = companyPeople.filter(p => {
        const key = `${p.person_name}|${p.company}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      
      // 5. Save
      for (const p of companyPeople) {
        allPeople.push(p);
      }
      saveJson(PROGRESS_FILE, allPeople);
      
      checkpoint.completed.push(rowNum);
      saveJson(CHECKPOINT_FILE, checkpoint);
      
      processed++;
      const targets = companyPeople.filter(p => ['Primary','Secondary','Tertiary'].includes(p.ep_tier));
      console.error(`[people]   ${companyPeople.length} people, ${targets.length} EP targets`);
      if (targets.length > 0) {
        console.error(`[people]   Targets: ${targets.map(t => `${t.person_name} [${t.ep_tier}]`).join(', ')}`);
      }
      
      await delay(3000, 6000);
      
    } catch (err) {
      console.error(`[people]   ERROR: ${err.message}`);
      errors++;
      // Don't checkpoint failed rows
      try { await browser.close(); } catch {}
      await delay(5000, 10000);
      browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
      queryCount = 0;
    }
  }
  
  try { await browser.close(); } catch {}
  
  // Summary
  const byTier = {};
  for (const p of allPeople) {
    byTier[p.ep_tier] = (byTier[p.ep_tier] || 0) + 1;
  }
  
  console.error(`\n[people] ═══════════════════════════════`);
  console.error(`[people] DONE — Processed: ${processed} | Errors: ${errors}`);
  console.error(`[people] Total people: ${allPeople.length}`);
  for (const [tier, count] of Object.entries(byTier).sort((a,b) => a[1] > b[1] ? -1 : 1)) {
    console.error(`[people]   ${tier}: ${count}`);
  }
  console.error(`[people] ═══════════════════════════════`);
  
  // Top targets
  const targets = allPeople.filter(p => ['Primary','Secondary','Tertiary'].includes(p.ep_tier));
  targets.sort((a, b) => {
    const order = { Primary: 1, Secondary: 2, Tertiary: 3 };
    return (order[a.ep_tier] || 99) - (order[b.ep_tier] || 99);
  });
  console.error(`\n=== TOP 20 EP TARGETS ===`);
  targets.slice(0, 20).forEach((p, idx) => {
    console.error(`${idx+1}. [${p.ep_tier}] ${p.person_name} — ${p.company} — ${p.job_title?.substring(0,60)}`);
  });
}

main().catch(err => { console.error(`[people] Fatal: ${err.message}`); process.exit(1); });
