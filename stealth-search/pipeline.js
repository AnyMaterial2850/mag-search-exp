/**
 * Full Pipeline: Search → Clean → Rank → Google Sheet
 * 
 * Usage: node pipeline.js
 * 
 * Reads queries from queries.txt, runs stealth search,
 * ranks results against EP framework, pushes to Sheet.
 */

import { launch } from 'cloakbrowser';
import { randomInt } from 'crypto';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { execSync } from 'child_process';

const SHEET_ID = '1w8yEzOVUbn8Td4jqmTx9ezLfKU-D1ESynfqBcjE8gX4';
const ENGINES = ['bing', 'duckduckgo'];

// ── Ranking (inline for pipeline) ──
function scoreCompany(c) {
  let score = 0, triggers = [];
  const t = `${c.title||''} ${c.snippet||''} ${c.notes||''} ${c.website||''}`.toLowerCase();
  const hq = (c.hq_country||'').toLowerCase();
  const v = (c.vertical||'').toLowerCase();
  
  if (c.pe_owned==='yes'||c.pe_owned==='Yes') { score+=25; triggers.push('PE confirmed'); }
  else if (['private equity','pe-backed','acquired by','backed by','portfolio company'].some(k=>t.includes(k))) { score+=25; triggers.push('PE signal'); }
  
  if (['growth targets','revenue growth','expand pipeline','new deals','increase sales'].some(k=>t.includes(k))) { score+=15; triggers.push('Revenue signal'); }
  if (['new head of sales','new sales director','new commercial','new md','appointed','hired','joins as'].some(k=>t.includes(k))) { score+=15; triggers.push('Org change'); }
  if (['new office','expanding','opens in','launches','enters market','new service'].some(k=>t.includes(k))) { score+=12; triggers.push('Expansion'); }
  if (['crm','salesforce','hubspot','revops','marketing director','bd hire'].some(k=>t.includes(k))) { score+=10; triggers.push('Infra invest'); }
  if (['mipim','monaco yacht show','ebace','frieze','art basel','top marques','goodwood'].some(k=>t.includes(k))) { score+=8; triggers.push('Event trigger'); }
  
  const emp = parseInt(String(c.employees||'0').replace(/[^0-9]/g,''))||0;
  if (emp>=20) { score+=5; triggers.push('Size fit'); }
  
  if (['superyacht','yacht','real estate','property','aviation','jet','charter','art','collect','car','automobile','travel','expedition'].some(k=>v.includes(k)||t.includes(k))) { score+=5; triggers.push('Vertical fit'); }
  if (['uk','united kingdom','us','united states','uae','dubai','monaco','switzerland','singapore'].some(k=>hq.includes(k))) { score+=5; triggers.push('Geo fit'); }
  
  score = Math.min(score, 100);
  let tier = 'LOW'; if (score>=80) tier='HOT'; else if (score>=50) tier='WARM';
  return { score, tier, triggers: triggers.join('; ') };
}

// ── Search helpers ──
const JUNK = ['zhihu.com','baidu.com','csdn.net','qq.com','weibo.com','douyin.com','jd.com','bilibili.com',
  'pinterest.com','tiktok.com','youtube.com','reddit.com','facebook.com','twitter.com','instagram.com',
  'amazon.','ebay.','walmart.','booking.com','tripadvisor.','airbnb.','wikipedia.org',
  'trustpilot.com','yelp.com','glassdoor.com','indeed.com','g2.com','capterra.com'];

function isJunk(u) { return JUNK.some(d => u.toLowerCase().includes(d)); }

function cleanUrl(u) {
  if (u.includes('bing.com/ck/a')) { const m=u.match(/u=a1([a-zA-Z0-9+/=]+)/); if(m) try{return Buffer.from(m[1],'base64').toString('utf8')}catch{} }
  if (u.includes('duckduckgo.com/l/')) { const m=u.match(/uddg=([^&]+)/); if(m) return decodeURIComponent(m[1]); }
  return u;
}

function getDomain(u) { try{return new URL(u).hostname.replace(/^www\./,'')}catch{return '';} }

function detectGeo(q) {
  const ql = q.toLowerCase();
  if (ql.includes('monaco')) return {locale:'en-MC',lang:'en'};
  if (ql.includes('uae')||ql.includes('dubai')) return {locale:'en-AE',lang:'en'};
  if (ql.includes('singapore')||ql.includes('sg')) return {locale:'en-SG',lang:'en'};
  if (ql.includes('switzerland')||ql.includes('zurich')||ql.includes('geneva')) return {locale:'en-CH',lang:'en'};
  if (ql.includes('us')||ql.includes('usa')) return {locale:'en-US',lang:'en'};
  return {locale:'en-GB',lang:'en'};
}

function delay(min,max) { return new Promise(r=>setTimeout(r,randomInt(min,max))); }

async function searchPage(page, query, engine, geo) {
  const url = engine==='bing'
    ? `https://www.bing.com/search?q=${encodeURIComponent(query)}&count=20&setlang=${geo.lang}`
    : `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}&kl=${geo.locale}`;
  
  await page.goto(url, {waitUntil:'domcontentloaded',timeout:30000});
  await delay(2000,4000);
  await page.mouse.move(randomInt(200,800),randomInt(200,600));
  await delay(500,1500);
  await page.evaluate(()=>window.scrollBy(0,300));
  await delay(1000,2500);
  
  return page.evaluate((eng) => {
    const f = [];
    if (eng==='bing') {
      document.querySelectorAll('#b_results > li.b_algo').forEach(el => {
        const a=el.querySelector('h2 a'), p=el.querySelector('p');
        if(a) f.push({title:a.textContent.trim(),url:a.href,snippet:p?p.textContent.trim():''});
      });
    } else {
      document.querySelectorAll('.result').forEach(el => {
        const a=el.querySelector('.result__a'), s=el.querySelector('.result__snippet');
        if(a) f.push({title:a.textContent.trim(),url:a.href,snippet:s?s.textContent.trim():''});
      });
    }
    return f;
  }, engine);
}

// ── Vertical detector ──
function detectVertical(title, snippet) {
  const t = `${title} ${snippet}`.toLowerCase();
  if (/yacht|superyacht|megayacht/.test(t)) return 'Superyacht';
  if (/real estate|property|prime|luxury home|private office/.test(t)) return 'Luxury Real Estate';
  if (/aviation|jet|charter|private fly|air charter/.test(t)) return 'Private Aviation';
  if (/art|collectible|auction|gallery|advisory/.test(t)) return 'Fine Art';
  if (/car|supercar|hypercar|classic|automobile|motor/.test(t)) return 'Luxury Autos';
  if (/travel|expedition|bespoke|luxury holiday|safari/.test(t)) return 'High-end Travel';
  return 'Unknown';
}

function detectGeoFromText(title, snippet) {
  const t = `${title} ${snippet}`.toLowerCase();
  if (/monaco/.test(t)) return 'Monaco';
  if (/dubai|uae/.test(t)) return 'UAE';
  if (/singapore/.test(t)) return 'Singapore';
  if (/switzerland|zurich|geneva|swiss/.test(t)) return 'Switzerland';
  if (/united states|usa|florida|new york|california|palm beach/.test(t)) return 'US';
  return 'UK';
}

// ── Main pipeline ──
async function main() {
  const queryFile = process.argv[2] || 'queries.txt';
  if (!existsSync(queryFile)) { console.error('No queries file'); process.exit(1); }
  
  const queries = readFileSync(queryFile,'utf8').split('\n').map(q=>q.trim()).filter(q=>q);
  console.error(`[pipeline] ${queries.length} queries to process`);
  
  let browser = await launch({headless:true,humanize:true,args:['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage']});
  
  const allCompanies = [];
  const seenDomains = new Set();
  
  for (let i=0; i<queries.length; i++) {
    const query = queries[i];
    const engine = ENGINES[i%ENGINES.length];
    const geo = detectGeo(query);
    
    console.error(`[pipeline] [${i+1}/${queries.length}] ${engine}: "${query}"`);
    
    try {
      const page = await browser.newPage();
      await page.setViewportSize({width:1440+randomInt(-50,50),height:900+randomInt(-30,30)});
      
      const raw = await searchPage(page, query, engine, geo);
      await page.close();
      
      for (const r of raw) {
        const url = cleanUrl(r.url);
        const domain = getDomain(url);
        if (isJunk(url) || isJunk(r.url) || !domain || seenDomains.has(domain)) continue;
        seenDomains.add(domain);
        
        const vertical = detectVertical(r.title, r.snippet);
        const hqCountry = detectGeoFromText(r.title, r.snippet);
        
        allCompanies.push({
          company_name: r.title.replace(/[|[\]{}]/g,'').trim().substring(0,100),
          vertical,
          hq_city: '',
          hq_country: hqCountry,
          website: domain,
          employees: 'Unknown',
          revenue_gbp: 'Unknown',
          pe_owned: 'Unknown',
          decision_maker_name: '',
          decision_maker_title: '',
          notes: r.snippet.substring(0,200),
          source_query: query,
          source_engine: engine
        });
      }
      
      console.error(`[pipeline]   → ${allCompanies.length} unique companies so far`);
      
      const d = randomInt(12000,22000);
      console.error(`[pipeline]   Waiting ${Math.round(d/1000)}s...`);
      await delay(d, d+1000);
      
      // Browser restart every 12 queries
      if ((i+1)%12===0 && i<queries.length-1) {
        console.error(`[pipeline] Browser rotation...`);
        try{await browser.close()}catch{}
        await delay(3000,6000);
        browser = await launch({headless:true,humanize:true,args:['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage']});
      }
    } catch (err) {
      console.error(`[pipeline]   ERROR: ${err.message}`);
      try{await browser.close()}catch{}
      await delay(5000,10000);
      browser = await launch({headless:true,humanize:true,args:['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage']});
    }
  }
  
  try{await browser.close()}catch{}
  
  // ── Rank ──
  const ranked = allCompanies.map(c => {
    const {score, tier, triggers} = scoreCompany(c);
    return {...c, rank_score: score, rank_tier: tier, rank_triggers: triggers};
  }).sort((a,b) => b.rank_score - a.rank_score);
  
  // Save JSON
  writeFileSync('pipeline-results-ranked.json', JSON.stringify(ranked, null, 2));
  console.error(`\n[pipeline] Ranked ${ranked.length} companies`);
  console.error(`  HOT:  ${ranked.filter(r=>r.rank_tier==='HOT').length}`);
  console.error(`  WARM: ${ranked.filter(r=>r.rank_tier==='WARM').length}`);
  console.error(`  LOW:  ${ranked.filter(r=>r.rank_tier==='LOW').length}`);
  
  // ── Push to Google Sheet ──
  console.error(`[pipeline] Pushing to Google Sheet...`);
  const SHEET_TAB = 'Stealth Results';
  
  // Create tab header
  try {
    execSync(`gog sheets update "${SHEET_ID}" "'${SHEET_TAB}'!A1:R1" --values-json '[["Company Name","Vertical","HQ City","HQ Country","Website","Employees","Revenue","PE Owned","Decision Maker","Title","Notes","Source Query","Engine","Rank Score","Rank Tier","Rank Triggers"]]' --input USER_ENTERED`, {stdio:'pipe'});
  } catch(e) {
    // Tab might not exist yet, try append instead
    console.error(`[pipeline] Creating new tab via append...`);
  }
  
  // Batch write in chunks of 10
  const rows = ranked.map(c => [
    c.company_name, c.vertical, c.hq_city, c.hq_country, c.website,
    c.employees, c.revenue_gbp, c.pe_owned, c.decision_maker_name, c.decision_maker_title,
    c.notes, c.source_query, c.source_engine, c.rank_score, c.rank_tier, c.rank_triggers
  ]);
  
  for (let i=0; i<rows.length; i+=10) {
    const chunk = rows.slice(i, i+10);
    const startRow = i + 2; // Row 1 is header
    const endRow = startRow + chunk.length - 1;
    try {
      const jsonStr = JSON.stringify([chunk]);
      execSync(`gog sheets append "${SHEET_ID}" "'${SHEET_TAB}'!A:P" --values-json '${jsonStr.replace(/'/g, "'\\''")}' --insert INSERT_ROWS`, {stdio:'pipe'});
    } catch(e) {
      // Fallback: try without tab name
      try {
        const jsonStr = JSON.stringify([chunk]);
        execSync(`gog sheets append "${SHEET_ID}" "Sheet1!A:P" --values-json '${jsonStr.replace(/'/g, "'\\''")}' --insert INSERT_ROWS`, {stdio:'pipe'});
      } catch(e2) {
        console.error(`[pipeline] Sheet write error at row ${i}: ${e2.message}`);
      }
    }
    await delay(500, 1000);
  }
  
  console.error(`[pipeline] ✅ Done. ${ranked.length} ranked companies pushed to Sheet.`);
  
  // Print top 10
  console.error(`\n=== TOP 10 COMPANIES ===`);
  ranked.slice(0,10).forEach((c,i) => {
    console.error(`${i+1}. [${c.rank_tier}] ${c.rank_score}pts — ${c.company_name} (${c.vertical}, ${c.hq_country}) — ${c.rank_triggers}`);
  });
}

main().catch(err => { console.error(`Fatal: ${err.message}`); process.exit(1); });
