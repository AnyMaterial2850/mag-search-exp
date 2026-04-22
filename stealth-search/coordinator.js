/**
 * Stealth Search Coordinator v2
 * 
 * - Round-robins Bing + DuckDuckGo with human delays
 * - Geo-aware: sets locale/language per query
 * - Result cleaning: drops junk domains, keeps company sites
 * - Outputs ranked JSON ready for Google Sheet upload
 * - Browser restart every 15 queries for fingerprint rotation
 */

import { launch } from 'cloakbrowser';
import { randomInt } from 'crypto';
import { readFileSync, writeFileSync, existsSync } from 'fs';

// ── Config ──
const ENGINES = ['bing', 'duckduckgo'];
const JUNK_DOMAINS = [
  'zhihu.com', 'baidu.com', 'weibo.com', 'douyin.com', 'taobao.com',
  'jd.com', 'bilibili.com', 'csdn.net', 'qq.com', 'sohu.com',
  'sina.com.cn', '163.com', 'toutiao.com', 'pinterest.com', 'tiktok.com',
  'youtube.com', 'reddit.com', 'facebook.com', 'twitter.com', 'instagram.com',
  'amazon.', 'ebay.', 'walmart.', 'aliexpress.', 'booking.com',
  'tripadvisor.', 'expedia.', 'airbnb.', 'wikipedia.org',
  'trustpilot.com', 'yelp.com', 'glassdoor.com', 'indeed.com',
  'g2.com', 'capterra.com', 'producthunt.com',
  'bing.com/ck/a', 'duckduckgo.com/l/'
];

const COMPANY_SIGNALS = [
  // Brokerage/dealer keywords in URL or title
  { pattern: /broker|brokerage|advis|dealer|consult|agency|group|international/i, score: 2 },
  { pattern: /yacht|aviation|jet|charter|realty|estate|property|art|collect|car|automobile|travel|expedition/i, score: 2 },
  { pattern: /private.office|luxury|ultra|prime|super|prime|uhnw|hnw/i, score: 1 },
  { pattern: /\.com$|\.co\.uk$|\.ch$|\.mc$|\.ae$|\.sg$/i, score: 0 }, // TLD signal, neutral
];

const GEO_LOCALES = {
  'monaco': { locale: 'en-GB', timezone: 'Europe/Monaco', lang: 'en' },
  'uk': { locale: 'en-GB', timezone: 'Europe/London', lang: 'en' },
  'us': { locale: 'en-US', timezone: 'America/New_York', lang: 'en' },
  'uae': { locale: 'en-AE', timezone: 'Asia/Dubai', lang: 'en' },
  'dubai': { locale: 'en-AE', timezone: 'Asia/Dubai', lang: 'en' },
  'switzerland': { locale: 'en-CH', timezone: 'Europe/Zurich', lang: 'en' },
  'singapore': { locale: 'en-SG', timezone: 'Asia/Singapore', lang: 'en' },
};

function detectGeo(query) {
  const q = query.toLowerCase();
  for (const [key, val] of Object.entries(GEO_LOCALES)) {
    if (q.includes(key)) return val;
  }
  return GEO_LOCALES['uk']; // Default
}

function humanDelay(minMs, maxMs) {
  return new Promise(resolve => setTimeout(resolve, randomInt(minMs, maxMs)));
}

function isJunkUrl(url) {
  return JUNK_DOMAINS.some(d => url.toLowerCase().includes(d));
}

function companySignalScore(title, url) {
  let score = 0;
  const text = `${title} ${url}`.toLowerCase();
  for (const sig of COMPANY_SIGNALS) {
    if (sig.pattern.test(text)) score += sig.score;
  }
  return score;
}

function cleanUrl(url) {
  // Unwrap Bing/DDG redirect URLs
  if (url.includes('bing.com/ck/a')) {
    const match = url.match(/u=a1([a-zA-Z0-9+\/=]+)/);
    if (match) {
      try { return Buffer.from(match[1], 'base64').toString('utf8'); } catch {}
    }
  }
  if (url.includes('duckduckgo.com/l/')) {
    const match = url.match(/uddg=([^&]+)/);
    if (match) return decodeURIComponent(match[1]);
  }
  return url;
}

function extractDomain(url) {
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

async function searchWithBrowser(page, query, engine, geo) {
  let url;
  if (engine === 'bing') {
    url = `https://www.bing.com/search?q=${encodeURIComponent(query)}&count=20&setlang=${geo.lang}&cc=${geo.locale.split('-')[1] || 'GB'}`;
  } else {
    url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}&kl=${geo.locale}`;
  }
  
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await humanDelay(2000, 4000);
  
  // Human scroll
  await page.mouse.move(randomInt(200, 800), randomInt(200, 600));
  await humanDelay(500, 1500);
  await page.evaluate(() => window.scrollBy(0, 300));
  await humanDelay(1000, 2500);
  
  // Extract
  const results = await page.evaluate((eng) => {
    const found = [];
    if (eng === 'bing') {
      document.querySelectorAll('#b_results > li.b_algo').forEach(el => {
        const a = el.querySelector('h2 a');
        const p = el.querySelector('p');
        if (a) found.push({ title: a.textContent.trim(), url: a.href, snippet: p ? p.textContent.trim() : '' });
      });
    } else {
      document.querySelectorAll('.result').forEach(el => {
        const a = el.querySelector('.result__a');
        const snip = el.querySelector('.result__snippet');
        if (a) found.push({ title: a.textContent.trim(), url: a.href, snippet: snip ? snip.textContent.trim() : '' });
      });
    }
    return found;
  }, engine);
  
  return Array.isArray(results) ? results : [];
}

async function main() {
  const queryFile = process.argv[2];
  const outputFile = process.argv[3] || 'results.json';
  
  if (!queryFile || !existsSync(queryFile)) {
    console.error('Usage: node coordinator.js <queries.txt> [output.json]');
    process.exit(1);
  }
  
  const queries = readFileSync(queryFile, 'utf8')
    .split('\n').map(q => q.trim()).filter(q => q.length > 0);
  
  console.error(`[coordinator] ${queries.length} queries`);
  
  let browser = await launch({
    headless: true, humanize: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });
  
  const allResults = [];
  let totalResults = 0;
  
  for (let i = 0; i < queries.length; i++) {
    const query = queries[i];
    const engine = ENGINES[i % ENGINES.length];
    const geo = detectGeo(query);
    
    console.error(`[coordinator] [${i + 1}/${queries.length}] ${engine}: "${query}" (geo: ${geo.locale})`);
    
    try {
      const page = await browser.newPage();
      await page.setViewportSize({ width: 1440 + randomInt(-50, 50), height: 900 + randomInt(-30, 30) });
      
      const rawResults = await searchWithBrowser(page, query, engine, geo);
      await page.close();
      
      // Clean and score
      for (const r of rawResults) {
        const cleanU = cleanUrl(r.url);
        if (isJunkUrl(cleanU) || isJunkUrl(r.url)) continue;
        
        const domain = extractDomain(cleanU);
        const signalScore = companySignalScore(r.title, cleanU);
        
        allResults.push({
          source_query: query,
          engine,
          title: r.title,
          url: cleanU,
          domain,
          snippet: r.snippet,
          signal_score: signalScore
        });
      }
      
      totalResults = allResults.length;
      console.error(`[coordinator]   → ${allResults.length} clean results (after junk removal)`);
      
      // Delay between queries
      const delay = randomInt(12000, 22000);
      console.error(`[coordinator]   Waiting ${Math.round(delay / 1000)}s...`);
      await humanDelay(delay, delay + 1000);
      
      // Browser restart every 12 queries
      if ((i + 1) % 12 === 0 && i < queries.length - 1) {
        console.error(`[coordinator] Restarting browser for fingerprint rotation...`);
        await browser.close();
        await humanDelay(3000, 6000);
        browser = await launch({
          headless: true, humanize: true,
          args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });
      }
    } catch (err) {
      console.error(`[coordinator]   ERROR: ${err.message}`);
      // Try to restart browser on error
      try { await browser.close(); } catch {}
      await humanDelay(5000, 10000);
      browser = await launch({
        headless: true, humanize: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
      });
    }
  }
  
  try { await browser.close(); } catch {}
  
  // Sort by signal score (highest first)
  allResults.sort((a, b) => b.signal_score - a.signal_score);
  
  // Deduplicate by domain
  const seen = new Set();
  const deduped = allResults.filter(r => {
    if (seen.has(r.domain)) return false;
    seen.add(r.domain);
    return true;
  });
  
  writeFileSync(outputFile, JSON.stringify(deduped, null, 2));
  console.error(`[coordinator] Done. ${deduped.length} unique company results → ${outputFile}`);
}

main().catch(err => {
  console.error(`[coordinator] Fatal: ${err.message}`);
  process.exit(1);
});
