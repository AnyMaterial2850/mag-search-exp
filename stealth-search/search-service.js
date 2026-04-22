/**
 * Stealth Search Service v2
 * 
 * Uses CloakBrowser (stealth Chromium) to search engines
 * with human-like behaviour, bypassing bot detection.
 * 
 * Output: JSON array of search results [{title, url, snippet, engine}]
 */

import { launch } from 'cloakbrowser';
import { randomInt } from 'crypto';

const ENGINES = {
  google: {
    buildUrl: (q) => `https://www.google.com/search?q=${encodeURIComponent(q)}&num=20`,
    name: 'Google'
  },
  bing: {
    buildUrl: (q) => `https://www.bing.com/search?q=${encodeURIComponent(q)}&count=20`,
    name: 'Bing'
  },
  duckduckgo: {
    buildUrl: (q) => `https://html.duckduckgo.com/html/?q=${encodeURIComponent(q)}`,
    name: 'DuckDuckGo'
  }
};

function parseArgs() {
  const args = process.argv.slice(2);
  let engine = 'bing';
  let maxResults = 10;
  const queryParts = [];
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--engine' && args[i + 1]) {
      engine = args[++i];
    } else if (args[i] === '--max-results' && args[i + 1]) {
      maxResults = parseInt(args[++i]);
    } else {
      queryParts.push(args[i]);
    }
  }
  
  const query = queryParts.join(' ');
  if (!query) {
    console.error('Usage: node search-service.js [--engine google|bing|duckduckgo] [--max-results N] "search query"');
    process.exit(1);
  }
  
  return { engine, maxResults, query };
}

function humanDelay(minMs = 2000, maxMs = 5000) {
  const delay = randomInt(minMs, maxMs);
  return new Promise(resolve => setTimeout(resolve, delay));
}

async function search(query, engine = 'bing', maxResults = 10) {
  const engineConfig = ENGINES[engine];
  if (!engineConfig) throw new Error(`Unknown engine: ${engine}`);

  console.error(`[stealth-search] Launching CloakBrowser...`);
  
  const browser = await launch({
    headless: true,
    humanize: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
    ]
  });

  try {
    const page = await browser.newPage();
    await page.setViewportSize({ width: 1440, height: 900 });
    
    const url = engineConfig.buildUrl(query);
    console.error(`[stealth-search] Navigating to ${engineConfig.name}: "${query}"`);
    
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await humanDelay(2000, 4000);
    
    // Check for CAPTCHA
    const currentUrl = page.url();
    const pageContent = await page.content();
    if (currentUrl.includes('sorry') || currentUrl.includes('challenge') || pageContent.includes('captcha') || pageContent.includes('CAPTCHA')) {
      console.error(`[stealth-search] CAPTCHA detected, waiting...`);
      await humanDelay(5000, 10000);
      // Re-check
      const newUrl = page.url();
      if (newUrl.includes('sorry') || newUrl.includes('challenge')) {
        console.error(`[stealth-search] Still blocked after waiting, trying Google fallback...`);
        if (engine !== 'google') {
          await browser.close();
          return search(query, 'google', maxResults);
        }
        return [];
      }
    }
    
    // Human-like scrolling
    await page.mouse.move(randomInt(200, 800), randomInt(200, 600));
    await humanDelay(500, 1500);
    await page.evaluate(() => window.scrollBy(0, 300));
    await humanDelay(1000, 2500);
    
    // Extract all links from the page with context
    const results = await page.evaluate((eng) => {
      const found = [];
      
      if (eng === 'bing') {
        // Bing results
        document.querySelectorAll('#b_results > li.b_algo').forEach(el => {
          const a = el.querySelector('h2 a');
          const p = el.querySelector('p');
          if (a) {
            found.push({
              title: a.textContent.trim(),
              url: a.href,
              snippet: p ? p.textContent.trim() : ''
            });
          }
        });
      } else if (eng === 'google') {
        // Google results
        document.querySelectorAll('#search .g, #rso .g').forEach(el => {
          const a = el.querySelector('a');
          const h3 = el.querySelector('h3');
          const spans = el.querySelectorAll('span');
          let snippet = '';
          spans.forEach(s => { if (s.textContent.length > 50 && !snippet) snippet = s.textContent.trim(); });
          if (a && h3 && !a.href.includes('google.com')) {
            found.push({
              title: h3.textContent.trim(),
              url: a.href,
              snippet: snippet
            });
          }
        });
      } else if (eng === 'duckduckgo') {
        // DDG HTML results
        document.querySelectorAll('.result').forEach(el => {
          const a = el.querySelector('.result__a');
          const snip = el.querySelector('.result__snippet');
          if (a) {
            found.push({
              title: a.textContent.trim(),
              url: a.href,
              snippet: snip ? snip.textContent.trim() : ''
            });
          }
        });
      }
      
      return found;
    }, engine);
    
    const trimmed = (Array.isArray(results) ? results : []).slice(0, maxResults).map(r => ({ ...r, engine }));
    
    console.error(`[stealth-search] Found ${trimmed.length} results from ${engineConfig.name}`);
    return trimmed;
  } finally {
    await browser.close();
  }
}

const { engine, maxResults, query } = parseArgs();
search(query, engine, maxResults)
  .then(results => console.log(JSON.stringify(results, null, 2)))
  .catch(err => {
    console.error(`[stealth-search] Fatal: ${err.message}`);
    console.log(JSON.stringify([]));
    process.exit(1);
  });
