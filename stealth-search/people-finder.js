/**
 * People Finder — scrape team pages and classify contacts by EP tier.
 *
 * Usage:
 *   node people-finder.js yco y.co /team/directors /team/yacht-sales /team/yacht-charter /team/yacht-management
 */

import { launch } from 'cloakbrowser';

const [,, companyName='Company', domain='example.com', ...paths] = process.argv;
if (!paths.length) {
  console.error('Usage: node people-finder.js <companyName> <domain> <path1> [path2...]');
  process.exit(1);
}

const re = {
  primary: /\b(head of sales|sales director|commercial director|director of sales|director of brokerage|vp of sales|vice president of sales|chief revenue officer|cro|chief commercial officer|cco|head of commercial|sales manager|head of business development|director of business development|managing broker|head of brokerage)\b/i,
  secondary: /\b(managing director|chief executive officer|ceo|managing partner|founding partner|co-founder|president|group ceo|principal|owner|chairman)\b/i,
  tertiary: /\b(chief operating officer|coo|head of strategy|strategy director|director of operations|head of growth|svp business development|vp business development|head of partnerships|director of partnerships|head of marketing)\b/i,
  adjacentUser: /\b(yacht sales|sales & purchase|sales operations|broker|charter|head of charter|brokerage|business development)\b/i,
  financeApprover: /\b(financial director|finance director|cfo|chief financial officer)\b/i,
};

function classify(title='') {
  if (re.primary.test(title)) return { tier: 'Primary', rationale: 'Exact EP sales/commercial leadership match' };
  if (re.secondary.test(title)) return { tier: 'Secondary', rationale: 'Exact EP founder/firm leadership match' };
  if (re.tertiary.test(title)) return { tier: 'Tertiary', rationale: 'Exact EP ops/strategy/growth leadership match' };
  if (re.financeApprover.test(title)) return { tier: 'Committee / Approver', rationale: 'Likely budget approver, not primary EP outreach target' };
  if (re.adjacentUser.test(title)) return { tier: 'Adjacent User', rationale: 'Potential end user / influencer, not clear buying owner' };
  return { tier: 'Not Target', rationale: 'Not in EP target hierarchy' };
}

const browser = await launch({ headless: true, humanize: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
const page = await browser.newPage();
await page.setViewportSize({ width: 1440, height: 900 });

const all = [];
const seen = new Set();

for (const path of paths) {
  const url = `https://${domain}${path}`;
  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2500));

    const found = await page.evaluate(() => {
      const out = [];
      const selectors = [
        '[class*=team]', '[class*=person]', '[class*=member]', '[class*=card]', '[class*=profile]',
        '[data-testid*=team]', '[data-testid*=member]'
      ];
      document.querySelectorAll(selectors.join(',')).forEach(el => {
        const name = el.querySelector('h1,h2,h3,h4,h5,[class*=name]')?.textContent?.trim();
        const title = el.querySelector('[class*=title],[class*=role],[class*=position],p,small,span')?.textContent?.trim();
        if (name && name.length > 1 && name.length < 100) {
          out.push({ name, title: (title || '').substring(0, 120) });
        }
      });
      return out;
    });

    for (const p of found) {
      const key = `${p.name}|${p.title}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const location = (p.title.split('|')[1] || '').trim();
      const { tier, rationale } = classify(p.title);
      all.push({
        company: companyName,
        person_name: p.name,
        job_title: p.title,
        location,
        ep_tier: tier,
        tier_rationale: rationale,
        source: url,
      });
    }
  } catch (err) {
    console.error(`Failed ${url}: ${err.message}`);
  }
}

await browser.close();

all.sort((a, b) => {
  const order = { 'Primary': 1, 'Secondary': 2, 'Tertiary': 3, 'Committee / Approver': 4, 'Adjacent User': 5, 'Not Target': 6 };
  return (order[a.ep_tier] || 99) - (order[b.ep_tier] || 99) || a.person_name.localeCompare(b.person_name);
});

console.log(JSON.stringify(all, null, 2));
