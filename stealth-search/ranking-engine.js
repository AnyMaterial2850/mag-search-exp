/**
 * Intel IQ Ranking Engine
 * Aligned to the Engagement Pack (ICP) trigger framework
 * 
 * Scoring (max 100):
 *   PE Ownership Signal          25pts  (always-on qualifying signal per EP)
 *   Revenue Pain / Growth Signal 15pts  (growth targets, underperformance, broker pressure)
 *   Org Change Trigger           15pts  (new Sales Dir/CCO/MD hire in last 6 months)
 *   Expansion Trigger            12pts  (new geography, new asset class, new service line)
 *   Infrastructure Investment     10pts  (CRM investment, RevOps hire, marketing/BD hire)
 *   Event/Seasonal Trigger        8pts  (MIPIM, Monaco Yacht Show, EBACE, Art Basel etc.)
 *   Size Fit                       5pts  (20-500+ employees, £2M+ revenue)
 *   Vertical Fit                   5pts  (matches one of 6 target verticals)
 *   Geographic Fit                  5pts  (HQ in UK, US, UAE, Monaco, CH, SG)
 * 
 * Tiers:
 *   80-100: HOT — reach out now, multiple triggers active
 *   50-79:  WARM — strong ICP fit, monitoring for trigger activation
 *   0-49:   LOW — right vertical but no active signals
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';

const EP_TRIGGERS = {
  pe_signals: {
    keywords: ['private equity', 'pe-backed', 'pe-owned', 'backed by', 'acquired by', 'investment from', 'portfolio company'],
    weight: 25
  },
  revenue_pain: {
    keywords: ['growth targets', 'revenue growth', 'new deals', 'increase sales', 'expand pipeline', 'underperformance', 'missed targets', 'performance pressure'],
    weight: 15
  },
  org_change: {
    titles: ['new head of sales', 'new sales director', 'new commercial director', 'new md', 'new ceo', 'new cco', 'appointed sales', 'hired sales director', 'joins as sales', 'new brokerage team'],
    weight: 15
  },
  expansion: {
    keywords: ['new office', 'expanding into', 'new geography', 'new asset class', 'new service line', 'launches division', 'opens in', 'enters market'],
    weight: 12
  },
  infra_invest: {
    keywords: ['crm', 'salesforce', 'hubspot', 'revops', 'new marketing director', 'head of business development', 'new bd hire', 'data investment', 'sales enablement'],
    weight: 10
  },
  events: {
    keywords: ['mipim', 'monaco yacht show', 'ebace', 'frieze', 'art basel', 'top marques monaco', 'goodwood', 'auction season', 'mediterranean season', 'ski season'],
    weight: 8
  }
};

const TARGET_VERTICLES = [
  'superyacht', 'yacht', 'luxury real estate', 'property', 'private aviation', 'jet charter',
  'air charter', 'fine art', 'art advisory', 'auction', 'collectible', 'luxury automobile',
  'supercar', 'hypercar', 'classic car', 'collector car', 'luxury travel', 'expedition',
  'travel designer', 'bespoke travel', 'ultra-luxury travel'
];

const TARGET_GEOS = ['uk', 'united kingdom', 'us', 'united states', 'uae', 'dubai', 'abu dhabi',
  'monaco', 'switzerland', 'zurich', 'geneva', 'singapore'];

function scoreCompany(company) {
  let score = 0;
  let triggers = [];
  const text = `${company.title || ''} ${company.snippet || ''} ${company.notes || ''} ${company.website || ''}`.toLowerCase();
  const hq = (company.hq_country || '').toLowerCase();
  const vertical = (company.vertical || '').toLowerCase();

  // 1. PE ownership (25pts)
  if (company.pe_owned === 'yes' || company.pe_owned === 'Yes') {
    score += EP_TRIGGERS.pe_signals.weight;
    triggers.push('PE ownership (confirmed)');
  } else {
    for (const kw of EP_TRIGGERS.pe_signals.keywords) {
      if (text.includes(kw)) {
        score += EP_TRIGGERS.pe_signals.weight;
        triggers.push('PE signal: ' + kw);
        break;
      }
    }
  }

  // 2. Revenue pain (15pts)
  for (const kw of EP_TRIGGERS.revenue_pain.keywords) {
    if (text.includes(kw)) { score += EP_TRIGGERS.revenue_pain.weight; triggers.push('Revenue signal'); break; }
  }

  // 3. Org change (15pts)
  for (const kw of EP_TRIGGERS.org_change.titles) {
    if (text.includes(kw)) { score += EP_TRIGGERS.org_change.weight; triggers.push('Org change: ' + kw); break; }
  }

  // 4. Expansion (12pts)
  for (const kw of EP_TRIGGERS.expansion.keywords) {
    if (text.includes(kw)) { score += EP_TRIGGERS.expansion.weight; triggers.push('Expansion: ' + kw); break; }
  }

  // 5. Infra invest (10pts)
  for (const kw of EP_TRIGGERS.infra_invest.keywords) {
    if (text.includes(kw)) { score += EP_TRIGGERS.infra_invest.weight; triggers.push('Infra: ' + kw); break; }
  }

  // 6. Events (8pts)
  for (const kw of EP_TRIGGERS.events.keywords) {
    if (text.includes(kw)) { score += EP_TRIGGERS.events.weight; triggers.push('Event: ' + kw); break; }
  }

  // 7. Size fit (5pts)
  const empStr = String(company.employees || '0');
  const empMin = parseInt(empStr.replace(/[^0-9]/g, '')) || 0;
  if (empMin >= 20) { score += 5; triggers.push('Size fit'); }

  // 8. Vertical fit (5pts)
  if (TARGET_VERTICLES.some(v => vertical.includes(v) || text.includes(v))) {
    score += 5; triggers.push('Vertical fit');
  }

  // 9. Geo fit (5pts)
  if (TARGET_GEOS.some(g => hq.includes(g) || text.includes(g))) {
    score += 5; triggers.push('Geo fit');
  }

  score = Math.min(score, 100);
  let tier = 'LOW';
  if (score >= 80) tier = 'HOT';
  else if (score >= 50) tier = 'WARM';

  return { score, tier, triggers: triggers.join('; ') };
}

function rankCompanies(companies) {
  return companies.map(c => {
    const { score, tier, triggers } = scoreCompany(c);
    return { ...c, rank_score: score, rank_tier: tier, rank_triggers: triggers };
  }).sort((a, b) => b.rank_score - a.rank_score);
}

export { scoreCompany, rankCompanies };

// CLI mode
if (process.argv[1] && process.argv[2]) {
  const inputFile = process.argv[2];
  if (existsSync(inputFile)) {
    const companies = JSON.parse(readFileSync(inputFile, 'utf8'));
    const ranked = rankCompanies(companies);
    const outFile = inputFile.replace('.json', '-ranked.json');
    writeFileSync(outFile, JSON.stringify(ranked, null, 2));
    console.error(`Ranked ${ranked.length} companies → ${outFile}`);
    console.error(`HOT: ${ranked.filter(r => r.rank_tier === 'HOT').length} | WARM: ${ranked.filter(r => r.rank_tier === 'WARM').length} | LOW: ${ranked.filter(r => r.rank_tier === 'LOW').length}`);
    console.log(JSON.stringify(ranked, null, 2));
  }
}
