'use strict';
/**
 * Vercel 서버리스 함수 — /api/search
 * ?q=삼성전자  →  Naver 자동완성으로 국내 종목 검색
 */
const https = require('https');

const _rl = new Map();
function checkRateLimit(ip) {
  const now = Date.now();
  let e = _rl.get(ip);
  if (!e || now >= e.resetAt) { e = { count: 0, resetAt: now + 60_000 }; _rl.set(ip, e); }
  e.count++;
  if (_rl.size > 2000) for (const [k, v] of _rl) if (now >= v.resetAt) _rl.delete(k);
  return e.count <= 60;
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer':    'https://finance.naver.com/',
        'Accept':     'application/json',
      },
      timeout: 5000,
    }, res => {
      const buf = [];
      res.on('data', c => buf.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(buf).toString() }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || 'unknown';
  if (!checkRateLimit(ip)) return res.status(429).json({ results: [] });

  const q = new URL(req.url, 'http://localhost').searchParams.get('q') || '';
  if (!q.trim()) return res.json({ results: [] });

  try {
    const r = await httpsGet(
      `https://ac.finance.naver.com/ac?q=${encodeURIComponent(q)}&q_enc=UTF-8&st=111&ssc=tab.ac.stock&ie=utf8&independent=true&cs=utf-8`
    );
    if (r.status !== 200) throw new Error('HTTP ' + r.status);
    const d = JSON.parse(r.body);
    // items[0]: [[이름, 코드, 타입, 시장], ...]
    const results = (d.items?.[0] || []).slice(0, 6).map(item => ({
      name:   item[0],
      code:   item[1],
      market: item[3] || '',
    }));
    return res.json({ results });
  } catch (e) {
    return res.status(503).json({ results: [], error: e.message });
  }
};
