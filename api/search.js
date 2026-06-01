'use strict';
/**
 * Vercel 서버리스 함수 — /api/search
 * ?q=삼성전자  →  2단계 검색으로 종목명+코드 반환
 *
 * 1단계: ac.search.naver.com 으로 자동완성 텍스트 목록 조회
 * 2단계: 각 후보에 " 주가" 붙여 병렬 조회 → answer 필드에서 종목코드 추출
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
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' },
      timeout: 4000,
    }, res => {
      const buf = [];
      res.on('data', c => buf.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(buf).toString() }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

const AC_BASE = 'https://ac.search.naver.com/nx/ac?q_enc=UTF-8&st=11&frm=nv&r_format=json&r_enc=UTF-8&r_unicode=0&t_koreng=1&ans=2&run=2&rev=4&rq_m=co&nw=1';

async function acSearch(q) {
  const r = await httpsGet(`${AC_BASE}&q=${encodeURIComponent(q)}`);
  if (r.status !== 200) throw new Error('AC HTTP ' + r.status);
  return JSON.parse(r.body);
}

// "종목명 주가" 쿼리에서 answer → {name, code, market} 추출
async function resolveCode(nameQuery) {
  try {
    const d = await acSearch(nameQuery);
    const a = d.answer?.[0];
    if (!a || a.length < 6) return null;
    // answer 배열: [?, displayName, type, ?, ?, code, market, price, ...]
    const code = a[5];
    if (!/^\d{6}$/.test(code)) return null;
    const name = a[1].replace(/\s*주가$/, '').trim();
    return { name, code, market: a[6] || '' };
  } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || 'unknown';
  if (!checkRateLimit(ip)) return res.status(429).json({ results: [] });

  const q = new URL(req.url, 'http://localhost').searchParams.get('q') || '';
  if (!q.trim()) return res.json({ results: [] });

  try {
    // 1단계: 자동완성 텍스트 목록
    const d = await acSearch(q);
    const rawItems = d.items?.[0] || [];

    // 중복 없이 후보 이름 수집 (최대 8개)
    const seen = new Set();
    const candidates = [];
    for (const [text] of rawItems) {
      const base = text.replace(/\s*주가$/, '').trim();
      if (!seen.has(base)) { seen.add(base); candidates.push(base); }
      if (candidates.length >= 8) break;
    }

    // 2단계: 병렬로 "이름 주가" 코드 조회
    const settled = await Promise.allSettled(
      candidates.map(name => resolveCode(name + ' 주가'))
    );

    const results = [];
    const codeSeen = new Set();
    for (const s of settled) {
      if (s.status !== 'fulfilled' || !s.value) continue;
      const { name, code, market } = s.value;
      if (codeSeen.has(code)) continue;
      codeSeen.add(code);
      results.push({ name, code, market });
      if (results.length >= 6) break;
    }

    return res.json({ results });
  } catch (e) {
    return res.status(503).json({ results: [], error: e.message });
  }
};
