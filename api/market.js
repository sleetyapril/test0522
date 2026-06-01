'use strict';
/**
 * Vercel 서버리스 함수 — /api/market
 * ?type=futures  KIS REST 선물 (주간·야간 tr_id 순차 시도) → 캐시 → Naver
 * ?type=spot     Naver KPI200 현물만 사용
 * (미지정)       futures와 동일
 */
const https = require('https');

const KIS_REST = 'https://openapi.koreainvestment.com:9443';

// 워밍 인스턴스 내 상태 캐시
const _state = {
  tok:     { token: '', exp: 0 },
  futures: { price: 0, chDay: 0, prevClose: 0, contractCode: '', ts: 0 },
};

// IP당 분당 30회 rate limit
const _rl = new Map(); // ip → { count, resetAt }
const RL_LIMIT  = 30;
const RL_WINDOW = 60_000;

function checkRateLimit(ip) {
  const now = Date.now();
  let entry = _rl.get(ip);
  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + RL_WINDOW };
    _rl.set(ip, entry);
  }
  entry.count++;
  // 만료된 IP 정리 (메모리 누수 방지)
  if (_rl.size > 2000) {
    for (const [k, v] of _rl) if (now >= v.resetAt) _rl.delete(k);
  }
  return entry.count <= RL_LIMIT;
}

function httpsReq(method, url, body, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const u = new URL(url);
    const opts = {
      hostname: u.hostname,
      port:     u.port || 443,
      path:     u.pathname + u.search,
      method,
      headers: {
        'User-Agent':   'KOSPI-Defender/1.0',
        'Content-Type': 'application/json; charset=utf-8',
        ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
        ...extraHeaders,
      },
      timeout: 8000,
    };
    const req = https.request(opts, res => {
      const buf = [];
      res.on('data', c => buf.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(buf).toString() }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    if (payload) req.write(payload);
    req.end();
  });
}

function getSession() {
  const kst = new Date(Date.now() + 9 * 3_600_000);
  const day = kst.getUTCDay();
  const t   = kst.getUTCHours() * 60 + kst.getUTCMinutes();
  if (day >= 1 && day <= 5 && t >= 540  && t <= 945) return 'day';
  if (t >= 1080 && day >= 1 && day <= 6)             return 'night';
  if (t <  300  && (day >= 2 || day === 0))           return 'night';
  return 'closed';
}

function getNearMonthCode() {
  const now  = Date.now();
  const kst  = new Date(now + 9 * 3_600_000);
  const year = kst.getUTCFullYear();
  const yy   = String(year % 100).padStart(2, '0');
  for (const m of [3, 6, 9, 12]) {
    let d = new Date(Date.UTC(year, m - 1, 1));
    while (d.getUTCDay() !== 4) d.setUTCDate(d.getUTCDate() + 1);
    d.setUTCDate(d.getUTCDate() + 7);
    d.setUTCHours(6, 45, 0, 0);
    if (now < d.getTime()) return `101W${yy}${String(m).padStart(2, '0')}`;
  }
  return `101W${String((year + 1) % 100).padStart(2, '0')}03`;
}

async function kisToken(appKey, appSecret) {
  if (_state.tok.token && Date.now() < _state.tok.exp) return _state.tok.token;
  const r = await httpsReq('POST', `${KIS_REST}/oauth2/tokenP`, {
    grant_type: 'client_credentials',
    appkey:    appKey,
    appsecret: appSecret,
  });
  if (r.status !== 200) throw new Error('KIS 토큰 HTTP ' + r.status);
  const d = JSON.parse(r.body);
  if (!d.access_token) throw new Error('access_token 없음');
  _state.tok.token = d.access_token;
  _state.tok.exp   = Date.now() + (d.expires_in - 600) * 1000;
  return _state.tok.token;
}

// 주간·야간 tr_id를 순서대로 시도해 선물 현재가(또는 종가) 반환
async function kisFutures(appKey, appSecret, session) {
  const token = await kisToken(appKey, appSecret);
  const code  = getNearMonthCode();
  // 세션에 맞는 tr_id를 먼저 시도하고, 실패 시 반대 tr_id도 시도
  const trIds = session === 'night'
    ? ['FHKIF03020100', 'FHKIF03010100']
    : ['FHKIF03010100', 'FHKIF03020100'];

  for (const trId of trIds) {
    try {
      const r = await httpsReq('GET',
        `${KIS_REST}/uapi/domestic-futureoption/v1/quotations/inquire-price?FUT_CODE=${code}`,
        null, {
          authorization: `Bearer ${token}`,
          appkey:        appKey,
          appsecret:     appSecret,
          'tr_id':       trId,
        }
      );
      if (r.status !== 200) continue;
      const d = JSON.parse(r.body);
      if (d.rt_cd !== '0') continue;
      const o         = d.output;
      const price     = parseFloat(o.futs_prpr);
      const chDay     = parseFloat(o.prdy_ctrt);
      const prevClose = parseFloat(o.futs_bspr) || (price - parseFloat(o.prdy_vrss || '0'));
      if (isNaN(price) || price < 100) continue;
      return { price, chDay: isNaN(chDay) ? 0 : chDay, prevClose, contractCode: code };
    } catch (_) {}
  }
  throw new Error('KIS 선물 조회 실패 (모든 tr_id)');
}

// ── KIS REST: 국내 주식 현재가 ──
async function kisStockPrice(appKey, appSecret, code) {
  const token = await kisToken(appKey, appSecret);
  const r = await httpsReq('GET',
    `${KIS_REST}/uapi/domestic-stock/v1/quotations/inquire-price?FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD=${code}`,
    null, {
      authorization: `Bearer ${token}`,
      appkey:        appKey,
      appsecret:     appSecret,
      'tr_id':       'FHKST01010100',
    }
  );
  if (r.status !== 200) throw new Error('KIS HTTP ' + r.status);
  const d = JSON.parse(r.body);
  if (d.rt_cd !== '0') throw new Error(`KIS: ${d.msg1} (rt_cd=${d.rt_cd})`);
  const o        = d.output;
  const price    = parseFloat(o.stck_prpr);
  const chDay    = parseFloat(o.prdy_ctrt);
  const prevClose = parseFloat(o.stck_prdy_clpr) || (price - parseFloat(o.prdy_vrss || '0'));
  if (isNaN(price) || price < 1) throw new Error('유효하지 않은 주가: ' + o.stck_prpr);
  return { price, chDay: isNaN(chDay) ? 0 : chDay, prevClose, name: o.hts_kor_isnm || code };
}

// ── Naver: 국내 주식 현재가 + NXT(시간외) 지원 ──
async function fetchNaverStock(code) {
  const r = await httpsReq('GET',
    `https://polling.finance.naver.com/api/realtime/domestic/stock/${code}`,
    null, { Referer: 'https://finance.naver.com/', Accept: 'application/json' }
  );
  if (r.status !== 200) throw new Error('Naver 주식 HTTP ' + r.status);
  const d   = JSON.parse(r.body);
  const row = d.datas?.[0];
  if (!row) throw new Error('Naver 주식 데이터 없음');

  const regularOpen = row.marketStatus === 'OPEN';
  const over        = row.overMarketPriceInfo;
  const nxtOpen     = !regularOpen && over?.overMarketStatus === 'OPEN' && over.overPrice;

  let price, chDay, prevClose;
  if (nxtOpen) {
    // 정규장 마감 후 시간외(NXT) 가격 사용
    price    = parseFloat(String(over.overPrice).replace(/,/g, ''));
    chDay    = parseFloat(over.fluctuationsRatio ?? 0);
    const d2 = parseFloat(String(over.compareToPreviousClosePrice ?? '0').replace(/,/g, ''));
    prevClose = Math.round(price - d2);
  } else {
    price    = parseFloat(row.closePriceRaw ?? row.closePrice);
    chDay    = parseFloat(row.fluctuationsRatioRaw ?? row.fluctuationsRatio ?? 0);
    const d2 = parseFloat(String(row.compareToPreviousClosePriceRaw ?? '0').replace(/,/g, ''));
    prevClose = Math.round(price - d2);
  }

  if (isNaN(price) || price < 1) throw new Error('유효하지 않은 주식 가격');
  return {
    price,
    chDay:    isNaN(chDay) ? 0 : chDay,
    prevClose: isNaN(prevClose) ? price : prevClose,
    isOpen:   regularOpen,
    nxt:      !!nxtOpen,
    name:     row.stockName || code,
    pollingInterval: d.pollingInterval || 5000,
  };
}

async function fetchNaver() {
  const r = await httpsReq('GET',
    'https://polling.finance.naver.com/api/realtime/domestic/index/KPI200',
    null, { Referer: 'https://finance.naver.com/', Accept: 'application/json' }
  );
  if (r.status !== 200) throw new Error('Naver HTTP ' + r.status);
  const d   = JSON.parse(r.body);
  const row = d.datas?.[0];
  if (!row) throw new Error('Naver 데이터 없음');
  const price  = parseFloat(row.closePriceRaw);
  const chDay  = parseFloat(row.fluctuationsRatioRaw);
  const absChg = parseFloat((row.compareToPreviousClosePriceRaw || '0').replace(/,/g, ''));
  if (isNaN(price) || price < 100) throw new Error('유효하지 않은 Naver 가격');
  return {
    price,
    chDay:     isNaN(chDay) ? 0 : chDay,
    prevClose: isNaN(absChg) ? price : +(price - absChg).toFixed(2),
    isOpen:    row.marketStatus === 'OPEN',
    pollingInterval: d.pollingInterval || 7000,
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store, max-age=0');

  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim()
          || req.headers['x-real-ip']
          || 'unknown';
  if (!checkRateLimit(ip)) {
    return res.status(429).json({ error: '요청이 너무 많습니다. 잠시 후 다시 시도하세요.', ts: Date.now() });
  }

  const session    = getSession();
  const APP_KEY    = process.env.KIS_APP_KEY    || '';
  const APP_SECRET = process.env.KIS_APP_SECRET || '';
  const params     = new URL(req.url, 'http://localhost').searchParams;
  const type       = params.get('type') || 'futures';
  const stockCode  = (params.get('code') || '').replace(/[^0-9A-Za-z]/g, '').slice(0, 12);

  // ── 종목 모드: KIS는 NXT(시간외) 미지원 → Naver만 사용 ──
  if (type === 'stock' && stockCode) {
    try {
      const d = await fetchNaverStock(stockCode);
      return res.json({
        price:    d.price,
        ch1m:     0,
        chDay:    d.chDay,
        session,
        source:   d.nxt ? 'nxt' : (d.isOpen ? session : 'closed'),
        prevClose: d.prevClose,
        name:     d.name,
        pollingInterval: d.pollingInterval,
        ts: Date.now(),
      });
    } catch (e) {
      console.error('[Naver 종목]', e.message);
      return res.status(503).json({ error: e.message, session, source: 'demo', ts: Date.now() });
    }
  }

  // ── 1순위: KIS REST 선물 (type=futures 또는 미지정) ──
  if (APP_KEY && type !== 'spot') {
    try {
      const d = await kisFutures(APP_KEY, APP_SECRET, session);
      _state.futures = { ...d, ts: Date.now() };  // 종가 캐시 갱신
      return res.json({
        price:        d.price,
        ch1m:         0,
        chDay:        d.chDay,
        session,
        source:       session === 'closed' ? 'closed' : session,
        prevClose:    d.prevClose,
        contractCode: d.contractCode,
        pollingInterval: 5000,
        ts: Date.now(),
      });
    } catch (e) {
      console.error('[KIS]', e.message);
      // KIS 실패 시 캐시된 선물 종가 반환 (당일 12시간 이내)
      if (_state.futures.price && (Date.now() - _state.futures.ts) < 12 * 3_600_000) {
        return res.json({
          price:        _state.futures.price,
          ch1m:         0,
          chDay:        _state.futures.chDay,
          session,
          source:       'closed',
          prevClose:    _state.futures.prevClose,
          contractCode: _state.futures.contractCode,
          pollingInterval: 30000,
          ts: _state.futures.ts,
        });
      }
    }
  }

  // ── 2순위: 네이버 KPI200 현물 ──
  try {
    const d = await fetchNaver();
    return res.json({
      price:    d.price,
      ch1m:     0,
      chDay:    d.chDay,
      session,
      source:   d.isOpen ? session : 'closed',
      prevClose: d.prevClose,
      pollingInterval: d.pollingInterval,
      ts: Date.now(),
    });
  } catch (e) {
    console.error('[Naver]', e.message);
  }

  return res.status(503).json({ error: '모든 소스 실패', session, source: 'demo', ts: Date.now() });
};
