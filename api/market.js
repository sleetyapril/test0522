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

// 단기 응답 캐시 (3초) — 클라이언트 3초 폴링 대응, Naver 중복 호출 방지
const _cache = new Map(); // key → { data, ts }
const CACHE_TTL = 3000;

function getCached(key) {
  const c = _cache.get(key);
  if (c && Date.now() - c.ts < CACHE_TTL) return c.data;
  return null;
}
function setCached(key, data) {
  _cache.set(key, { data, ts: Date.now() });
  if (_cache.size > 200) {
    const cut = Date.now() - CACHE_TTL * 5;
    for (const [k, v] of _cache) if (v.ts < cut) _cache.delete(k);
  }
}

// IP당 분당 120회 rate limit (3초 폴링 × 최대 2클라이언트 여유)
const _rl = new Map(); // ip → { count, resetAt }
const RL_LIMIT  = 120;
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
  if (t <  360  && (day >= 2 || day === 0))           return 'night';
  return 'closed';
}

function logSubscription(apiName, symbol, lastPrice) {
  console.log('[market-subscription]', {
    apiName,
    symbol,
    timestamp: new Date().toISOString(),
    lastPrice,
  });
}

function getNearMonthCode() {
  const now  = Date.now();
  const kst  = new Date(now + 9 * 3_600_000);
  const year = kst.getUTCFullYear();
  for (const m of [3, 6, 9, 12]) {
    let d = new Date(Date.UTC(year, m - 1, 1));
    while (d.getUTCDay() !== 4) d.setUTCDate(d.getUTCDate() + 1);
    d.setUTCDate(d.getUTCDate() + 7);
    d.setUTCHours(6, 45, 0, 0);
    if (now < d.getTime()) return `101W${String(m).padStart(2, '0')}`;
  }
  return '101W03';
}

function getKisFuturesSymbol(session) {
  const month = getNearMonthCode().slice(-2);
  if (session === 'night') {
    const code = month === '12' ? 'C' : String(parseInt(month, 10));
    return `101W${code}000`;
  }
  return `101S${month}`;
}

async function kisToken(appKey, appSecret) {
  if (_state.tok.token && Date.now() < _state.tok.exp) return _state.tok.token;
  const r = await httpsReq('POST', `${KIS_REST}/oauth2/tokenP`, {
    grant_type: 'client_credentials',
    appkey:    appKey,
    appsecret: appSecret,
  });
  if (r.status === 403) {
    // 분당 1회 발급 제한 — 만료된 토큰이 있으면 계속 사용, 없으면 에러
    if (_state.tok.token) {
      console.warn('[KIS] 토큰 rate-limited (1/min), 기존 토큰 재사용');
      _state.tok.exp = Date.now() + 55_000; // 55초 후 재시도
      return _state.tok.token;
    }
    const body = JSON.parse(r.body);
    throw new Error('KIS 토큰 rate-limited: ' + (body.error_description || r.body.slice(0, 100)));
  }
  if (r.status !== 200) throw new Error('KIS 토큰 HTTP ' + r.status + ': ' + r.body.slice(0, 100));
  const d = JSON.parse(r.body);
  if (!d.access_token) throw new Error('access_token 없음: ' + r.body.slice(0, 100));
  _state.tok.token = d.access_token;
  _state.tok.exp   = Date.now() + (d.expires_in - 600) * 1000;
  return _state.tok.token;
}

// 주간·야간 tr_id를 순서대로 시도해 선물 현재가(또는 종가) 반환
async function kisFutures(appKey, appSecret, session) {
  const token = await kisToken(appKey, appSecret);
  const code  = getKisFuturesSymbol(session);
  // 세션에 맞는 KIS API만 사용한다. 야간 실패 시 주간 선물로 위장하지 않고 현물 fallback으로 내려간다.
  const trIds = ['FHMIF10000000'];

  for (const trId of trIds) {
    try {
      const r = await httpsReq('GET',
        `${KIS_REST}/uapi/domestic-futureoption/v1/quotations/inquire-price?FID_COND_MRKT_DIV_CODE=F&FID_INPUT_ISCD=${code}`,
        null, {
          authorization: `Bearer ${token}`,
          appkey:        appKey,
          appsecret:     appSecret,
          'tr_id':       trId,
        }
      );
      if (r.status !== 200) {
        console.warn(`[KIS 선물] tr_id=${trId} HTTP ${r.status}: ${r.body.slice(0, 200)}`);
        continue;
      }
      const d = JSON.parse(r.body);
      if (d.rt_cd !== '0') {
        console.warn(`[KIS 선물] tr_id=${trId} rt_cd=${d.rt_cd} msg=${d.msg1}`);
        continue;
      }
      const o         = d.output || d.output1 || {};
      const price     = parseFloat(o.futs_prpr || o.stck_prpr);
      const chDay     = parseFloat(o.futs_prdy_ctrt || o.prdy_ctrt);
      const prevClose = parseFloat(o.futs_bspr || o.hts_thpr) || (price - parseFloat(o.futs_prdy_vrss || o.prdy_vrss || '0'));
      if (isNaN(price) || price < 100) continue;
      return { price, chDay: isNaN(chDay) ? 0 : chDay, prevClose, contractCode: code };
    } catch (e) { console.warn(`[KIS 선물] tr_id=${trId} 예외:`, e.message); }
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

  // ── 종목 모드: 장중(day)에는 KIS 우선, NXT/실패 시 Naver fallback ──
  if (type === 'stock' && stockCode) {
    const hit = getCached('stock:' + stockCode);
    if (hit) return res.json(hit);

    // 1순위: KIS — 장중에만 (NXT 미지원)
    if (APP_KEY && session === 'day') {
      try {
        const d = await kisStockPrice(APP_KEY, APP_SECRET, stockCode);
        logSubscription('KIS domestic stock quote', stockCode, d.price);
        const body = {
          price:    d.price,
          ch1m:     0,
          chDay:    d.chDay,
          session,
          source:   'kis_stock',
          prevClose: d.prevClose,
          name:     d.name,
          pollingInterval: 3000,
          ts: Date.now(),
        };
        setCached('stock:' + stockCode, body);
        return res.json(body);
      } catch (e) {
        console.warn('[KIS 종목]', e.message);
      }
    }

    // 2순위: Naver — 장외/NXT/KIS 실패 시
    try {
      const d = await fetchNaverStock(stockCode);
      logSubscription(d.nxt ? 'Naver NXT stock quote' : 'Naver domestic stock quote', stockCode, d.price);
      const body = {
        price:    d.price,
        ch1m:     0,
        chDay:    d.chDay,
        session,
        source:   'stock',
        prevClose: d.prevClose,
        name:     d.name,
        pollingInterval: d.pollingInterval,
        ts: Date.now(),
      };
      setCached('stock:' + stockCode, body);
      return res.json(body);
    } catch (e) {
      console.error('[Naver 종목]', e.message);
      return res.status(503).json({ error: e.message, session, source: 'demo', ts: Date.now() });
    }
  }

  // ── 1순위: KIS REST 선물 (type=futures 또는 미지정) ──
  if (APP_KEY && type !== 'spot') {
    try {
      const d = await kisFutures(APP_KEY, APP_SECRET, session);
      const apiName = session === 'night'
        ? 'KIS [국내선물옵션] 실시간시세 > KRX야간선물 실시간종목체결'
        : 'KIS [국내선물옵션] 실시간시세 > 지수선물 실시간체결가';
      logSubscription(apiName, d.contractCode, d.price);
      _state.futures = { ...d, ts: Date.now() };  // 종가 캐시 갱신
      return res.json({
        price:        d.price,
        ch1m:         0,
        chDay:        d.chDay,
        session,
        source:       session === 'night' ? 'night_futures'
                    : session === 'day' ? 'day_futures'
                    : 'closed',
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
  const spotHit = getCached('spot');
  if (spotHit) return res.json(spotHit);
  try {
    const d = await fetchNaver();
    const source = type === 'futures' ? 'spot_fallback' : (d.isOpen ? 'spot' : 'closed');
    logSubscription(
      type === 'futures' ? 'KOSPI200 Spot Fallback' : 'Naver KOSPI200 spot quote',
      'KPI200',
      d.price
    );
    const body = {
      price:    d.price,
      ch1m:     0,
      chDay:    d.chDay,
      session,
      source,
      prevClose: d.prevClose,
      pollingInterval: d.pollingInterval,
      ts: Date.now(),
    };
    setCached('spot', body);
    return res.json(body);
  } catch (e) {
    console.error('[Naver]', e.message);
  }

  return res.status(503).json({ error: '모든 소스 실패', session, source: 'demo', ts: Date.now() });
};
