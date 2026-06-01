'use strict';
/**
 * KOSPI Defender — Market Data Server
 *
 * 데이터 소스 우선순위:
 *   1. KIS Open API WebSocket  — .env KIS_APP_KEY/KIS_APP_SECRET 설정 시 (실시간 0딜레이)
 *   2. 네이버 금융 폴링 API     — API 키 불필요, 7초 갱신 (기본값)
 *   3. Yahoo Finance v8/v7     — 네이버 실패 시 fallback
 *   4. 시뮬레이션              — 모든 소스 실패 시
 */

const express   = require('express');
const https     = require('https');
const http      = require('http');
const path      = require('path');
const fs        = require('fs');
const WebSocket = require('ws');

// ── .env 로드 ──────────────────────────────────
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  fs.readFileSync(envPath, 'utf8').split(/\r?\n/).forEach(line => {
    const eq = line.indexOf('=');
    if (eq < 1) return;
    const k = line.slice(0, eq).trim();
    const v = line.slice(eq + 1).trim().replace(/^["']|["']$/g, '');
    if (k) process.env[k] = v;
  });
}

const APP_KEY    = process.env.KIS_APP_KEY    || '';
const APP_SECRET = process.env.KIS_APP_SECRET || '';
const PORT       = process.env.PORT || 3000;

// KIS 실전투자 엔드포인트
const KIS_REST = 'https://openapi.koreainvestment.com:9443';
const KIS_WS   = 'wss://openapi.koreainvestment.com:21000/websocket';

// ── 유틸 ───────────────────────────────────────
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

function getSession() {
  const kst = new Date(Date.now() + 9 * 3_600_000);
  const day = kst.getUTCDay();
  const t   = kst.getUTCHours() * 60 + kst.getUTCMinutes();
  if (day >= 1 && day <= 5 && t >= 540  && t <= 945) return 'day';
  if (t >= 1080 && day >= 1 && day <= 6)             return 'night';
  if (t <  300  && (day >= 2 || day === 0))           return 'night';
  return 'closed';
}

function httpsReq(method, url, body, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const u = new URL(url);
    const options = {
      hostname: u.hostname,
      port:     u.port || 443,
      path:     u.pathname + u.search,
      method,
      headers: {
        'User-Agent':    'KOSPI-Defender/1.0',
        'Content-Type':  'application/json; charset=utf-8',
        ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
        ...extraHeaders,
      },
      timeout: 12_000,
    };
    const req = https.request(options, res => {
      const buf = [];
      res.on('data', c => buf.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(buf).toString() }));
    });
    req.on('error',   reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    if (payload) req.write(payload);
    req.end();
  });
}

// ── KOSPI 200 선물 근월물 코드 계산 ────────────
function getSecondThursday(year, month) {
  // 해당 월 첫날부터 목요일 찾기
  let d = new Date(Date.UTC(year, month - 1, 1));
  while (d.getUTCDay() !== 4) d.setUTCDate(d.getUTCDate() + 1);
  d.setUTCDate(d.getUTCDate() + 7);      // 두 번째 목요일
  d.setUTCHours(6, 45, 0, 0);            // 15:45 KST = 06:45 UTC
  return d;
}

function getNearMonthCode() {
  const now      = Date.now();
  const kst      = new Date(now + 9 * 3_600_000);
  const year     = kst.getUTCFullYear();
  const yy       = String(year % 100).padStart(2, '0');
  const quarters = [3, 6, 9, 12];

  for (const month of quarters) {
    const expiry = getSecondThursday(year, month);
    if (now < expiry.getTime()) {
      return `101W${yy}${String(month).padStart(2, '0')}`;
    }
  }
  const next = String((year + 1) % 100).padStart(2, '0');
  return `101W${next}03`;
}

// ── 공유 데이터 캐시 ───────────────────────────
const cache = {
  price:     1389,
  ch1m:      0,
  chDay:     0,
  session:   'closed',
  source:    'demo',   // 'day'|'night'|'closed'|'demo'
  prevClose: 1389,
  contractCode: '',
  ts:        Date.now(),
};

// 1분 변화율 계산용 슬라이딩 윈도우
const priceWindow = [];   // { price, ts }

function pushPriceWindow(price) {
  priceWindow.push({ price, ts: Date.now() });
  const cutoff = Date.now() - 90_000;
  while (priceWindow.length && priceWindow[0].ts < cutoff) priceWindow.shift();
}

function calc1mChange(cur) {
  const target = Date.now() - 60_000;
  if (!priceWindow.length) return 0;
  let best = priceWindow[0];
  for (const p of priceWindow) {
    if (Math.abs(p.ts - target) < Math.abs(best.ts - target)) best = p;
  }
  if (Math.abs(best.ts - target) > 30_000) return 0;
  return (cur - best.price) / best.price * 100;
}

// ═══════════════════════════════════════════════
// ① KIS WebSocket (실시간)
// ═══════════════════════════════════════════════
const kis = { ws: null, connected: false, reconnTimer: null };

async function kisGetApprovalKey() {
  // WebSocket 전용 approval_key — /oauth2/Approval 엔드포인트
  const res = await httpsReq('POST', `${KIS_REST}/oauth2/Approval`, {
    grant_type: 'client_credentials',
    appkey:     APP_KEY,
    secretkey:  APP_SECRET,          // ← Approval은 secretkey (tokenP는 appsecret)
  });
  if (res.status !== 200) throw new Error(`HTTP ${res.status}: ${res.body.slice(0, 200)}`);
  const d = JSON.parse(res.body);
  if (!d.approval_key) throw new Error('approval_key 없음: ' + JSON.stringify(d));
  console.log('[KIS] approval_key 발급 완료');
  return d.approval_key;
}

async function kisConnect() {
  if (!APP_KEY) return;
  clearTimeout(kis.reconnTimer);

  let approvalKey;
  try {
    approvalKey = await kisGetApprovalKey();
  } catch (e) {
    console.error('[KIS] approval_key 실패:', e.message);
    kis.reconnTimer = setTimeout(kisConnect, 60_000);
    return;
  }

  const session = getSession();
  const trId    = session === 'night' ? 'H0NFCNT0' : 'H0ZFCNT0';
  const code    = getNearMonthCode();
  cache.contractCode = code;
  console.log(`[KIS] WebSocket 연결  tr_id=${trId}  code=${code}`);

  if (kis.ws) try { kis.ws.close(); } catch (_) {}
  const ws = new WebSocket(KIS_WS);
  kis.ws = ws;

  ws.on('open', () => {
    ws.send(JSON.stringify({
      header: {
        approval_key: approvalKey,
        custtype:     'P',           // 개인
        tr_type:      '1',           // 등록
        'content-type': 'utf-8',
      },
      body: { input: { tr_id: trId, tr_key: code } },
    }));
    kis.connected = true;
  });

  ws.on('message', raw => {
    const msg = raw.toString();

    // ── JSON 제어 메시지 ──
    if (msg.startsWith('{')) {
      try {
        const d = JSON.parse(msg);
        const hdr = d.header || {};

        // PINGPONG 응답
        if (hdr.tr_id === 'PINGPONG') {
          ws.send(JSON.stringify({ header: { tr_id: 'PINGPONG' } }));
          return;
        }
        // 구독 결과 확인
        if (d.body?.rt_cd === '0') {
          console.log(`[KIS] 구독 성공: ${d.body.msg1}`);
        } else if (d.body?.rt_cd) {
          console.warn('[KIS] 구독 응답:', JSON.stringify(d.body));
        }
      } catch (_) {}
      return;
    }

    // ── 실시간 데이터 MSGTYPE|TR_ID|MSGCNT|DATA ──
    const parts = msg.split('|');
    if (parts.length < 4) return;
    const fields = parts[3].split('^');

    // H0ZFCNT0 / H0NFCNT0 필드 순서:
    // [0] 종목코드  [1] 영업일자  [2] 체결시각
    // [3] 현재가   [4] 전일대비  [5] 전일대비율
    const price = parseFloat(fields[3]);
    const chDay = parseFloat(fields[5]);
    if (!price || isNaN(price) || price < 100) return;

    pushPriceWindow(price);
    const ch1m = calc1mChange(price);

    cache.price  = price;
    cache.ch1m   = clamp(ch1m,  -3,  3);
    cache.chDay  = isNaN(chDay) ? cache.chDay : clamp(chDay, -5, 5);
    cache.source = session === 'night' ? 'night' : 'day';
    cache.ts     = Date.now();
  });

  ws.on('close', (code, reason) => {
    kis.connected = false;
    console.warn(`[KIS WS] 연결 종료 (${code}) — 30초 후 재연결`);
    kis.reconnTimer = setTimeout(kisConnect, 30_000);
  });

  ws.on('error', e => console.error('[KIS WS 오류]', e.message));
}

// 세션이 바뀌면 tr_id 재구독 (주간↔야간)
let _lastSession = '';
setInterval(() => {
  const s = getSession();
  if (s !== _lastSession && kis.connected) {
    _lastSession = s;
    console.log(`[KIS] 세션 전환 (${s}) — 재구독`);
    kisConnect();
  }
  cache.session = s;
}, 60_000);

// ═══════════════════════════════════════════════
// ② 네이버 금융 폴링 API  (API 키 불필요, 기본 소스)
//    CORS 차단 → 서버에서만 호출 가능
// ═══════════════════════════════════════════════
const NAVER_URL = 'https://polling.finance.naver.com/api/realtime/domestic/index/KPI200';

async function fetchNaver() {
  const res = await httpsReq('GET', NAVER_URL, null, {
    Referer: 'https://finance.naver.com/',
    Accept:  'application/json',
  });
  if (res.status !== 200) throw new Error('HTTP ' + res.status);
  const d   = JSON.parse(res.body);
  const row = d.datas?.[0];
  if (!row) throw new Error('데이터 없음');

  const price  = parseFloat(row.closePriceRaw);
  const chDay  = parseFloat(row.fluctuationsRatioRaw);
  const isOpen = row.marketStatus === 'OPEN';
  if (isNaN(price) || price < 100) throw new Error('유효하지 않은 가격: ' + row.closePriceRaw);

  return { price, chDay, isOpen, pollingInterval: d.pollingInterval || 7000 };
}

// ═══════════════════════════════════════════════
// ③ Yahoo Finance (KIS 없거나 미연결 시 fallback)
// ═══════════════════════════════════════════════
const yf = { cookies: '', crumb: '', refreshedAt: 0 };

async function yfRefreshSession() {
  const res = await httpsReq('GET', 'https://finance.yahoo.com/quote/%5EKS200/', null, {
    Accept: 'text/html',
  });
  const setCookies = (res.body.match(/Set-Cookie: ([^;]+)/g) || []).map(s => s.slice(12));
  yf.cookies = setCookies.join('; ');
  const m = res.body.match(/"crumb"\s*:\s*"([^"\\]+)"/);
  if (m) yf.crumb = m[1].replace(/\\u002F/g, '/');
  yf.refreshedAt = Date.now();
  console.log('[YF] 세션 갱신  crumb=', yf.crumb ? yf.crumb.slice(0, 8) + '…' : '없음');
}

async function yfFetch() {
  if (!yf.crumb || Date.now() - yf.refreshedAt > 25 * 60_000) await yfRefreshSession();
  const url = `https://query2.finance.yahoo.com/v8/finance/chart/%5EKS200?interval=1m&range=1d&crumb=${encodeURIComponent(yf.crumb)}`;
  const res = await httpsReq('GET', url, null, {
    Cookie:   yf.cookies,
    Referer:  'https://finance.yahoo.com/',
    Accept:   'application/json',
  });
  if (res.status === 429) throw new Error('rate-limited (429)');
  if (res.status !== 200) throw new Error('HTTP ' + res.status);
  const d = JSON.parse(res.body);
  if (d.chart?.error) throw new Error(d.chart.error.description);
  const result = d.chart.result[0];
  const meta   = result.meta;
  const closes = result.indicators.quote[0].close.filter(c => c != null);
  const cur    = closes.length ? closes[closes.length - 1] : meta.regularMarketPrice;
  const prv    = closes.length >= 2 ? closes[closes.length - 2] : meta.chartPreviousClose;
  return {
    price: cur,
    ch1m:  prv ? (cur - prv) / prv * 100 : 0,
    chDay: meta.chartPreviousClose ? (cur - meta.chartPreviousClose) / meta.chartPreviousClose * 100 : 0,
    prevClose: meta.chartPreviousClose,
  };
}

// ── Yahoo Finance v7 fallback ──
async function yfFetchV7() {
  const res = await httpsReq('GET',
    'https://query1.finance.yahoo.com/v7/finance/quote?symbols=%5EKS200', null, {
      Accept: 'application/json', Referer: 'https://finance.yahoo.com/',
    });
  if (res.status === 429) throw new Error('rate-limited (429)');
  if (res.status !== 200) throw new Error('HTTP ' + res.status);
  const q = JSON.parse(res.body).quoteResponse?.result?.[0];
  if (!q?.regularMarketPrice) throw new Error('no price');
  const cur  = q.regularMarketPrice;
  const prev = cache.source === 'day' ? cache.price : (q.regularMarketPreviousClose || cur);
  return {
    price: cur,
    ch1m:  prev && prev !== cur ? (cur - prev) / prev * 100 : 0,
    chDay: q.regularMarketPreviousClose ? (cur - q.regularMarketPreviousClose) / q.regularMarketPreviousClose * 100 : 0,
    prevClose: q.regularMarketPreviousClose,
  };
}

// ═══════════════════════════════════════════════
// ③ 시뮬레이션 fallback
// ═══════════════════════════════════════════════
function simStep() {
  const shock = (Math.random() - 0.47) * 0.35;
  cache.ch1m  = clamp(cache.ch1m + shock - cache.ch1m * 0.3, -3, 3);
  cache.price = cache.price * (1 + cache.ch1m / 100);
}

// ═══════════════════════════════════════════════
// 폴링 루프  네이버(기본) → Yahoo(fallback)
// ═══════════════════════════════════════════════
let nextPollMs = 7_000;
let failStreak = 0;

async function poll() {
  if (kis.connected) return;

  cache.session = getSession();

  // 장 마감 → source만 업데이트
  if (cache.session === 'closed') {
    if (cache.source !== 'closed' && cache.source !== 'demo') {
      cache.source = 'closed';
      console.log('[KOSPI] 장 마감');
    }
    nextPollMs = 60_000; // 장외엔 1분마다 확인
    return;
  }

  let price = null, chDay = 0;

  // ── 1순위: 네이버 금융 ──
  try {
    const d = await fetchNaver();
    price      = d.price;
    chDay      = d.chDay;
    nextPollMs = d.pollingInterval; // Naver 권장값 (보통 7000ms)
    failStreak = 0;
  } catch (e) {
    console.warn('[Naver]', e.message);
  }

  // ── 2순위: Yahoo Finance ──
  if (price === null) {
    for (const fn of [yfFetch, yfFetchV7]) {
      try {
        const d = await fn();
        price      = d.price;
        chDay      = d.chDay;
        nextPollMs = 10_000;
        failStreak = 0;
        break;
      } catch (e) {
        console.warn(`[YF] ${fn.name}:`, e.message);
      }
    }
  }

  if (price !== null) {
    pushPriceWindow(price);
    cache.price   = price;
    cache.ch1m    = clamp(calc1mChange(price), -3, 3);
    cache.chDay   = clamp(chDay, -5, 5);
    cache.source  = cache.session; // 'day' | 'night'
    cache.ts      = Date.now();
    console.log(`[KOSPI] ${price.toFixed(2)}  1m:${cache.ch1m.toFixed(3)}%  day:${chDay.toFixed(3)}%`);
  } else {
    // ── 3순위: 시뮬레이션 ──
    failStreak++;
    simStep();
    cache.source = 'demo';
    nextPollMs   = Math.min(7_000 * Math.pow(2, failStreak), 120_000);
    console.warn(`[KOSPI] 모든 소스 실패 (${failStreak}회) — ${nextPollMs / 1000}s 후 재시도`);
  }
}

function schedulePoll() {
  poll().finally(() => {
    if (!kis.connected) setTimeout(schedulePoll, nextPollMs);
  });
}

// ═══════════════════════════════════════════════
// 시작
// ═══════════════════════════════════════════════
(async () => {
  cache.contractCode = getNearMonthCode();
  cache.session      = getSession();

  if (APP_KEY) {
    console.log('[KIS] API 키 감지 → KIS WebSocket 연결 시도');
    await kisConnect();
    // 5초 후에도 미연결이면 네이버 폴링 병행
    setTimeout(() => { if (!kis.connected) schedulePoll(); }, 5_000);
  } else {
    console.log('[INFO] 네이버 금융 폴링 시작 (API 키 불필요)');
    schedulePoll();
  }
})();

// ═══════════════════════════════════════════════
// Express API
// ═══════════════════════════════════════════════
const app = express();

app.get('/api/market', (_req, res) => {
  res.json({
    price:        +cache.price.toFixed(2),
    ch1m:         +cache.ch1m.toFixed(4),
    chDay:        +cache.chDay.toFixed(4),
    session:       cache.session,
    source:        cache.source,
    prevClose:    +cache.prevClose.toFixed(2),
    contractCode:  cache.contractCode,
    ts:            cache.ts,
    age:           Math.floor((Date.now() - cache.ts) / 1000),
    kisLive:       kis.connected,
  });
});

app.use(express.static(__dirname));
app.get('/',     (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/play', (_req, res) => res.sendFile(path.join(__dirname, 'game.html')));

http.createServer(app).listen(PORT, () => {
  console.log('\n─────────────────────────────────────────────');
  console.log(`  KOSPI Defender  →  http://localhost:${PORT}`);
  console.log(`  Game            →  http://localhost:${PORT}/game.html`);
  if (!APP_KEY) {
    console.log('  데이터: 네이버 금융 → Yahoo Finance → 시뮬레이션');
    console.log('\n  💡 KIS WebSocket 연동 (선물 실시간):');
    console.log('     https://apiportal.koreainvestment.com  무료 가입');
    console.log('     .env  →  KIS_APP_KEY=...  /  KIS_APP_SECRET=...');
  }
  console.log('─────────────────────────────────────────────\n');
});
