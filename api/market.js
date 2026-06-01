'use strict';
/**
 * Vercel 서버리스 함수 — /api/market
 * 네이버 금융 폴링 API에서 KOSPI 200 실시간 데이터 반환
 * (서버리스 = 상태 없음, 호출마다 Naver에서 직접 fetch)
 */
const https = require('https');

function httpsGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/',
        Accept: 'application/json',
        ...headers,
      },
      timeout: 8000,
    }, res => {
      const buf = [];
      res.on('data', c => buf.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(buf).toString() }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
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

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store, max-age=0');

  const session = getSession();

  try {
    const r = await httpsGet(
      'https://polling.finance.naver.com/api/realtime/domestic/index/KPI200'
    );
    if (r.status !== 200) throw new Error('HTTP ' + r.status);

    const d   = JSON.parse(r.body);
    const row = d.datas?.[0];
    if (!row) throw new Error('데이터 없음');

    const price    = parseFloat(row.closePriceRaw);
    const chDay    = parseFloat(row.fluctuationsRatioRaw);
    const absChg   = parseFloat((row.compareToPreviousClosePriceRaw || '0').replace(/,/g, ''));
    const prevClose = isNaN(absChg) ? price : +(price - absChg).toFixed(2);

    if (isNaN(price) || price < 100) throw new Error('유효하지 않은 가격');

    return res.json({
      price,
      ch1m:      0,        // 클라이언트에서 슬라이딩 윈도우로 계산
      chDay:     isNaN(chDay) ? 0 : chDay,
      session,
      source:    row.marketStatus === 'OPEN' ? session : 'closed',
      prevClose,
      pollingInterval: d.pollingInterval || 7000,
      ts: Date.now(),
    });

  } catch (e) {
    // Naver 실패 시 빈 응답 (클라이언트가 시뮬레이션으로 fallback)
    return res.status(503).json({
      error: e.message,
      session,
      source: 'demo',
      ts: Date.now(),
    });
  }
};
