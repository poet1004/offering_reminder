const CONFIG = window.__IPO_STATIC_CONFIG__ || {};
const state = {
  feed: null,
  health: null,
  backtest: [],
  items: [],
  unlockRows: [],
  timeline: [],
  today: null,
  ui: { listingPage: 1, listingPerPage: 8, subscriptionPage: 1, subscriptionPerPage: 8, unlockPage: 1, unlockPerPage: 8, explorerPage: 1, explorerPerPage: 8, calendarMonthOffset: 0, calendarSelectedDate: '' },
};

const el = (selector) => document.querySelector(selector);
const els = (selector) => Array.from(document.querySelectorAll(selector));

function parseDate(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (!text) return null;
  const plain = text.length >= 10 ? text.slice(0, 10) : text;
  const date = new Date(`${plain}T00:00:00`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function startOfDay(value = new Date()) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function diffDays(a, b) {
  if (!a || !b) return null;
  const one = Date.UTC(a.getFullYear(), a.getMonth(), a.getDate());
  const two = Date.UTC(b.getFullYear(), b.getMonth(), b.getDate());
  return Math.round((one - two) / 86400000);
}

function formatDateShort(value) {
  const date = parseDate(value);
  if (!date) return '-';
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${month}.${day}`;
}

function formatDateFull(value) {
  const date = parseDate(value);
  if (!date) return '-';
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function formatDateTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
}

function formatNumber(value) {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return new Intl.NumberFormat('ko-KR').format(num);
}

function formatPrice(value) {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return `${new Intl.NumberFormat('ko-KR').format(num)}원`;
}

function formatRatio(value, digits = 2, suffix = '%') {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return `${num.toFixed(digits)}${suffix}`;
}

function formatCompetition(value) {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return `${num.toFixed(2)}:1`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/주식회사/g, '')
    .replace(/엔에이치/g, 'nh')
    .replace(/엔에이치투자/g, 'nh')
    .replace(/케이비/g, 'kb')
    .replace(/아이비케이에스/g, 'ibks')
    .replace(/에스케이/g, 'sk')
    .replace(/엘에스/g, 'ls')
    .replace(/제(\d+)호스팩/g, '스팩$1호')
    .replace(/\(주\)/g, '');
}

function sortByDateAsc(a, b, key) {
  const ad = parseDate(a[key]);
  const bd = parseDate(b[key]);
  if (!ad && !bd) return 0;
  if (!ad) return 1;
  if (!bd) return -1;
  return ad - bd;
}

function sortByDateDesc(a, b, key) {
  return sortByDateAsc(b, a, key);
}


function compareByProximity(a, b, key, today) {
  const ad = parseDate(a[key]);
  const bd = parseDate(b[key]);
  const aDelta = ad ? Math.abs(diffDays(ad, today) ?? 999999) : 999999;
  const bDelta = bd ? Math.abs(diffDays(bd, today) ?? 999999) : 999999;
  if (aDelta !== bDelta) return aDelta - bDelta;
  if (ad && bd) return ad - bd;
  if (ad) return -1;
  if (bd) return 1;
  return 0;
}

function isUpcomingStage(item) {
  const stage = String(item.stage || '');
  if (/청약예정|수요예측|청약/.test(stage) && !/상장후/.test(stage)) return true;
  const start = parseDate(item.subscriptionStart);
  return !!(start && diffDays(start, state.today) !== null && diffDays(start, state.today) >= 0);
}

function isListedStage(item) {
  const stage = String(item.stage || '');
  if (/상장후|상장/.test(stage)) return true;
  const listing = parseDate(item.listingDate);
  return !!(listing && diffDays(listing, state.today) !== null && diffDays(listing, state.today) <= 0);
}

function isDelistedStatus(item) {
  return /상장폐지|청산/.test(String(item.listingStatus || ''));
}

function countNearbyItems(items, key, today, days = 30) {
  return (items || []).filter((item) => {
    const date = parseDate(item[key]);
    if (!date) return false;
    const delta = diffDays(date, today);
    return delta !== null && Math.abs(delta) <= days;
  }).length;
}

function buildUpcomingTimeline(items, today) {
  const rows = [];
  for (const item of items || []) {
    const subStart = parseDate(item.subscriptionStart);
    if (subStart) {
      const delta = diffDays(subStart, today);
      if (delta !== null && delta >= -5 && delta <= 45) {
        rows.push({
          id: `${item.id || item.displayName}_subscription`,
          type: 'subscription',
          date: item.subscriptionStart,
          dateObj: subStart,
          name: item.displayName,
          market: item.market,
          stage: item.stage,
          subLabel: `청약 ${item.subscriptionRange}`,
        });
      }
    }
    const listingDate = parseDate(item.listingDate);
    if (listingDate) {
      const delta = diffDays(listingDate, today);
      if (delta !== null && delta >= -30 && delta <= 45) {
        rows.push({
          id: `${item.id || item.displayName}_listing`,
          type: 'listing',
          date: item.listingDate,
          dateObj: listingDate,
          name: item.displayName,
          market: item.market,
          stage: item.stage,
          subLabel: '상장',
        });
      }
    }
  }
  rows.sort((a, b) => {
    const aDelta = Math.abs(diffDays(a.dateObj, today) ?? 999999);
    const bDelta = Math.abs(diffDays(b.dateObj, today) ?? 999999);
    if (aDelta !== bDelta) return aDelta - bDelta;
    return a.dateObj - b.dateObj;
  });
  return rows.slice(0, 8);
}

function buildCalendarEntries(items, unlockRows, monthStart, monthEnd) {
  const rowsByDay = new Map();
  const pushEntry = (dateValue, entry) => {
    const key = toDateKey(dateValue);
    if (!key) return;
    const date = parseDate(dateValue);
    if (!date) return;
    if (date < monthStart || date > monthEnd) return;
    if (!rowsByDay.has(key)) rowsByDay.set(key, []);
    rowsByDay.get(key).push(entry);
  };

  for (const item of items || []) {
    const start = parseDate(item.subscriptionStart);
    const end = parseDate(item.subscriptionEnd) || start;
    if (start && end) {
      const cursor = new Date(start);
      while (cursor <= end) {
        pushEntry(cursor, {
          id: `${item.id || item.displayName}_subscription_${toDateKey(cursor)}`,
          type: 'subscription',
          name: item.displayName,
          market: item.market,
          stage: item.stage,
          meta: `청약 ${item.subscriptionRange}`,
        });
        cursor.setDate(cursor.getDate() + 1);
      }
    }
    if (item.listingDate) {
      pushEntry(item.listingDate, {
        id: `${item.id || item.displayName}_listing`,
        type: 'listing',
        name: item.displayName,
        market: item.market,
        stage: item.stage,
        meta: '상장',
      });
    }
  }

  for (const row of unlockRows || []) {
    pushEntry(row.date, {
      id: row.id,
      type: 'unlock',
      name: row.name,
      market: row.market,
      stage: '상장후',
      meta: `${termLabel(row.term)} 보호예수 해제`,
    });
  }

  for (const value of rowsByDay.values()) {
    value.sort((a, b) => String(a.type || '').localeCompare(String(b.type || ''), 'ko') || String(a.name || '').localeCompare(String(b.name || ''), 'ko'));
  }
  return rowsByDay;
}

function choice(...values) {
  for (const value of values) {
    if (value === 0) return value;
    if (Array.isArray(value) && value.length > 0) return value;
    if (value && typeof value === 'object' && Object.keys(value).length > 0) return value;
    if (value !== null && value !== undefined && value !== '') return value;
  }
  return null;
}

function mergeObjects(base, extra) {
  const merged = { ...(base || {}) };
  Object.entries(extra || {}).forEach(([key, value]) => {
    if (merged[key] === undefined || merged[key] === null || merged[key] === '' || (typeof merged[key] === 'object' && merged[key] && Object.keys(merged[key]).length === 0)) {
      merged[key] = value;
    }
  });
  return merged;
}

function mergeItems(primary, incoming) {
  const merged = { ...primary };
  Object.keys(incoming).forEach((key) => {
    if (key === 'underwriters') {
      const set = new Set([...(primary.underwriters || []), ...(incoming.underwriters || [])].filter(Boolean));
      merged.underwriters = Array.from(set);
      return;
    }
    if (key === 'unlockDetails') {
      merged.unlockDetails = mergeObjects(primary.unlockDetails, incoming.unlockDetails);
      return;
    }
    if (key === 'unlockSchedule') {
      merged.unlockSchedule = { ...(primary.unlockSchedule || {}), ...(incoming.unlockSchedule || {}) };
      return;
    }
    const current = merged[key];
    const next = incoming[key];
    const currentScore = valueScore(current);
    const nextScore = valueScore(next);
    if (nextScore > currentScore) merged[key] = next;
  });
  merged.displayName = chooseDisplayName(primary.displayName || primary.name, incoming.displayName || incoming.name);
  return merged;
}

function valueScore(value) {
  if (value === 0) return 5;
  if (value === null || value === undefined || value === '') return 0;
  if (Array.isArray(value)) return value.length;
  if (typeof value === 'object') return Object.values(value).filter((item) => item !== null && item !== undefined && item !== '').length;
  if (typeof value === 'string') return value.length;
  return 10;
}

function chooseDisplayName(a, b) {
  if (!a) return b || '';
  if (!b) return a || '';
  const score = (text) => {
    const raw = String(text);
    let s = 0;
    if (/스팩/.test(raw)) s += 4;
    if (/제\d+호/.test(raw)) s += 2;
    if (/[A-Z]/.test(raw)) s += 1;
    s -= raw.length * 0.01;
    return s;
  };
  return score(a) >= score(b) ? a : b;
}

function enrichItems(items) {
  const map = new Map();
  for (const raw of items || []) {
    const item = {
      ...raw,
      displayName: raw.name,
      normalizedName: normalizeName(raw.nameKey || raw.name),
      symbol: raw.symbol ? String(raw.symbol).padStart(6, '0') : null,
      market: raw.market || '미상',
      stage: raw.stage || '미상',
      underwriters: Array.isArray(raw.underwriters) ? raw.underwriters.filter(Boolean) : [],
    };
    const key = item.symbol ? `sym:${item.symbol}` : `name:${item.normalizedName}:${item.listingDate || item.subscriptionStart || item.id}`;
    if (!map.has(key)) {
      map.set(key, item);
    } else {
      map.set(key, mergeItems(map.get(key), item));
    }
  }
  return Array.from(map.values()).map((item) => {
    const offerPrice = numberOrNull(item.offerPrice);
    const currentPrice = numberOrNull(item.currentPrice);
    const returnPct = offerPrice && currentPrice ? ((currentPrice - offerPrice) / offerPrice) * 100 : null;
    const listingStatus = deriveListingStatus(item);
    return {
      ...item,
      offerPrice,
      currentPrice,
      returnPct,
      listingStatus,
      subscriptionRange: formatRange(item.subscriptionStart, item.subscriptionEnd),
      forecastText: formatDateShort(item.forecastDate),
      underwriterText: item.underwriters.length ? item.underwriters.join(', ') : '-',
      priceBandText: formatPriceBand(item.priceBandLow, item.priceBandHigh),
      signalText: deriveSignalText(item),
      searchText: [
        item.displayName,
        item.name,
        item.nameKey,
        item.symbol,
        item.market,
        item.stage,
        item.sector,
        item.underwriters.join(' '),
      ].filter(Boolean).join(' ').toLowerCase(),
    };
  });
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function formatRange(start, end) {
  if (!start && !end) return '-';
  const s = formatDateShort(start);
  const e = formatDateShort(end);
  if (s !== '-' && e !== '-' && s !== e) return `${s}~${e}`;
  return s !== '-' ? s : e;
}

function formatPriceBand(low, high) {
  const lowNum = numberOrNull(low);
  const highNum = numberOrNull(high);
  if (lowNum && highNum) return `${formatPrice(lowNum)} ~ ${formatPrice(highNum)}`;
  if (lowNum) return formatPrice(lowNum);
  if (highNum) return formatPrice(highNum);
  return '-';
}

function deriveListingStatus(item) {
  if (item.delistingDate) return '상장폐지';
  if (item.listingStatus) return item.listingStatus;
  if (item.currentPrice || item.ma20 || item.ma60 || item.rsi14) return '상장';
  const listingDate = parseDate(item.listingDate);
  if (listingDate && /스팩/.test(item.displayName || item.name || '') && !item.currentPrice) {
    const age = diffDays(startOfDay(new Date()), listingDate);
    if (age !== null && age >= 365) return '상장상태 미확인';
  }
  return item.listingDate ? '상장' : '미상';
}

function deriveSignalText(item) {
  const ma20 = numberOrNull(item.ma20);
  const ma60 = numberOrNull(item.ma60);
  const rsi14 = numberOrNull(item.rsi14);
  const current = numberOrNull(item.currentPrice);
  const parts = [];
  if (current && ma20 && ma60) {
    if (current > ma20 && ma20 > ma60) parts.push('상승 추세');
    else if (current < ma20 && ma20 < ma60) parts.push('약세 추세');
    else parts.push('중립');
  }
  if (rsi14) {
    if (rsi14 >= 70) parts.push(`RSI ${rsi14.toFixed(1)} 과열`);
    else if (rsi14 <= 30) parts.push(`RSI ${rsi14.toFixed(1)} 과매도`);
    else parts.push(`RSI ${rsi14.toFixed(1)}`);
  }
  return parts.length ? parts.join(' · ') : '-';
}

function buildUnlockRows(items) {
  const rows = [];
  const terms = ['15d', '1m', '3m', '6m', '1y'];
  for (const item of items) {
    const details = item.unlockDetails || {};
    for (const term of terms) {
      const detail = details[term] || {};
      const date = detail.date || (item.unlockSchedule ? item.unlockSchedule[term] : null);
      if (!date) continue;
      rows.push({
        id: `${item.id || item.displayName}_${term}`,
        name: item.displayName,
        symbol: item.symbol,
        market: item.market,
        term,
        date,
        shares: numberOrNull(detail.shares),
        ratio: numberOrNull(detail.ratio),
        remainingLockedShares: numberOrNull(detail.remainingLockedShares),
        searchText: `${item.displayName} ${item.symbol || ''} ${item.market || ''}`.toLowerCase(),
      });
    }
  }
  return rows.sort((a, b) => sortByDateAsc(a, b, 'date'));
}

function buildTimeline(feed, today) {
  return (feed.events || [])
    .map((event) => ({ ...event, dateObj: parseDate(event.date), searchName: String(event.name || '').toLowerCase() }))
    .filter((event) => event.dateObj)
    .sort((a, b) => a.dateObj - b.dateObj)
    .filter((event) => {
      const delta = diffDays(event.dateObj, today);
      return delta !== null && delta >= -3 && delta <= 30;
    })
    .slice(0, 12);
}

function computeCountsFromEvents(events, today, days = 30) {
  const counts = { subscription: 0, listing: 0, unlock: 0 };
  for (const event of events || []) {
    const date = parseDate(event.date);
    const delta = diffDays(date, today);
    if (delta === null || delta < 0 || delta > days) continue;
    if (event.type === 'listing') counts.listing += 1;
    else if (String(event.type || '').startsWith('subscription_')) counts.subscription += 1;
    else if (String(event.type || '').startsWith('unlock_')) counts.unlock += 1;
  }
  return counts;
}

function deriveMarketMood(quotes) {
  const candidates = ['KOSPI', 'KOSDAQ', 'NASDAQ100 Futures', 'S&P500 Futures'];
  const moves = (quotes || [])
    .filter((row) => candidates.includes(row.name))
    .map((row) => numberOrNull(row.changePct))
    .filter((value) => value !== null);
  if (!moves.length) return { label: '중립', detail: '시장 지표 대기' };
  const average = moves.reduce((acc, value) => acc + value, 0) / moves.length;
  const maxAbs = Math.max(...moves.map((value) => Math.abs(value)));
  if (maxAbs >= 3 || Math.abs(average) >= 1.8) return { label: average >= 0 ? '강한 위험선호' : '변동성 확대', detail: `평균 ${formatRatio(average, 2)}` };
  if (average >= 0.6) return { label: '강세', detail: `평균 ${formatRatio(average, 2)}` };
  if (average <= -0.6) return { label: '약세', detail: `평균 ${formatRatio(average, 2)}` };
  return { label: '중립', detail: `평균 ${formatRatio(average, 2)}` };
}

function eventTypeLabel(type) {
  const map = {
    subscription_start: '청약 시작',
    subscription_end: '청약 마감',
    listing: '상장',
    unlock_15d: '15일 보호예수 해제',
    unlock_1m: '1개월 보호예수 해제',
    unlock_3m: '3개월 보호예수 해제',
    unlock_6m: '6개월 보호예수 해제',
    unlock_1y: '1년 보호예수 해제',
  };
  return map[type] || type;
}

function loadJson(paths) {
  const candidates = Array.isArray(paths) ? paths : [paths];
  return candidates.reduce((promise, path) => {
    return promise.catch(() => fetch(path, { cache: 'no-store' }).then((response) => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    }));
  }, Promise.reject(new Error('no candidates')));
}

async function init() {
  try {
    setStatus('데이터 불러오는 중', false);
    const [feed, backtest, health] = await Promise.all([
      loadJson(CONFIG.feedPaths || ['./data/mobile-feed.json']),
      loadJson(CONFIG.backtestPaths || ['./data/backtest-summary.json']).catch(() => []),
      loadJson(CONFIG.healthPaths || ['./data/mobile-feed-verify.json']).catch(() => null),
    ]);
    state.feed = feed;
    state.backtest = Array.isArray(backtest) ? backtest : [];
    state.health = health;
    const today = startOfDay(new Date()) || parseDate(feed.upstreamUpdatedAt || feed.generatedAt) || new Date();
    state.today = today;
    state.feedDate = parseDate(feed.upstreamUpdatedAt || feed.generatedAt);
    state.items = enrichItems(feed.items || []);
    state.unlockRows = buildUnlockRows(state.items);
    state.timeline = buildUpcomingTimeline(state.items, today);
    fillSelectOptions(state.items);
    renderAll();
    bindControls();
    setStatus('정상', true);
    const freshness = `원본 기준 ${formatDateTime(feed.upstreamUpdatedAt || feed.generatedAt)} / 피드 생성 ${formatDateTime(feed.generatedAt)}`;
    el('#freshness-text').textContent = freshness;
  } catch (error) {
    console.error(error);
    setStatus('불러오기 실패', false);
    el('#freshness-text').textContent = error.message || 'JSON 피드를 읽지 못했습니다.';
    els('.section-block').forEach((node) => {
      if (node.id !== 'dashboard') {
        node.innerHTML = `<div class="empty-state">데이터를 불러오지 못했습니다. ${escapeHtml(error.message || '')}</div>`;
      }
    });
  }
}

function setStatus(text, ok) {
  const node = el('#load-status');
  if (!node) return;
  node.textContent = text;
  node.style.background = ok ? 'rgba(22, 163, 74, 0.18)' : 'rgba(245, 158, 11, 0.20)';
}

function fillSelectOptions(items) {
  const marketSet = new Set(items.map((item) => item.market).filter(Boolean));
  ['#subscription-market'].forEach((selector) => populateSelect(selector, marketSet));
}


function populateSelect(selector, values) {
  const select = el(selector);
  if (!select) return;
  Array.from(values).sort((a, b) => String(a).localeCompare(String(b), 'ko')).forEach((value) => {
    const opt = document.createElement('option');
    opt.value = value;
    opt.textContent = value;
    select.appendChild(opt);
  });
}

function populateShortsSelect(items) {
  const select = el('#shorts-item-select');
  if (!select) return;
  const today = state.today;
  const candidates = items
    .filter((item) => item.subscriptionStart)
    .sort((a, b) => sortByDateAsc(a, b, 'subscriptionStart'))
    .sort((a, b) => {
      const ad = Math.abs(diffDays(parseDate(a.subscriptionStart), today) ?? 9999);
      const bd = Math.abs(diffDays(parseDate(b.subscriptionStart), today) ?? 9999);
      return ad - bd;
    })
    .slice(0, 30);
  candidates.forEach((item) => {
    const opt = document.createElement('option');
    opt.value = item.id;
    opt.textContent = `${item.displayName} · ${item.subscriptionRange}`;
    select.appendChild(opt);
  });
}

function bindControls() {
  [
    '#subscription-query', '#subscription-market', '#subscription-group',
    '#listing-query', '#listing-group',
    '#unlock-query', '#unlock-term-filter',
  ].forEach((selector) => {
    const node = el(selector);
    if (node) node.addEventListener('input', () => {
      if (selector.startsWith('#listing-')) state.ui.listingPage = 1;
      if (selector.startsWith('#subscription-')) state.ui.subscriptionPage = 1;
      if (selector.startsWith('#unlock-')) state.ui.unlockPage = 1;
      renderAll();
    });
    if (node) node.addEventListener('change', () => {
      if (selector.startsWith('#listing-')) state.ui.listingPage = 1;
      if (selector.startsWith('#subscription-')) state.ui.subscriptionPage = 1;
      if (selector.startsWith('#unlock-')) state.ui.unlockPage = 1;
      renderAll();
    });
  });

  const calendarPrev = el('#calendar-prev');
  if (calendarPrev) calendarPrev.addEventListener('click', () => {
    state.ui.calendarMonthOffset -= 1;
    state.ui.calendarSelectedDate = '';
    renderCalendar();
  });
  const calendarNext = el('#calendar-next');
  if (calendarNext) calendarNext.addEventListener('click', () => {
    state.ui.calendarMonthOffset += 1;
    state.ui.calendarSelectedDate = '';
    renderCalendar();
  });
}


function renderAll() {
  renderDashboard();
  renderCalendar();
  renderSubscriptions();
  renderListings();
  renderUnlocks();
}


function renderDashboard() {
  const feed = state.feed;
  if (!feed) return;
  const today = state.today;
  const items = state.items;
  const usdkrw = (feed.marketQuotes || []).find((row) => row.name === 'USD/KRW');
  const marketMood = deriveMarketMood(feed.marketQuotes || []);
  const stats = [
    { label: '30일 내 청약', value: countNearbyItems(items.filter((item) => item.subscriptionStart), 'subscriptionStart', today, 30), sub: '최근 ±30일 일정' },
    { label: '30일 내 상장', value: countNearbyItems(items.filter((item) => item.listingDate), 'listingDate', today, 30), sub: '최근 ±30일 일정' },
    { label: '환율', value: usdkrw ? formatNumber(usdkrw.last) : '-', sub: usdkrw ? `USD/KRW ${formatRatio(usdkrw.changePct, 2)}` : '환율 데이터 대기' },
    { label: '시장 분위기', value: marketMood.label, sub: marketMood.detail },
  ];
  const summaryNode = el('#summary-stats');
  if (summaryNode) {
    summaryNode.innerHTML = stats.map((stat) => `
      <div class="stat-card">
        <div class="label">${escapeHtml(stat.label)}</div>
        <div class="value mono">${typeof stat.value === 'number' ? escapeHtml(formatNumber(stat.value)) : escapeHtml(String(stat.value))}</div>
        <div class="sub">${escapeHtml(stat.sub)}</div>
      </div>
    `).join('');
  }

  const quotes = (feed.marketQuotes || []).slice(0, 8);
  const marketNode = el('#market-grid');
  if (marketNode) {
    marketNode.innerHTML = quotes.length ? quotes.map((quote) => {
      const change = numberOrNull(quote.changePct);
      const changeClass = change === null ? '' : change >= 0 ? 'good' : 'bad';
      return `
        <div class="quote-card">
          <div class="name">${escapeHtml(quote.name)}</div>
          <div class="price mono">${escapeHtml(formatNumber(quote.last))}</div>
          <div class="change ${changeClass}">${escapeHtml(formatRatio(change, 2))}</div>
          <div class="table-sub">${escapeHtml(quote.group || '')}${quote.asOf ? ` · ${escapeHtml(formatDateShort(quote.asOf))}` : ''}</div>
        </div>
      `;
    }).join('') : emptyState('시장 지표가 없습니다.');
  }

  const timelineNode = el('#timeline-list');
  if (timelineNode) {
    timelineNode.innerHTML = state.timeline.length ? state.timeline.slice(0, 8).map((event) => {
      const delta = diffDays(event.dateObj, today);
      const dday = delta === 0 ? 'D-day' : delta > 0 ? `D-${delta}` : `D+${Math.abs(delta)}`;
      return `
        <div class="timeline-item compact">
          <div class="timeline-date">${escapeHtml(formatDateShort(event.date))}<br /><span class="table-sub">${escapeHtml(dday)}</span></div>
          <div class="timeline-content">
            <div class="timeline-label">${escapeHtml(event.name)}</div>
            <div class="timeline-sub">${escapeHtml(event.subLabel)} · ${escapeHtml(event.market || '미상')}</div>
          </div>
        </div>
      `;
    }).join('') : emptyState('가까운 일정이 없습니다.');
  }
}


function buildBriefingLine1(quotes, largestMove) {
  const kospi = quotes.find((item) => item.name === 'KOSPI');
  const kosdaq = quotes.find((item) => item.name === 'KOSDAQ');
  const pieces = [];
  if (kospi) pieces.push(`코스피 ${formatRatio(kospi.changePct)}`);
  if (kosdaq) pieces.push(`코스닥 ${formatRatio(kosdaq.changePct)}`);
  if (largestMove) pieces.push(`가장 큰 움직임은 ${largestMove.name} ${formatRatio(largestMove.changePct)}`);
  return pieces.length ? pieces.join(' · ') : '시장 지표가 아직 준비되지 않았습니다.';
}

function buildBriefingLine2(summary, nearest) {
  const next30 = computeCountsFromEvents(state.feed?.events || [], state.today, 30);
  const parts = [`30일 청약 ${formatNumber(next30.subscription || 0)}건`, `상장 ${formatNumber(next30.listing || 0)}건`, `보호예수 해제 ${formatNumber(next30.unlock || 0)}건`];
  if (nearest) parts.push(`가까운 일정 ${formatDateShort(nearest.date)} ${nearest.name}`);
  return parts.join(' · ');
}

function toDateKey(value) {
  const date = parseDate(value);
  if (!date) return '';
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function getCalendarMonthStart() {
  const base = state.today || startOfDay(new Date()) || new Date();
  return new Date(base.getFullYear(), base.getMonth() + (state.ui.calendarMonthOffset || 0), 1);
}

function renderCalendar() {
  const grid = el('#calendar-grid');
  const detail = el('#calendar-detail');
  const title = el('#calendar-title');
  if (!grid || !detail) return;
  const monthStart = getCalendarMonthStart();
  const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);
  if (title) title.textContent = `${monthStart.getFullYear()}년 ${monthStart.getMonth() + 1}월`;

  const rowsByDay = buildCalendarEntries(state.items, state.unlockRows, monthStart, monthEnd);
  const weekdays = ['월', '화', '수', '목', '금', '토', '일'];
  const cells = weekdays.map((name) => `<div class="calendar-weekday">${name}</div>`);
  const mondayFirst = (monthStart.getDay() + 6) % 7;
  for (let i = 0; i < mondayFirst; i += 1) cells.push('<div class="calendar-cell is-empty"></div>');

  const todayKey = toDateKey(state.today);
  let selectedKey = state.ui.calendarSelectedDate || '';
  const monthPrefix = `${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, '0')}`;
  if (!selectedKey || !selectedKey.startsWith(monthPrefix)) {
    selectedKey = todayKey.startsWith(monthPrefix) ? todayKey : '';
  }
  if (!selectedKey) selectedKey = Array.from(rowsByDay.keys()).sort()[0] || `${monthPrefix}-01`;
  state.ui.calendarSelectedDate = selectedKey;

  for (let day = 1; day <= monthEnd.getDate(); day += 1) {
    const dateKey = `${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const dayEvents = rowsByDay.get(dateKey) || [];
    const visible = dayEvents.slice(0, 2);
    const extra = Math.max(0, dayEvents.length - visible.length);
    const classes = ['calendar-cell'];
    if (dayEvents.length) classes.push('has-events');
    if (dateKey === todayKey) classes.push('is-today');
    if (dateKey === selectedKey) classes.push('is-selected');
    cells.push(`
      <button type="button" class="${classes.join(' ')}" data-calendar-date="${dateKey}">
        <div class="calendar-day">${day}</div>
        <div class="calendar-agenda">
          ${visible.map((event) => `<div class="calendar-agenda-item ${escapeHtml(event.type)}"><span class="calendar-agenda-name">${escapeHtml(event.name)}</span></div>`).join('')}
          ${extra ? `<div class="calendar-more">+${extra}건</div>` : ''}
        </div>
      </button>
    `);
  }
  grid.innerHTML = cells.join('');
  grid.querySelectorAll('[data-calendar-date]').forEach((button) => {
    button.addEventListener('click', () => {
      state.ui.calendarSelectedDate = button.dataset.calendarDate || '';
      renderCalendar();
    });
  });

  const selectedEvents = rowsByDay.get(selectedKey) || [];
  if (!selectedEvents.length) {
    detail.innerHTML = `<div class="empty-state">선택한 날짜에 일정이 없습니다.</div>`;
    return;
  }
  detail.innerHTML = `
    <div class="card-head">
      <h3>${escapeHtml(formatDateFull(selectedKey))} 일정</h3>
      <span class="card-note">${escapeHtml(String(selectedEvents.length))}건</span>
    </div>
    <div class="calendar-detail-grid">
      ${selectedEvents.map((event) => `
        <div class="calendar-detail-item ${escapeHtml(event.type)}">
          <div><strong>${escapeHtml(event.name || '')}</strong></div>
          <div class="meta">${escapeHtml(event.meta || '')}${event.market ? ` · ${escapeHtml(event.market)}` : ''}</div>
        </div>
      `).join('')}
    </div>
  `;
}


function renderMiniIssueCards(items, mode) {
  if (!items.length) return emptyState('표시할 종목이 없습니다.');
  return items.map((item) => {
    const meta = mode === 'subscription'
      ? `${item.subscriptionRange} · ${item.underwriterText}`
      : `${formatDateShort(item.listingDate)} · ${item.market}`;
    return `
      <div class="mini-card">
        <div class="title">${escapeHtml(item.market)} · ${escapeHtml(item.stage)}</div>
        <div class="headline">${escapeHtml(item.displayName)}</div>
        <div class="table-sub">${escapeHtml(meta)}</div>
      </div>
    `;
  }).join('');
}

function renderSubscriptions() {
  const listNode = el('#subscription-list');
  const metaNode = el('#subscription-meta');
  if (!listNode || !metaNode) return;
  const query = (el('#subscription-query')?.value || '').trim().toLowerCase();
  const market = el('#subscription-market')?.value || '';
  const group = el('#subscription-group')?.value || 'upcoming';
  let items = state.items.filter((item) => item.subscriptionStart);
  if (query) items = items.filter((item) => item.searchText.includes(query));
  if (market) items = items.filter((item) => item.market === market);
  if (group === 'upcoming') items = items.filter((item) => isUpcomingStage(item));
  else if (group === 'listed') items = items.filter((item) => isListedStage(item));
  items.sort((a, b) => compareByProximity(a, b, 'subscriptionStart', state.today));

  const perPage = state.ui.subscriptionPerPage || 8;
  const total = items.length;
  const pageCount = Math.max(1, Math.ceil(total / perPage));
  const currentPage = Math.min(Math.max(1, state.ui.subscriptionPage || 1), pageCount);
  state.ui.subscriptionPage = currentPage;
  const start = (currentPage - 1) * perPage;
  const pageItems = items.slice(start, start + perPage);

  metaNode.textContent = `${total}건 · ${currentPage}/${pageCount} 페이지`;
  listNode.innerHTML = pageItems.length ? pageItems.map(renderIssueCard).join('') : emptyState('조건에 맞는 청약 종목이 없습니다.');
  renderPager('subscription-pager', currentPage, pageCount, perPage, total, (page) => {
    state.ui.subscriptionPage = page;
    renderSubscriptions();
  });
}


function renderIssueCard(item) {
  const competition = item.institutionalCompetitionRatio ? formatCompetition(item.institutionalCompetitionRatio) : '-';
  const links = [];
  if (item.dartViewerUrl) links.push(`<a class="link-chip" href="${escapeHtml(item.dartViewerUrl)}" target="_blank" rel="noreferrer">증권신고서</a>`);
  if (item.irPdfUrl) links.push(`<a class="link-chip" href="${escapeHtml(item.irPdfUrl)}" target="_blank" rel="noreferrer">IR PDF</a>`);
  if (item.irUrl) links.push(`<a class="link-chip" href="${escapeHtml(item.irUrl)}" target="_blank" rel="noreferrer">IR 자료실</a>`);
  if (item.homepUrl) links.push(`<a class="link-chip" href="${escapeHtml(item.homepUrl)}" target="_blank" rel="noreferrer">홈페이지</a>`);
  const shareholderText = item.existingShareholderRatio ? formatRatio(item.existingShareholderRatio) : '-';
  const lockupText = item.lockupCommitmentRatio ? formatRatio(item.lockupCommitmentRatio) : '-';
  return `
    <article class="issue-card">
      <div class="issue-card-header">
        <div>
          <h3>${escapeHtml(item.displayName)}</h3>
          <div class="issue-code">${escapeHtml(item.symbol || '코드 미상')} · ${escapeHtml(item.market)}${item.sector ? ` · ${escapeHtml(item.sector)}` : ''}</div>
        </div>
        <div class="badge-row">
          <span class="badge">${escapeHtml(item.stage)}</span>
          <span class="badge">청약 ${escapeHtml(item.subscriptionRange)}</span>
        </div>
      </div>
      <dl>
        <div><dt>수요예측</dt><dd>${escapeHtml(item.forecastText)}</dd></div>
        <div><dt>주관사</dt><dd>${escapeHtml(item.underwriterText)}</dd></div>
        <div><dt>희망가</dt><dd>${escapeHtml(item.priceBandText)}</dd></div>
        <div><dt>공모가</dt><dd>${escapeHtml(formatPrice(item.offerPrice))}</dd></div>
        <div><dt>기관경쟁률</dt><dd>${escapeHtml(competition)}</dd></div>
        <div><dt>확약 / 기존주주</dt><dd>${escapeHtml(`${lockupText} / ${shareholderText}`)}</dd></div>
      </dl>
      ${links.length ? `<div class="link-row">${links.join('')}</div>` : ''}
    </article>
  `;
}

function renderListings() {
  const body = el('#listing-table tbody');
  const metaNode = el('#listing-meta');
  if (!body || !metaNode) return;
  const query = (el('#listing-query')?.value || '').trim().toLowerCase();
  const group = el('#listing-group')?.value || 'listed';
  let items = state.items.filter((item) => item.listingDate);
  if (query) items = items.filter((item) => item.searchText.includes(query));
  if (group === 'upcoming') items = items.filter((item) => parseDate(item.listingDate) && diffDays(parseDate(item.listingDate), state.today) >= 0 && !isDelistedStatus(item));
  else if (group === 'listed') items = items.filter((item) => !isDelistedStatus(item));
  else if (group === 'delisted') items = items.filter((item) => isDelistedStatus(item));
  items.sort((a, b) => compareByProximity(a, b, 'listingDate', state.today));

  const perPage = state.ui.listingPerPage || 8;
  const total = items.length;
  const pageCount = Math.max(1, Math.ceil(total / perPage));
  const currentPage = Math.min(Math.max(1, state.ui.listingPage || 1), pageCount);
  state.ui.listingPage = currentPage;
  const start = (currentPage - 1) * perPage;
  const pageItems = items.slice(start, start + perPage);

  metaNode.textContent = `${total}건 · ${currentPage}/${pageCount} 페이지`;
  body.innerHTML = pageItems.map((item) => {
    const returnClass = item.returnPct === null ? '' : item.returnPct >= 0 ? 'good' : 'bad';
    return `
      <tr>
        <td><span class="table-main">${escapeHtml(item.displayName)}</span><span class="table-sub">${escapeHtml(item.symbol || '')} · ${escapeHtml(item.listingStatus)}</span></td>
        <td>${escapeHtml(item.market)}</td>
        <td class="mono">${escapeHtml(formatDateShort(item.listingDate))}</td>
        <td class="mono">${escapeHtml(formatPrice(item.offerPrice))}</td>
        <td class="mono">${escapeHtml(formatPrice(item.currentPrice))}</td>
        <td class="mono"><span class="value ${returnClass}">${escapeHtml(formatRatio(item.returnPct))}</span></td>
        <td>${escapeHtml(formatCompetition(item.institutionalCompetitionRatio))}</td>
        <td>${escapeHtml(formatRatio(item.lockupCommitmentRatio))}</td>
        <td>${escapeHtml(item.signalText)}</td>
      </tr>
    `;
  }).join('') || `<tr><td colspan="9">조건에 맞는 상장 종목이 없습니다.</td></tr>`;

  renderPager('listing-pager', currentPage, pageCount, perPage, total, (page) => {
    state.ui.listingPage = page;
    renderListings();
  });
}



function renderPager(containerId, currentPage, pageCount, perPage, totalItems, onChange) {
  const container = el(`#${containerId}`);
  if (!container) return;
  if (pageCount <= 1) {
    container.innerHTML = '';
    return;
  }
  const pages = [];
  const start = Math.max(1, currentPage - 2);
  const end = Math.min(pageCount, start + 4);
  pages.push(`<button class="pager-button" data-page="${Math.max(1, currentPage - 1)}" ${currentPage === 1 ? 'disabled' : ''}>‹</button>`);
  for (let page = start; page <= end; page += 1) {
    pages.push(`<button class="pager-button ${page === currentPage ? 'active' : ''}" data-page="${page}">${page}</button>`);
  }
  pages.push(`<button class="pager-button" data-page="${Math.min(pageCount, currentPage + 1)}" ${currentPage === pageCount ? 'disabled' : ''}>›</button>`);
  pages.push(`<span class="pager-summary">${formatNumber(totalItems)}건 · ${perPage}개씩</span>`);
  container.innerHTML = pages.join('');
  container.querySelectorAll('button[data-page]').forEach((button) => {
    button.addEventListener('click', () => {
      const page = Number(button.dataset.page || currentPage);
      if (!Number.isFinite(page) || page === currentPage) return;
      onChange(page);
    });
  });
}

function renderUnlocks() {
  const body = el('#unlock-table tbody');
  const metaNode = el('#unlock-meta');
  if (!body || !metaNode) return;
  const query = (el('#unlock-query')?.value || '').trim().toLowerCase();
  const term = el('#unlock-term-filter')?.value || '';
  let rows = [...state.unlockRows];
  if (query) rows = rows.filter((row) => row.searchText.includes(query));
  if (term) rows = rows.filter((row) => row.term === term);
  rows.sort((a, b) => compareByProximity(a, b, 'date', state.today));

  const perPage = state.ui.unlockPerPage || 8;
  const total = rows.length;
  const pageCount = Math.max(1, Math.ceil(total / perPage));
  const currentPage = Math.min(Math.max(1, state.ui.unlockPage || 1), pageCount);
  state.ui.unlockPage = currentPage;
  const start = (currentPage - 1) * perPage;
  const pageRows = rows.slice(start, start + perPage);

  metaNode.textContent = `${total}건 · ${currentPage}/${pageCount} 페이지`;
  body.innerHTML = pageRows.map((row) => `
    <tr>
      <td><span class="table-main">${escapeHtml(row.name)}</span><span class="table-sub">${escapeHtml(row.symbol || '')}</span></td>
      <td>${escapeHtml(row.market)}</td>
      <td class="mono">${escapeHtml(formatDateShort(row.date))}</td>
      <td>${escapeHtml(termLabel(row.term))}</td>
      <td class="mono">${escapeHtml(formatNumber(row.shares))}</td>
      <td class="mono">${escapeHtml(formatRatio(row.ratio))}</td>
      <td class="mono">${escapeHtml(formatNumber(row.remainingLockedShares))}</td>
    </tr>
  `).join('') || `<tr><td colspan="7">조건에 맞는 보호예수 해제 일정이 없습니다.</td></tr>`;
  renderPager('unlock-pager', currentPage, pageCount, perPage, total, (page) => {
    state.ui.unlockPage = page;
    renderUnlocks();
  });
}


function termLabel(term) {
  const map = { '15d': '15일', '1m': '1개월', '3m': '3개월', '6m': '6개월', '1y': '1년' };
  return map[term] || term;
}

function renderExplorer() {
  if (!el('#explorer-table')) return;
}


function renderShorts() {
  return;
}


function buildShortsScript(item) {
  const marketTone = buildMarketTone();
  const points = buildInvestmentPoints(item);
  const lines = [
    `곧 일반 청약을 시작하는 기업은 ${item.displayName}입니다.`,
    `${item.market} 상장을 추진하고 있고, 대표 주관사는 ${item.underwriterText}입니다.`,
    `수요예측 일정은 ${item.forecastText === '-' ? '공개 전' : item.forecastText}, 일반 청약은 ${item.subscriptionRange}에 진행됩니다.`,
    `${item.priceBandText !== '-' ? `희망 공모가 밴드는 ${item.priceBandText}` : '희망 공모가 정보는 아직 비어 있습니다.'}${item.offerPrice ? `, 확정 공모가는 ${formatPrice(item.offerPrice)}입니다.` : '.'}`,
    `${item.institutionalCompetitionRatio ? `기관경쟁률은 ${formatCompetition(item.institutionalCompetitionRatio)} 수준이고,` : '기관 수요예측 결과는 아직 비어 있고,'} ${item.lockupCommitmentRatio ? `의무보유확약은 ${formatRatio(item.lockupCommitmentRatio)}입니다.` : '확약 비율은 아직 확인 전입니다.'}`,
    `투자 포인트는 ${points.join(', ')}입니다.`,
    marketTone,
    `청약 전에는 증권신고서와 IR 자료를 함께 확인하고, 유통 가능 물량과 기존주주 비율도 같이 체크해 두는 게 좋습니다.`
  ];
  return lines.join('\n');
}

function buildInvestmentPoints(item) {
  const points = [];
  if (item.sector) points.push(`${item.sector} 업종 흐름`);
  if (item.institutionalCompetitionRatio && item.institutionalCompetitionRatio >= 500) points.push('기관 수요 강도');
  if (item.lockupCommitmentRatio && item.lockupCommitmentRatio >= 10) points.push('확약 비율 안정감');
  if (item.existingShareholderRatio && item.existingShareholderRatio >= 50) points.push('기존주주 비중 점검');
  if (!points.length) points.push('공모가 밴드와 유통 가능 물량', '주관사 배정 방식', '상장 후 초기 수급');
  return points.slice(0, 3);
}

function buildMarketTone() {
  const quotes = state.feed?.marketQuotes || [];
  const futures = quotes.find((row) => /futures/i.test(row.name || ''));
  const nasdaq = quotes.find((row) => /nasdaq/i.test(row.name || ''));
  const target = futures || nasdaq;
  if (target && numberOrNull(target.changePct) !== null) {
    const move = numberOrNull(target.changePct);
    if (Math.abs(move) >= 2) return `시장 배경으로는 ${target.name}가 ${formatRatio(move)} 움직여 위험 선호 변화가 비교적 컸습니다.`;
    return `시장 배경으로는 ${target.name}가 ${formatRatio(move)} 움직이며 중립에 가까운 흐름이었습니다.`;
  }
  return '시장 배경은 중립으로 보고 개별 종목 수요와 유통 구조를 더 중요하게 보는 편이 좋겠습니다.';
}

function renderDataHealth() {
  if (!el('#warning-list')) return;
}


function emptyState(text) {
  return `<div class="empty-state">${escapeHtml(text)}</div>`;
}

document.addEventListener('DOMContentLoaded', init);
