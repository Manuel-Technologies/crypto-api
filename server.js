'use strict';

/**
 * production-ready crypto aggregator
 * - fetches Binance and Bitget tickers
 * - provides aggregated endpoints and per-symbol search
 * - caching (in-memory or optional Redis)
 * - retries, timeouts, logging, rate-limiting, metrics
 */

require('dotenv').config();

const express = require('express');
const axios = require('axios');
const axiosRetry = require('axios-retry');
const helmet = require('helmet');
const morgan = require('morgan');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const NodeCache = require('node-cache');
const Redis = require('ioredis');
const Joi = require('joi');
const winston = require('winston');
const promClient = require('prom-client');

const app = express();

// --- Configuration (use env or defaults) ---
const PORT = Number(process.env.PORT || 3000);
const CACHE_TTL = Number(process.env.CACHE_TTL || 15); // seconds
const AXIOS_TIMEOUT = Number(process.env.AXIOS_TIMEOUT || 8000); // ms
const MAX_KLINES_LIMIT = Number(process.env.MAX_KLINES_LIMIT || 1000);
const DEFAULT_KLINES_LIMIT = Number(process.env.DEFAULT_KLINES_LIMIT || 500);
const MAX_TOP = Number(process.env.MAX_TOP || 50);
const REDIS_URL = process.env.REDIS_URL || null;
const RATE_LIMIT_WINDOW = Number(process.env.RATE_LIMIT_WINDOW_MS || 60 * 1000); // 1min
const RATE_LIMIT_MAX = Number(process.env.RATE_LIMIT_MAX || 120);

// --- Logger (winston) ---
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const m = Object.keys(meta).length ? JSON.stringify(meta) : '';
      return `${timestamp} ${level.toUpperCase()}: ${message} ${m}`;
    })
  ),
  transports: [new winston.transports.Console()]
});

// --- HTTP client ---
const http = axios.create({
  timeout: AXIOS_TIMEOUT,
  headers: { 'User-Agent': `crypto-aggregator/1.0` }
});
// retries for transient network errors & 429
axiosRetry(http, {
  retries: 2,
  retryDelay: axiosRetry.exponentialDelay,
  retryCondition: (error) => {
    // retry on network errors or 429 or 5xx
    return axiosRetry.isNetworkOrIdempotentRequestError(error) || error.response?.status === 429;
  }
});

// --- Cache (NodeCache) and optional Redis ---
const memCache = new NodeCache({ stdTTL: CACHE_TTL, checkperiod: Math.max(1, Math.floor(CACHE_TTL * 0.2)) });
let redisClient = null;
let usingRedis = false;

if (REDIS_URL) {
  redisClient = new Redis(REDIS_URL);
  redisClient.on('error', err => logger.warn('Redis error', err));
  redisClient.on('connect', () => logger.info('Connected to Redis'));
  usingRedis = true;
}

// caching helpers
const cacheKey = (k) => `crypto:${k}`;
const cacheSet = async (k, v, ttl = CACHE_TTL) => {
  try {
    memCache.set(k, v, ttl);
    if (usingRedis) await redisClient.setex(cacheKey(k), ttl, JSON.stringify(v));
  } catch (err) {
    logger.warn('cacheSet error', err.message);
  }
};
const cacheGet = async (k) => {
  try {
    const mem = memCache.get(k);
    if (mem) return mem;
    if (usingRedis) {
      const val = await redisClient.get(cacheKey(k));
      if (val) {
        const parsed = JSON.parse(val);
        memCache.set(k, parsed, CACHE_TTL);
        return parsed;
      }
    }
    return null;
  } catch (err) {
    logger.warn('cacheGet error', err.message);
    return null;
  }
};

// --- Prometheus metrics ---
const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();
const requestCounter = new promClient.Counter({ name: 'http_requests_total', help: 'Total HTTP requests' });
const fetchCounter = new promClient.Counter({ name: 'external_fetch_requests_total', help: 'External fetch requests' });
const fetchFailures = new promClient.Counter({ name: 'external_fetch_failures_total', help: 'External fetch failures' });

// --- Middleware ---
app.use(helmet());
app.use(compression());
app.use(express.json());
app.use(morgan('combined', { stream: { write: (msg) => logger.info(msg.trim()) } }));

app.use(rateLimit({
  windowMs: RATE_LIMIT_WINDOW,
  max: RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests' }
}));

// --- Input validation schemas ---
const symbolSchema = Joi.string().pattern(/^[A-Za-z0-9_.-]{3,30}$/).required();
const intervalSchema = Joi.string().valid('1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M');

// --- Utility: safe numeric parse ---
const safeNum = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// --- Exchange fetchers (defensive) ---

async function fetchBinanceTickers() {
  fetchCounter.inc();
  try {
    const res = await http.get('https://api.binance.com/api/v3/ticker/24hr');
    if (!Array.isArray(res.data)) throw new Error('Unexpected Binance response');
    return res.data.map(item => ({
      symbol: item.symbol,
      price: safeNum(item.lastPrice ?? item.weightedAvgPrice ?? item.price),
      priceChangePercent: safeNum(item.priceChangePercent),
      exchange: 'Binance',
      listingTime: null
    }));
  } catch (err) {
    fetchFailures.inc();
    logger.warn('Binance fetch failed', err.message);
    return { error: String(err.message), data: [] };
  }
}

async function fetchBitgetTickers() {
  fetchCounter.inc();
  try {
    const res = await http.get('https://api.bitget.com/api/spot/v1/market/tickers');
    // defensive: Bitget responds { code, data: [...] } sometimes
    const payload = res.data?.data ?? res.data?.tickers ?? res.data;
    if (!Array.isArray(payload)) throw new Error('Unexpected Bitget response');
    return payload.map(item => {
      // changeRate is often fractional (0.0123) so convert to percent when needed
      const rawChange = item.changeRate ?? item.priceChangePercent ?? item.change;
      let changePercent = safeNum(rawChange);
      if (changePercent !== null && Math.abs(changePercent) < 3) changePercent = changePercent * 100;
      return {
        symbol: item.symbol,
        price: safeNum(item.last ?? item.close ?? item.price),
        priceChangePercent: changePercent,
        exchange: 'Bitget',
        listingTime: item.listTime ? safeNum(item.listTime) : null
      };
    });
  } catch (err) {
    fetchFailures.inc();
    logger.warn('Bitget fetch failed', err.message);
    return { error: String(err.message), data: [] };
  }
}

// fetch single symbol directly from exchanges (prefer exchange-specific endpoints when available)
async function fetchSymbolFromBinance(symbol) {
  fetchCounter.inc();
  try {
    // use /ticker/price and /ticker/24hr for more stable single-symbol endpoints
    const [p, s] = await Promise.all([
      http.get('https://api.binance.com/api/v3/ticker/price', { params: { symbol } }),
      http.get('https://api.binance.com/api/v3/ticker/24hr', { params: { symbol } })
    ]);
    const price = safeNum(p.data?.price ?? s.data?.lastPrice);
    const change = safeNum(s.data?.priceChangePercent);
    return { symbol, price, priceChangePercent: change, exchange: 'Binance', listingTime: null };
  } catch (err) {
    fetchFailures.inc();
    logger.debug('fetchSymbolFromBinance fail', { symbol, err: err.message });
    return null;
  }
}

async function fetchSymbolFromBitget(symbol) {
  fetchCounter.inc();
  try {
    // Bitget doesn't expose a neat single-symbol public query in same shape, so fetch tickers and filter
    const res = await http.get('https://api.bitget.com/api/spot/v1/market/tickers');
    const payload = res.data?.data ?? res.data;
    if (!Array.isArray(payload)) return null;
    const item = payload.find(i => String(i.symbol).toUpperCase() === String(symbol).toUpperCase());
    if (!item) return null;
    const rawChange = item.changeRate ?? item.priceChangePercent ?? item.change;
    let changePercent = safeNum(rawChange);
    if (changePercent !== null && Math.abs(changePercent) < 3) changePercent = changePercent * 100;
    return {
      symbol: item.symbol,
      price: safeNum(item.last ?? item.close ?? item.price),
      priceChangePercent: changePercent,
      exchange: 'Bitget',
      listingTime: item.listTime ? safeNum(item.listTime) : null
    };
  } catch (err) {
    fetchFailures.inc();
    logger.debug('fetchSymbolFromBitget fail', { symbol, err: err.message });
    return null;
  }
}

// --- Aggregation helpers ---
function aggregateBySymbol(rows) {
  const map = new Map();
  for (const c of rows) {
    if (!c || !c.symbol) continue;
    const s = String(c.symbol).toUpperCase();
    if (!map.has(s)) map.set(s, { symbol: s, prices: [], exchanges: new Set(), listingTimes: [] });
    const e = map.get(s);
    e.prices.push({ price: c.price, change: c.priceChangePercent, exchange: c.exchange });
    if (c.listingTime) e.listingTimes.push(c.listingTime);
    if (c.exchange) e.exchanges.add(c.exchange);
  }
  const out = [];
  for (const [symbol, item] of map.entries()) {
    const preferred = item.prices.find(p => p.exchange === 'Binance' && p.price != null)
      || item.prices.find(p => p.price != null)
      || { price: null, change: null };
    const changes = item.prices.map(p => p.change).filter(Number.isFinite);
    const avgChange = changes.length ? (changes.reduce((a, b) => a + b, 0) / changes.length) : null;
    out.push({
      symbol,
      price: safeNum(preferred.price),
      priceChangePercent: safeNum(avgChange),
      exchanges: Array.from(item.exchanges),
      listingTime: item.listingTimes.length ? Math.max(...item.listingTimes.filter(Boolean)) : null
    });
  }
  return out;
}

function categorize(coins, topN = MAX_TOP) {
  const finite = coins.filter(c => Number.isFinite(Number(c.priceChangePercent)));
  const gainers = [...finite].sort((a, b) => b.priceChangePercent - a.priceChangePercent).slice(0, topN);
  const losers = [...finite].sort((a, b) => a.priceChangePercent - b.priceChangePercent).slice(0, topN);
  const newlyListed = [...coins].filter(c => c.listingTime).sort((a,b) => b.listingTime - a.listingTime).slice(0, topN);
  return { gainers, losers, newlyListed };
}

// --- Core: fetch and cache tickers (parallel, resilient) ---
async function fetchAndCacheTickers(force = false) {
  const key = 'tickers_v1';
  if (!force) {
    const cached = await cacheGet(key);
    if (cached) {
      logger.debug('Serving tickers from cache');
      return cached;
    }
  }

  logger.info('Refreshing tickers from exchanges');
  const [binRes, bitRes] = await Promise.allSettled([fetchBinanceTickers(), fetchBitgetTickers()]);

  const sources = {
    binance: { ok: binRes.status === 'fulfilled' && Array.isArray(binRes.value), error: binRes.status === 'rejected' ? binRes.reason : (binRes.value?.error ?? null) },
    bitget: { ok: bitRes.status === 'fulfilled' && Array.isArray(bitRes.value), error: bitRes.status === 'rejected' ? bitRes.reason : (bitRes.value?.error ?? null) }
  };

  const binData = (binRes.status === 'fulfilled' && Array.isArray(binRes.value)) ? binRes.value : (Array.isArray(binRes.value?.data) ? binRes.value.data : []);
  const bitData = (bitRes.status === 'fulfilled' && Array.isArray(bitRes.value)) ? bitRes.value : (Array.isArray(bitRes.value?.data) ? bitRes.value.data : []);

  const allCoins = [...binData, ...bitData].filter(Boolean);

  const aggregated = aggregateBySymbol(allCoins);
  const { gainers, losers, newlyListed } = categorize(aggregated, MAX_TOP);

  const payload = {
    updatedAt: new Date().toISOString(),
    meta: {
      countPerExchange: { Binance: binData.length, Bitget: bitData.length },
      aggregatedCount: aggregated.length
    },
    sources,
    allCoins,
    aggregated,
    gainers,
    losers,
    newlyListed
  };

  await cacheSet(key, payload, CACHE_TTL);
  return payload;
}

// --- Routes ---

// Prometheus metrics
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', promClient.register.contentType);
    res.end(await promClient.register.metrics());
  } catch (err) {
    res.status(500).send(err.message);
  }
});

// Health
app.get('/health', (req, res) => {
  requestCounter.inc();
  res.json({ ok: true, time: new Date().toISOString(), uptime: process.uptime() });
});

// Meta
app.get('/meta', (req, res) => {
  res.json({ exchanges: ['Binance', 'Bitget'], endpoints: ['/api/crypto', '/api/crypto/search/:symbol', '/api/candles/:symbol/:interval'] });
});

// Main aggregated endpoint
app.get('/api/crypto', async (req, res) => {
  requestCounter.inc();
  try {
    const top = Math.min(Number(req.query.top) || MAX_TOP, 1000);
    const data = await fetchAndCacheTickers();
    // allow category query
    const category = String(req.query.category || '').toLowerCase();
    if (category === 'gainers') return res.json({ ...data, cached: true, gainers: data.gainers.slice(0, top) });
    if (category === 'losers') return res.json({ ...data, cached: true, losers: data.losers.slice(0, top) });
    res.json({ ...data, cached: true });
  } catch (err) {
    logger.error('/api/crypto error', err);
    res.status(500).json({ error: 'Failed to fetch crypto', details: String(err.message) });
  }
});

// Search-on-demand: check cache then query exchanges directly if missing
app.get('/api/crypto/search/:symbol', async (req, res) => {
  requestCounter.inc();
  const { symbol } = req.params;
  const { error: sErr } = symbolSchema.validate(symbol);
  if (sErr) return res.status(400).json({ error: 'Invalid symbol' });

  try {
    const cache = await cacheGet('tickers_v1');
    if (cache) {
      const found = cache.aggregated.find(c => c.symbol === symbol.toUpperCase()) || cache.allCoins.find(c => String(c.symbol).toUpperCase() === symbol.toUpperCase());
      if (found) return res.json({ source: 'cache', result: found });
    }

    // if not in cache, try direct per-exchange lookup in parallel
    const [b, g] = await Promise.allSettled([fetchSymbolFromBinance(symbol), fetchSymbolFromBitget(symbol)]);
    const candidates = [];
    if (b.status === 'fulfilled' && b.value) candidates.push(b.value);
    if (g.status === 'fulfilled' && g.value) candidates.push(g.value);

    if (candidates.length === 0) {
      return res.status(404).json({ error: 'Symbol not found on Binance or Bitget' });
    }

    // prefer Binance result if present
    const pref = candidates.find(c => c.exchange === 'Binance') || candidates[0];
    // update cache opportunistically
    try {
      const tickers = await cacheGet('tickers_v1') || { allCoins: [], aggregated: [] };
      tickers.allCoins = tickers.allCoins || [];
      tickers.allCoins.push(pref);
      tickers.aggregated = aggregateBySymbol(tickers.allCoins);
      await cacheSet('tickers_v1', tickers, CACHE_TTL);
    } catch (err) {
      logger.debug('Failed to opportunistically update cache', err.message);
    }

    res.json({ source: 'direct', result: pref });
  } catch (err) {
    logger.error('/api/crypto/search error', err);
    res.status(500).json({ error: 'Search failed', details: String(err.message) });
  }
});

// Candlesticks (Binance)
app.get('/api/candles/:symbol/:interval', async (req, res) => {
  requestCounter.inc();
  const symbol = String(req.params.symbol || '').toUpperCase();
  const interval = String(req.params.interval || '');
  const { error: sErr } = symbolSchema.validate(symbol);
  const { error: iErr } = intervalSchema.validate(interval);

  if (sErr || iErr) return res.status(400).json({ error: 'Invalid symbol or interval' });

  const qLimit = Number(req.query.limit || DEFAULT_KLINES_LIMIT);
  const limit = Number.isInteger(qLimit) ? Math.min(Math.max(1, qLimit), MAX_KLINES_LIMIT) : DEFAULT_KLINES_LIMIT;

  try {
    // use Binance klines
    const resp = await http.get('https://api.binance.com/api/v3/klines', { params: { symbol, interval, limit } });
    if (!Array.isArray(resp.data)) return res.status(502).json({ error: 'Unexpected Binance response' });

    const candles = resp.data.map(c => ({
      openTime: c[0],
      open: safeNum(c[1]),
      high: safeNum(c[2]),
      low: safeNum(c[3]),
      close: safeNum(c[4]),
      volume: safeNum(c[5]),
      closeTime: c[6]
    }));
    res.json({ symbol, interval, count: candles.length, candles });
  } catch (err) {
    logger.warn('Candles fetch failed', err.message);
    const status = err.response?.status || 500;
    res.status(status).json({ error: 'Failed to fetch candlestick data', details: err.message || err.toString() });
  }
});

// Root
app.get('/', (req, res) => {
  res.send('Crypto Aggregator API — healthy');
});

// Fallback error handler
app.use((err, req, res, next) => {
  logger.error('Unhandled error', err);
  res.status(500).json({ error: 'Internal Server Error' });
});

// Start
const server = app.listen(PORT, () => {
  logger.info(`Server listening on port ${PORT}`);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down');
  server.close(() => process.exit(0));
});
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down');
  server.close(() => process.exit(0));
});