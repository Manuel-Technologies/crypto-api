// server.js
'use strict';

require('dotenv').config();

const express = require('express');
const axios = require('axios');
const NodeCache = require('node-cache');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const morgan = require('morgan');
const cors = require('cors');

const app = express();

const PORT = process.env.PORT || 3000;
const CACHE_TTL = Number(process.env.CACHE_TTL || 15); // seconds
const AXIOS_TIMEOUT = Number(process.env.AXIOS_TIMEOUT || 8000); // ms
const MAX_KLINES_LIMIT = 1000;
const DEFAULT_KLINES_LIMIT = 500;
const MAX_TOP = Number(process.env.MAX_TOP || 50);

// Create a shared axios instance
const http = axios.create({
  timeout: AXIOS_TIMEOUT,
  headers: {
    'User-Agent': `Crypto-API/1.0`,
    Accept: 'application/json'
  }
});

// Basic middleware
const corsOrigins = process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : true;
app.use(cors({ origin: corsOrigins }));
app.use(express.json());
app.use(helmet());
app.use(morgan('combined'));

// Rate limiter (tune to your needs)
app.use(rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later.' }
}));

// Simple in-memory cache
const cache = new NodeCache({ stdTTL: CACHE_TTL, checkperiod: Math.max(1, Math.floor(CACHE_TTL * 0.2)) });

// Utility: safe number parse
const safeNum = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// Fetch Binance tickers
const fetchBinanceCoins = async () => {
  try {
    const res = await http.get('https://api.binance.com/api/v3/ticker/24hr');
    const payload = res && res.data;
    if (!Array.isArray(payload)) return [];

    return payload.map(item => ({
      symbol: item.symbol,
      price: safeNum(item.lastPrice),
      priceChangePercent: safeNum(item.priceChangePercent), // already percent on Binance
      exchange: 'Binance',
      listingTime: null // Binance 24hr ticker doesn't provide listing time
    }));
  } catch (err) {
    console.error('fetchBinanceCoins error:', err.message || err.toString());
    return [];
  }
};

// Fetch Bitget tickers (defensive parsing)
const fetchBitgetCoins = async () => {
  try {
    const res = await http.get('https://api.bitget.com/api/spot/v1/market/tickers');
    const payload = res && res.data;
    // Bitget usually replies { code: ..., data: [...] } but we'll be defensive
    const rows = (payload && (payload.data || payload.tickers || payload)) || [];
    if (!Array.isArray(rows)) return [];

    return rows.map(item => {
      // possible fields: item.last, item.changeRate, item.listTime, item.listingTime, item.createdAt
      const price = safeNum(item.last ?? item.price ?? item.close ?? item.p);
      const changeRaw = item.changeRate ?? item.priceChangePercent ?? item.change ?? null;
      const changePercent = changeRaw != null ? safeNum(changeRaw) * (Math.abs(changeRaw) < 3 ? 100 : 1) : null;
      // the multiplier attempt above tries to convert 0.012 -> 1.2% if bitget uses fractional changeRate.
      const listingTime = item.listTime || item.listingTime || item.createdAt || null;
      const listingTimeMs = listingTime ? safeNum(listingTime) : null;

      return {
        symbol: item.symbol,
        price,
        priceChangePercent: changePercent,
        exchange: 'Bitget',
        listingTime: listingTimeMs
      };
    });
  } catch (err) {
    console.error('fetchBitgetCoins error:', err.message || err.toString());
    return [];
  }
};

// Merge / aggregate coins by symbol
const aggregateBySymbol = (coins) => {
  const map = new Map();

  for (const c of coins) {
    if (!c || !c.symbol) continue;
    const s = c.symbol.toUpperCase();
    if (!map.has(s)) map.set(s, { symbol: s, prices: [], exchanges: new Set(), listingTimes: [] });
    const entry = map.get(s);
    entry.prices.push({ price: c.price, change: c.priceChangePercent, exchange: c.exchange });
    entry.exchanges.add(c.exchange);
    if (c.listingTime) entry.listingTimes.push(c.listingTime);
  }

  const out = [];
  for (const [symbol, e] of map) {
    // prefer Binance price if available, else first non-null
    let preferred = e.prices.find(p => p.exchange === 'Binance' && p.price != null)
      || e.prices.find(p => p.price != null)
      || e.prices[0] || { price: null, change: null };

    const changes = e.prices.map(p => p.change).filter(Number.isFinite);
    const avgChange = changes.length ? (changes.reduce((a, b) => a + b, 0) / changes.length) : null;

    out.push({
      symbol,
      price: safeNum(preferred.price),
      priceChangePercent: safeNum(avgChange),
      exchanges: Array.from(e.exchanges),
      listingTime: e.listingTimes.length ? Math.max(...e.listingTimes.filter(Boolean)) : null
    });
  }

  return out;
};

// Categorize helper (returns top N gainers, losers, newlyListed)
const categorize = (coins, topN = MAX_TOP) => {
  const finiteChange = coins.filter(c => Number.isFinite(Number(c.priceChangePercent)));
  const gainers = [...finiteChange].sort((a, b) => b.priceChangePercent - a.priceChangePercent).slice(0, topN);
  const losers = [...finiteChange].sort((a, b) => a.priceChangePercent - b.priceChangePercent).slice(0, topN);
  const newlyListed = [...coins].filter(c => c.listingTime).sort((a, b) => b.listingTime - a.listingTime).slice(0, topN);

  return { gainers, losers, newlyListed };
};

// /api/crypto - combined endpoint
app.get('/api/crypto', async (req, res) => {
  try {
    const cacheKey = 'tickers_all_v1';
    if (cache.has(cacheKey)) {
      const cached = cache.get(cacheKey);
      return res.json({ ...cached, cached: true });
    }

    // fetch concurrently
    const [binance, bitget] = await Promise.all([fetchBinanceCoins(), fetchBitgetCoins()]);
    const allCoins = [...binance, ...bitget];

    // aggregated by symbol
    const aggregated = aggregateBySymbol(allCoins);

    // create categories (use aggregated by default)
    const top = Number(req.query.top) || MAX_TOP;
    const { gainers, losers, newlyListed } = categorize(aggregated, Math.min(top, 1000));

    const payload = {
      updatedAt: new Date().toISOString(),
      meta: { countPerExchange: { Binance: binance.length, Bitget: bitget.length }, aggregatedCount: aggregated.length },
      allCoins,         // raw per-exchange array
      aggregated,       // deduplicated, merged by symbol
      gainers,
      losers,
      newlyListed
    };

    cache.set(cacheKey, payload);
    return res.json({ ...payload, cached: false });
  } catch (err) {
    console.error('/api/crypto error:', err);
    return res.status(500).json({ error: 'Failed to fetch crypto data', details: String(err.message || err) });
  }
});

// /api/candles/:symbol/:interval
app.get('/api/candles/:symbol/:interval', async (req, res) => {
  try {
    const rawSymbol = String(req.params.symbol || '');
    const symbol = rawSymbol.toUpperCase().trim();
    const interval = String(req.params.interval || '').trim();

    const allowed = new Set(['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M']);
    if (!allowed.has(interval)) {
      return res.status(400).json({ error: 'Invalid interval', allowed: Array.from(allowed) });
    }

    // limit param
    const qLimit = Number(req.query.limit || DEFAULT_KLINES_LIMIT);
    const limit = Number.isInteger(qLimit) ? Math.min(Math.max(1, qLimit), MAX_KLINES_LIMIT) : DEFAULT_KLINES_LIMIT;

    // simple symbol validation (alphanum and - _)
    if (!/^[A-Z0-9_/:-]+$/.test(symbol)) {
      return res.status(400).json({ error: 'Invalid symbol format' });
    }

    // call Binance
    try {
      const binanceResp = await http.get('https://api.binance.com/api/v3/klines', {
        params: { symbol, interval, limit }
      });

      const raw = binanceResp.data;
      if (!Array.isArray(raw)) return res.status(502).json({ error: 'Unexpected klines response' });

      const candles = raw.map(c => ({
        openTime: c[0],
        open: safeNum(c[1]),
        high: safeNum(c[2]),
        low: safeNum(c[3]),
        close: safeNum(c[4]),
        volume: safeNum(c[5]),
        closeTime: c[6],
        quoteAssetVolume: safeNum(c[7]),
        trades: Number.isFinite(Number(c[8])) ? Number(c[8]) : null,
        takerBuyBaseAssetVolume: safeNum(c[9]),
        takerBuyQuoteAssetVolume: safeNum(c[10])
      }));

      return res.json({ symbol, interval, limit, count: candles.length, candles });
    } catch (err) {
      // surface Binance errors clearly
      const status = err.response?.status || 500;
      const msg = err.response?.data || err.message || 'binance request failed';
      return res.status(status).json({ error: 'Failed to fetch candlestick data', details: msg });
    }
  } catch (err) {
    console.error('/api/candles error:', err);
    return res.status(500).json({ error: 'Internal server error', details: String(err.message || err) });
  }
});

// Health
app.get('/', (req, res) => res.send('Crypto API is running 🚀'));

// Start
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT} — PID:${process.pid}`);
});