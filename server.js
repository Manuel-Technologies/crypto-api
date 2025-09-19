const express = require('express');
const axios = require('axios');
const cors = require('cors');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 5000;

// -------------------------
// In-memory storage
// -------------------------
let coinsData = [];
let gainers = [];
let losers = [];
let newlyListed = [];

// -------------------------
// Fetch Binance tickers
// -------------------------
async function fetchBinanceTickers(retry = 3) {
  try {
    const { data } = await axios.get('https://api.binance.com/api/v3/ticker/24hr');
    console.log(`Fetched ${data.length} Binance tickers`);
    return data.map(c => ({
      symbol: c.symbol,
      price: parseFloat(c.lastPrice),
      priceChangePercent: parseFloat(c.priceChangePercent),
      exchange: 'Binance',
      listingTime: Date.now() - 1000 * 60 * 60 * 24 * 30 // placeholder: last 30 days
    }));
  } catch (err) {
    console.error('Binance fetch error:', err.message);
    if (retry > 0) return fetchBinanceTickers(retry - 1);
    return [];
  }
}

// -------------------------
// Fetch Bitget tickers
// -------------------------
async function fetchBitgetTickers(retry = 3) {
  try {
    const { data: bitgetRaw } = await axios.get('https://api.bitget.com/api/spot/v1/market/tickers');
    if (!bitgetRaw.data || !Array.isArray(bitgetRaw.data)) return [];
    console.log(`Fetched ${bitgetRaw.data.length} Bitget tickers`);

    return bitgetRaw.data
      .filter(c => c.last && c.changeRate)
      .map(c => ({
        symbol: c.symbol.replace('_', '').toUpperCase(),
        price: parseFloat(c.last),
        priceChangePercent: parseFloat(c.changeRate) * 100,
        exchange: 'Bitget',
        listingTime: Date.now() - 1000 * 60 * 60 * 24 * 30
      }));
  } catch (err) {
    console.error('Bitget fetch error:', err.message);
    if (retry > 0) return fetchBitgetTickers(retry - 1);
    return [];
  }
}

// -------------------------
// Update categories
// -------------------------
function updateCategories() {
  if (!coinsData.length) return;
  gainers = [...coinsData].sort((a, b) => (b.priceChangePercent || 0) - (a.priceChangePercent || 0)).slice(0, 50);
  losers = [...coinsData].sort((a, b) => (a.priceChangePercent || 0) - (b.priceChangePercent || 0)).slice(0, 50);

  const now = Date.now();
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  newlyListed = coinsData.filter(c => c.listingTime && now - c.listingTime < sevenDays);
}

// -------------------------
// Initialize coinsData
// -------------------------
async function initializeCoins() {
  const [binance, bitget] = await Promise.all([fetchBinanceTickers(), fetchBitgetTickers()]);
  coinsData = [...binance, ...bitget];
  console.log(`Total coins loaded: ${coinsData.length}`);
  updateCategories();
}

// -------------------------
// Binance WebSocket
// -------------------------
function connectBinanceWS() {
  const ws = new WebSocket('wss://stream.binance.com:9443/ws/!ticker@arr');

  ws.on('message', message => {
    const tickers = JSON.parse(message);
    tickers.forEach(t => {
      const coin = coinsData.find(c => c.symbol === t.s && c.exchange === 'Binance');
      if (coin) {
        coin.price = parseFloat(t.c) || coin.price;
        coin.priceChangePercent = parseFloat(t.P) || coin.priceChangePercent;
      }
    });
    updateCategories();
  });

  ws.on('close', () => {
    console.log('Binance WS closed. Reconnecting in 5s...');
    setTimeout(connectBinanceWS, 5000);
  });

  ws.on('error', err => {
    console.error('Binance WS error:', err.message);
    ws.close();
  });
}

// -------------------------
// Bitget WebSocket
// -------------------------
function connectBitgetWS() {
  const ws = new WebSocket('wss://ws.bitget.com/spot/v1/stream');

  ws.on('open', () => {
    ws.send(JSON.stringify({
      op: 'subscribe',
      args: [{ channel: 'tickers', instType: 'SPOT' }]
    }));
  });

  ws.on('message', message => {
    const msg = JSON.parse(message);
    if (msg.arg && msg.arg.channel === 'tickers' && msg.data) {
      msg.data.forEach(t => {
        const coin = coinsData.find(c => c.symbol === t.instId.replace('_','').toUpperCase() && c.exchange === 'Bitget');
        if (coin) {
          coin.price = parseFloat(t.last) || coin.price;
          coin.priceChangePercent = parseFloat(t.changeRate) * 100 || coin.priceChangePercent;
        }
      });
      updateCategories();
    }
  });

  ws.on('close', () => {
    console.log('Bitget WS closed. Reconnecting in 5s...');
    setTimeout(connectBitgetWS, 5000);
  });

  ws.on('error', err => {
    console.error('Bitget WS error:', err.message);
    ws.close();
  });
}

// -------------------------
// API Endpoints
// -------------------------

// Main endpoint
app.get('/api/crypto', (req, res) => {
  res.json({ allCoins: coinsData, gainers, losers, newlyListed });
});

// Candlestick endpoint
app.get('/api/candles/:symbol/:interval', async (req, res) => {
  const { symbol, interval } = req.params;
  try {
    // Try Binance
    const { data } = await axios.get(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=100`);
    return res.json({ exchange: 'Binance', data });
  } catch {
    // Fallback Bitget
    try {
      const { data } = await axios.get(`https://api.bitget.com/api/spot/v1/market/candles?symbol=${symbol}&period=${interval}&limit=100`);
      return res.json({ exchange: 'Bitget', data: data.data });
    } catch (err) {
      return res.status(500).json({ error: 'Failed to fetch candlestick data', details: err.message });
    }
  }
});

// Health check
app.get('/', (req, res) => res.send('Crypto API is running 🚀'));

// -------------------------
// Start server
// -------------------------
app.listen(PORT, async () => {
  console.log(`Crypto API running on port ${PORT}`);
  await initializeCoins();
  connectBinanceWS();
  connectBitgetWS();
});