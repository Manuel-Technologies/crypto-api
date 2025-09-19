const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

// Helper: fetch Binance prices
const fetchBinanceCoins = async () => {
  try {
    const { data } = await axios.get('https://api.binance.com/api/v3/ticker/24hr');
    return data.map((coin) => ({
      symbol: coin.symbol,
      price: parseFloat(coin.lastPrice),
      priceChangePercent: parseFloat(coin.priceChangePercent),
      exchange: 'Binance',
      listingTime: null
    }));
  } catch (err) {
    console.error('Binance fetch error:', err.message);
    return [];
  }
};

// Helper: fetch Bitget prices
const fetchBitgetCoins = async () => {
  try {
    const { data } = await axios.get('https://api.bitget.com/api/spot/v1/market/tickers');
    return data.data.map((coin) => ({
      symbol: coin.symbol,
      price: parseFloat(coin.last),
      priceChangePercent: parseFloat(coin.changeRate) * 100,
      exchange: 'Bitget',
      listingTime: coin.listTime ? new Date(coin.listTime).getTime() : null
    }));
  } catch (err) {
    console.error('Bitget fetch error:', err.message);
    return [];
  }
};

// Helper: categorize coins
const categorizeCoins = (coins) => {
  const gainers = [...coins].sort((a, b) => b.priceChangePercent - a.priceChangePercent).slice(0, 50);
  const losers = [...coins].sort((a, b) => a.priceChangePercent - b.priceChangePercent).slice(0, 50);
  const newlyListed = [...coins].filter(c => c.listingTime)
    .sort((a, b) => b.listingTime - a.listingTime)
    .slice(0, 50);
  return { gainers, losers, newlyListed };
};

// Route: fetch all coins and categories
app.get('/api/crypto', async (req, res) => {
  try {
    const [binanceCoins, bitgetCoins] = await Promise.all([fetchBinanceCoins(), fetchBitgetCoins()]);
    const allCoins = [...binanceCoins, ...bitgetCoins];
    const { gainers, losers, newlyListed } = categorizeCoins(allCoins);

    res.json({ allCoins, gainers, losers, newlyListed });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch crypto data', details: err.message });
  }
});

// Route: fetch candlesticks
app.get('/api/candles/:symbol/:interval', async (req, res) => {
  const { symbol, interval } = req.params;
  const validIntervals = ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M'];

  if (!validIntervals.includes(interval)) {
    return res.status(400).json({ error: 'Invalid interval' });
  }

  try {
    const { data } = await axios.get('https://api.binance.com/api/v3/klines', {
      params: { symbol, interval, limit: 500 }
    });
    const candles = data.map(c => ({
      openTime: c[0],
      open: parseFloat(c[1]),
      high: parseFloat(c[2]),
      low: parseFloat(c[3]),
      close: parseFloat(c[4]),
      volume: parseFloat(c[5]),
      closeTime: c[6]
    }));
    res.json(candles);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch candlestick data', details: err.message });
  }
});

// Health check
app.get('/', (req, res) => res.send('Crypto API is running 🚀'));

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));