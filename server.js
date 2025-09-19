const express = require('express');
const axios = require('axios');
const cors = require('cors');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 5000;

// In-memory storage for real-time prices
let coinsData = [];
let gainers = [];
let losers = [];
let newlyListed = [];

// -------------------------
// Utility: fetch initial tickers from Binance & Bitget
// -------------------------
async function fetchInitialTickers() {
    try {
        // Binance
        const { data: binanceData } = await axios.get('https://api.binance.com/api/v3/ticker/24hr');
        const binance = binanceData.map(c => ({
            symbol: c.symbol,
            price: parseFloat(c.lastPrice),
            priceChangePercent: parseFloat(c.priceChangePercent),
            exchange: 'Binance',
            listingTime: Date.now() - 1000 * 60 * 60 * 24 * 30
        }));

        // Bitget
        const { data: bitgetRaw } = await axios.get('https://api.bitget.com/api/spot/v1/market/tickers');
        const bitget = bitgetRaw.data.map(c => ({
            symbol: c.symbol.replace('_', '').toUpperCase(),
            price: parseFloat(c.last),
            priceChangePercent: parseFloat(c.changeRate) * 100,
            exchange: 'Bitget',
            listingTime: Date.now() - 1000 * 60 * 60 * 24 * 30
        }));

        coinsData = [...binance, ...bitget];
        updateCategories();
    } catch (err) {
        console.error('Initial fetch error:', err.message);
    }
}

// -------------------------
// Update gainers, losers, newly listed
// -------------------------
function updateCategories() {
    gainers = [...coinsData].sort((a, b) => b.priceChangePercent - a.priceChangePercent).slice(0, 50);
    losers = [...coinsData].sort((a, b) => a.priceChangePercent - b.priceChangePercent).slice(0, 50);

    const now = Date.now();
    const sevenDays = 7 * 24 * 60 * 60 * 1000;
    newlyListed = coinsData.filter(c => now - c.listingTime < sevenDays);
}

// -------------------------
// WebSocket: Binance real-time updates
// -------------------------
function connectBinanceWS() {
    const ws = new WebSocket('wss://stream.binance.com:9443/ws/!ticker@arr');

    ws.on('message', message => {
        const tickers = JSON.parse(message);
        tickers.forEach(t => {
            const coin = coinsData.find(c => c.symbol === t.s && c.exchange === 'Binance');
            if (coin) {
                coin.price = parseFloat(t.c);
                coin.priceChangePercent = parseFloat(t.P);
            }
        });
        updateCategories();
    });

    ws.on('close', () => {
        console.log('Binance WS closed. Reconnecting...');
        setTimeout(connectBinanceWS, 5000);
    });

    ws.on('error', err => {
        console.error('Binance WS error:', err.message);
        ws.close();
    });
}

// -------------------------
// WebSocket: Bitget real-time updates
// -------------------------
function connectBitgetWS() {
    const ws = new WebSocket('wss://ws.bitget.com/spot/v1/stream');

    ws.on('open', () => {
        // Subscribe to all tickers
        ws.send(JSON.stringify({
            op: 'subscribe',
            args: [{ channel: 'tickers', instType: 'SPOT' }]
        }));
    });

    ws.on('message', message => {
        const msg = JSON.parse(message);
        if (msg.arg && msg.arg.channel === 'tickers' && msg.data) {
            msg.data.forEach(t => {
                const coin = coinsData.find(c => c.symbol === t.instId.replace('_', '').toUpperCase() && c.exchange === 'Bitget');
                if (coin) {
                    coin.price = parseFloat(t.last);
                    coin.priceChangePercent = parseFloat(t.changeRate) * 100;
                }
            });
            updateCategories();
        }
    });

    ws.on('close', () => {
        console.log('Bitget WS closed. Reconnecting...');
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

// Main crypto endpoint
app.get('/api/crypto', (req, res) => {
    res.json({ allCoins: coinsData, gainers, losers, newlyListed });
});

// Candlestick endpoint
app.get('/api/candles/:symbol/:interval', async (req, res) => {
    const { symbol, interval } = req.params;
    try {
        let { data } = await axios.get(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=100`);
        res.json({ exchange: 'Binance', data });
    } catch {
        try {
            const { data } = await axios.get(`https://api.bitget.com/api/spot/v1/market/candles?symbol=${symbol}&period=${interval}&limit=100`);
            res.json({ exchange: 'Bitget', data: data.data });
        } catch (err) {
            res.status(500).json({ error: 'Failed to fetch candlestick data', details: err.message });
        }
    }
});

// Health check
app.get('/', (req, res) => res.send('Crypto API with WebSocket is running 🚀'));

// -------------------------
// Start server
// -------------------------
app.listen(PORT, async () => {
    console.log(`Crypto API running on port ${PORT}`);
    await fetchInitialTickers();
    connectBinanceWS();
    connectBitgetWS();
});