// WATTx Mining Pool API Server
// Provides statistics and worker tracking for the pool frontend

const express = require('express');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const fetch = require('node-fetch');
const path = require('path');

// Configuration
const config = {
  port: process.env.PORT || 3001,
  wattxRpcHost: process.env.WATTX_RPC_HOST || '127.0.0.1',
  wattxRpcPort: process.env.WATTX_RPC_PORT || 1337,
  wattxRpcUser: process.env.WATTX_RPC_USER || 'wattx',
  wattxRpcPass: process.env.WATTX_RPC_PASS || 'wattx',
  stratumPort: process.env.STRATUM_PORT || 3335,
  poolFee: 1.0, // 1% pool fee
  payoutThreshold: 0.1, // Minimum payout in WTX
  payoutInterval: 3600, // Payout check every hour
  poolWallet: process.env.POOL_WALLET || ''
};

// Initialize database
const db = new Database(path.join(__dirname, 'pool.db'));
initDatabase();

function initDatabase() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS workers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      address TEXT NOT NULL,
      worker_name TEXT NOT NULL,
      first_seen INTEGER NOT NULL,
      last_seen INTEGER NOT NULL,
      total_shares INTEGER DEFAULT 0,
      valid_shares INTEGER DEFAULT 0,
      invalid_shares INTEGER DEFAULT 0,
      hashrate REAL DEFAULT 0,
      UNIQUE(address, worker_name)
    );

    CREATE TABLE IF NOT EXISTS shares (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      address TEXT NOT NULL,
      worker_name TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      difficulty REAL NOT NULL,
      valid INTEGER NOT NULL,
      block_height INTEGER,
      algorithm TEXT DEFAULT 'randomx'
    );

    CREATE TABLE IF NOT EXISTS blocks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      height INTEGER NOT NULL UNIQUE,
      hash TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      reward REAL NOT NULL,
      finder_address TEXT NOT NULL,
      finder_worker TEXT,
      confirmed INTEGER DEFAULT 0,
      confirmations INTEGER DEFAULT 0,
      algorithm TEXT DEFAULT 'randomx'
    );

    CREATE TABLE IF NOT EXISTS payouts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      address TEXT NOT NULL,
      amount REAL NOT NULL,
      timestamp INTEGER NOT NULL,
      txid TEXT,
      status TEXT DEFAULT 'pending'
    );

    CREATE TABLE IF NOT EXISTS pool_stats (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp INTEGER NOT NULL,
      hashrate REAL NOT NULL,
      workers INTEGER NOT NULL,
      blocks_found INTEGER NOT NULL,
      shares_submitted INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_shares_timestamp ON shares(timestamp);
    CREATE INDEX IF NOT EXISTS idx_shares_address ON shares(address);
    CREATE INDEX IF NOT EXISTS idx_workers_address ON workers(address);
  `);
}

// Express app setup
const app = express();
app.use(cors());
app.use(express.json());

// Create HTTP server and WebSocket server
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

// Store connected WebSocket clients
const wsClients = new Set();

wss.on('connection', (ws) => {
  wsClients.add(ws);
  console.log('WebSocket client connected');

  ws.on('close', () => {
    wsClients.delete(ws);
    console.log('WebSocket client disconnected');
  });
});

// Broadcast to all WebSocket clients
function broadcast(type, data) {
  const message = JSON.stringify({ type, data, timestamp: Date.now() });
  wsClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// WATTx RPC helper
async function wattxRpc(method, params = []) {
  const auth = Buffer.from(`${config.wattxRpcUser}:${config.wattxRpcPass}`).toString('base64');

  try {
    const response = await fetch(`http://${config.wattxRpcHost}:${config.wattxRpcPort}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${auth}`
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: Date.now(),
        method,
        params
      })
    });

    const data = await response.json();
    if (data.error) {
      throw new Error(data.error.message);
    }
    return data.result;
  } catch (error) {
    console.error(`RPC error (${method}):`, error.message);
    throw error;
  }
}

// API Routes

// Pool statistics
app.get('/api/stats', async (req, res) => {
  try {
    // Get network info
    let networkInfo = {};
    let miningInfo = {};

    try {
      networkInfo = await wattxRpc('getblockchaininfo');
      miningInfo = await wattxRpc('getmininginfo');
    } catch (e) {
      console.log('Node not ready:', e.message);
    }

    // Get pool stats from database
    const workerCount = db.prepare('SELECT COUNT(DISTINCT address) as count FROM workers WHERE last_seen > ?').get(Date.now() / 1000 - 600);
    const blocksFound = db.prepare('SELECT COUNT(*) as count FROM blocks').get();
    const recentShares = db.prepare('SELECT COUNT(*) as count FROM shares WHERE timestamp > ?').get(Date.now() / 1000 - 3600);

    // Calculate pool hashrate (shares in last hour * difficulty / 3600)
    const shareStats = db.prepare(`
      SELECT SUM(difficulty) as total_diff
      FROM shares
      WHERE timestamp > ? AND valid = 1
    `).get(Date.now() / 1000 - 600);

    const poolHashrate = shareStats?.total_diff ? (shareStats.total_diff * 4294967296) / 600 : 0;

    res.json({
      pool: {
        hashrate: poolHashrate,
        hashrateFormatted: formatHashrate(poolHashrate),
        workers: workerCount?.count || 0,
        blocksFound: blocksFound?.count || 0,
        sharesPerHour: recentShares?.count || 0,
        fee: config.poolFee,
        payoutThreshold: config.payoutThreshold,
        stratumHost: 'stratum+tcp://wtx-pool.wattxchange.app',
        stratumPort: config.stratumPort
      },
      network: {
        height: networkInfo.blocks || 0,
        difficulty: miningInfo.difficulty || 0,
        hashrate: miningInfo.networkhashps || 0,
        hashrateFormatted: formatHashrate(miningInfo.networkhashps || 0)
      },
      coin: {
        name: 'WATTx',
        symbol: 'WTX',
        algorithm: 'X25X (Multi-Algorithm)'
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Blocks found by pool
app.get('/api/blocks', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const offset = parseInt(req.query.offset) || 0;

    const blocks = db.prepare(`
      SELECT * FROM blocks
      ORDER BY height DESC
      LIMIT ? OFFSET ?
    `).all(limit, offset);

    const total = db.prepare('SELECT COUNT(*) as count FROM blocks').get();

    res.json({
      blocks,
      total: total.count,
      limit,
      offset
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Worker statistics for an address
app.get('/api/workers/:address', async (req, res) => {
  try {
    const { address } = req.params;

    const workers = db.prepare(`
      SELECT * FROM workers
      WHERE address = ?
      ORDER BY last_seen DESC
    `).all(address);

    // Calculate total stats
    const totals = db.prepare(`
      SELECT
        SUM(valid_shares) as total_valid,
        SUM(invalid_shares) as total_invalid,
        SUM(hashrate) as total_hashrate
      FROM workers
      WHERE address = ?
    `).get(address);

    // Get recent shares
    const recentShares = db.prepare(`
      SELECT COUNT(*) as count, SUM(difficulty) as total_diff
      FROM shares
      WHERE address = ? AND timestamp > ? AND valid = 1
    `).get(address, Date.now() / 1000 - 3600);

    // Get pending balance (unpaid shares since last payout)
    const lastPayout = db.prepare(`
      SELECT MAX(timestamp) as ts FROM payouts WHERE address = ? AND status = 'completed'
    `).get(address);

    const pendingShares = db.prepare(`
      SELECT SUM(difficulty) as total_diff
      FROM shares
      WHERE address = ? AND valid = 1 AND timestamp > ?
    `).get(address, lastPayout?.ts || 0);

    res.json({
      address,
      workers,
      totals: {
        validShares: totals?.total_valid || 0,
        invalidShares: totals?.total_invalid || 0,
        hashrate: totals?.total_hashrate || 0,
        hashrateFormatted: formatHashrate(totals?.total_hashrate || 0)
      },
      recent: {
        sharesPerHour: recentShares?.count || 0,
        difficulty: recentShares?.total_diff || 0
      },
      pendingBalance: calculatePendingBalance(pendingShares?.total_diff || 0)
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Payments for an address
app.get('/api/payments/:address', async (req, res) => {
  try {
    const { address } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);

    const payments = db.prepare(`
      SELECT * FROM payouts
      WHERE address = ?
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(address, limit);

    const totalPaid = db.prepare(`
      SELECT SUM(amount) as total FROM payouts
      WHERE address = ? AND status = 'completed'
    `).get(address);

    res.json({
      payments,
      totalPaid: totalPaid?.total || 0
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Pool hashrate history
app.get('/api/hashrate/history', async (req, res) => {
  try {
    const hours = Math.min(parseInt(req.query.hours) || 24, 168);

    const history = db.prepare(`
      SELECT timestamp, hashrate, workers
      FROM pool_stats
      WHERE timestamp > ?
      ORDER BY timestamp ASC
    `).all(Date.now() / 1000 - hours * 3600);

    res.json({ history });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Record a share (called by stratum server integration)
app.post('/api/share', async (req, res) => {
  try {
    const { address, worker_name, difficulty, valid, block_height, algorithm } = req.body;

    if (!address) {
      return res.status(400).json({ error: 'Address required' });
    }

    const timestamp = Math.floor(Date.now() / 1000);

    // Insert share
    db.prepare(`
      INSERT INTO shares (address, worker_name, timestamp, difficulty, valid, block_height, algorithm)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(address, worker_name || 'default', timestamp, difficulty || 1, valid ? 1 : 0, block_height, algorithm || 'randomx');

    // Update worker stats
    db.prepare(`
      INSERT INTO workers (address, worker_name, first_seen, last_seen, total_shares, valid_shares, invalid_shares)
      VALUES (?, ?, ?, ?, 1, ?, ?)
      ON CONFLICT(address, worker_name) DO UPDATE SET
        last_seen = ?,
        total_shares = total_shares + 1,
        valid_shares = valid_shares + ?,
        invalid_shares = invalid_shares + ?
    `).run(
      address, worker_name || 'default', timestamp, timestamp,
      valid ? 1 : 0, valid ? 0 : 1,
      timestamp, valid ? 1 : 0, valid ? 0 : 1
    );

    // Broadcast share update
    broadcast('share', { address, worker_name, difficulty, valid, timestamp });

    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Record a block found
app.post('/api/block', async (req, res) => {
  try {
    const { height, hash, reward, finder_address, finder_worker, algorithm } = req.body;

    if (!height || !hash) {
      return res.status(400).json({ error: 'Height and hash required' });
    }

    const timestamp = Math.floor(Date.now() / 1000);

    db.prepare(`
      INSERT OR REPLACE INTO blocks (height, hash, timestamp, reward, finder_address, finder_worker, algorithm)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(height, hash, timestamp, reward || 0, finder_address || '', finder_worker || '', algorithm || 'randomx');

    // Broadcast block found
    broadcast('block', { height, hash, reward, finder_address, timestamp });

    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get X25X algorithm statistics
app.get('/api/algorithms', async (req, res) => {
  try {
    let algos = {};

    try {
      algos = await wattxRpc('getx25xalgorithms');
    } catch (e) {
      // Return default algorithm list if RPC fails
      algos = {
        algorithms: [
          { name: 'SHA256D', enabled: true },
          { name: 'Scrypt', enabled: true },
          { name: 'Ethash', enabled: true },
          { name: 'RandomX', enabled: true },
          { name: 'Equihash', enabled: true },
          { name: 'X11', enabled: true },
          { name: 'kHeavyHash', enabled: true }
        ]
      };
    }

    // Get shares by algorithm
    const sharesByAlgo = db.prepare(`
      SELECT algorithm, COUNT(*) as shares, SUM(difficulty) as total_diff
      FROM shares
      WHERE timestamp > ?
      GROUP BY algorithm
    `).all(Date.now() / 1000 - 86400);

    res.json({
      algorithms: algos.algorithms || algos,
      sharesByAlgorithm: sharesByAlgo
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Utility functions
function formatHashrate(hashrate) {
  if (hashrate >= 1e15) return (hashrate / 1e15).toFixed(2) + ' PH/s';
  if (hashrate >= 1e12) return (hashrate / 1e12).toFixed(2) + ' TH/s';
  if (hashrate >= 1e9) return (hashrate / 1e9).toFixed(2) + ' GH/s';
  if (hashrate >= 1e6) return (hashrate / 1e6).toFixed(2) + ' MH/s';
  if (hashrate >= 1e3) return (hashrate / 1e3).toFixed(2) + ' KH/s';
  return hashrate.toFixed(2) + ' H/s';
}

function calculatePendingBalance(totalDifficulty) {
  // Simplified: convert difficulty to estimated WTX based on pool share
  // In production, this would be based on actual block rewards
  const blockReward = 1.0; // WTX per block (example)
  const estimatedBlocks = totalDifficulty / (4294967296 * 1000); // rough estimate
  return estimatedBlocks * blockReward * (1 - config.poolFee / 100);
}

// Periodic stats recording
setInterval(() => {
  const workerCount = db.prepare('SELECT COUNT(DISTINCT address) as count FROM workers WHERE last_seen > ?').get(Date.now() / 1000 - 600);
  const blocksFound = db.prepare('SELECT COUNT(*) as count FROM blocks').get();
  const recentShares = db.prepare('SELECT COUNT(*) as count FROM shares WHERE timestamp > ?').get(Date.now() / 1000 - 3600);

  const shareStats = db.prepare(`
    SELECT SUM(difficulty) as total_diff
    FROM shares
    WHERE timestamp > ? AND valid = 1
  `).get(Date.now() / 1000 - 600);

  const poolHashrate = shareStats?.total_diff ? (shareStats.total_diff * 4294967296) / 600 : 0;

  db.prepare(`
    INSERT INTO pool_stats (timestamp, hashrate, workers, blocks_found, shares_submitted)
    VALUES (?, ?, ?, ?, ?)
  `).run(
    Math.floor(Date.now() / 1000),
    poolHashrate,
    workerCount?.count || 0,
    blocksFound?.count || 0,
    recentShares?.count || 0
  );

  // Broadcast stats update
  broadcast('stats', {
    hashrate: poolHashrate,
    workers: workerCount?.count || 0,
    blocks: blocksFound?.count || 0
  });
}, 60000); // Every minute

// Start server
server.listen(config.port, () => {
  console.log(`WATTx Pool API running on port ${config.port}`);
  console.log(`WebSocket available at ws://localhost:${config.port}/ws`);
});
