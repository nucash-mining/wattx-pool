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
  wattxRpcPort: process.env.WATTX_RPC_PORT || 2337,
  wattxRpcUser: process.env.WATTX_RPC_USER || 'wattxrpc',
  wattxRpcPass: process.env.WATTX_RPC_PASS || 'v4AZR3AmHHbrMkRfhXlkWH6MI1bFeHwV',
  stratumPort: process.env.STRATUM_PORT || 3334,
  poolFee: 1.0, // 1% pool fee
  payoutThreshold: 0.1, // Minimum payout in WTX
  payoutInterval: 3600, // Payout check every hour (seconds)
  defaultBlockReward: 1.0, // WTX per block (used when reward is not known)
  confirmationsRequired: 100, // Confirmations before a block is paid out
  poolWallet: process.env.POOL_WALLET || 'WPTAXDteyU2U1u1LRLXzXiVUjxryeZkAEP'
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

  // Add columns introduced after initial schema creation (safe to run every boot)
  try { db.exec('ALTER TABLE blocks ADD COLUMN paid INTEGER DEFAULT 0'); } catch (_) {}
  try { db.exec('ALTER TABLE payouts ADD COLUMN block_height INTEGER'); } catch (_) {}
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

// ---- Block confirmation tracking ----
async function updateBlockConfirmations() {
  try {
    const chainInfo = await wattxRpc('getblockchaininfo');
    const currentHeight = chainInfo.blocks;
    const unconfirmed = db.prepare('SELECT height FROM blocks WHERE confirmed = 0').all();
    const stmt = db.prepare('UPDATE blocks SET confirmations = ?, confirmed = ? WHERE height = ?');
    for (const { height } of unconfirmed) {
      const confs = Math.max(0, currentHeight - height + 1);
      stmt.run(confs, confs >= config.confirmationsRequired ? 1 : 0, height);
    }
  } catch (e) {
    console.error('Block confirmation update failed:', e.message);
  }
}

// ---- PROP payout loop ----
async function processPayouts() {
  const unpaidBlocks = db.prepare(
    'SELECT * FROM blocks WHERE confirmed = 1 AND paid = 0'
  ).all();

  for (const block of unpaidBlocks) {
    const reward = block.reward > 0 ? block.reward : config.defaultBlockReward;
    const afterFee = reward * (1 - config.poolFee / 100);

    // Shares submitted while mining this block height form the round
    const roundShares = db.prepare(`
      SELECT address, SUM(difficulty) AS total_diff
      FROM shares
      WHERE block_height = ? AND valid = 1
      GROUP BY address
    `).all(block.height);

    const totalDiff = roundShares.reduce((s, r) => s + r.total_diff, 0);

    if (totalDiff === 0) {
      // No shares recorded for this round — mark paid to avoid retry loop
      db.prepare('UPDATE blocks SET paid = 1 WHERE height = ?').run(block.height);
      continue;
    }

    const timestamp = Math.floor(Date.now() / 1000);
    let roundFailed = false;

    for (const miner of roundShares) {
      const proportion = miner.total_diff / totalDiff;
      const amount = parseFloat((afterFee * proportion).toFixed(8));
      if (amount < config.payoutThreshold) continue;

      try {
        const txid = await wattxRpc('sendtoaddress', [miner.address, amount]);
        db.prepare(`
          INSERT INTO payouts (address, amount, timestamp, txid, status, block_height)
          VALUES (?, ?, ?, ?, 'completed', ?)
        `).run(miner.address, amount, timestamp, txid, block.height);
        console.log(`Payout: ${amount} WTX → ${miner.address} (block ${block.height}, tx ${txid})`);
        broadcast('payout', { address: miner.address, amount, txid, block_height: block.height });
      } catch (e) {
        console.error(`Payout failed for ${miner.address} (block ${block.height}): ${e.message}`);
        roundFailed = true;
      }
    }

    // Only mark paid if no errors; retry on next interval if any payout failed
    if (!roundFailed) {
      db.prepare('UPDATE blocks SET paid = 1 WHERE height = ?').run(block.height);
    }
  }
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

  updateBlockConfirmations();
}, 60000); // Every minute

// Payout loop — runs every payoutInterval seconds
setInterval(() => {
  processPayouts().catch((e) => console.error('processPayouts error:', e.message));
}, config.payoutInterval * 1000);

// Start server
server.listen(config.port, () => {
  console.log(`WATTx Pool API running on port ${config.port}`);
  console.log(`WebSocket available at ws://localhost:${config.port}/ws`);
});
