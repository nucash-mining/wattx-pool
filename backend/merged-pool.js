// WATTx Multi-Algorithm Merged Mining Pool
// Supports merged mining across 7 algorithms, each with its parent chain

const express = require('express');
const cors = require('cors');
const http = require('http');
const net = require('net');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const fetch = require('node-fetch');
const crypto = require('crypto');
const path = require('path');

// ============================================================================
// CONFIGURATION
// ============================================================================

const config = {
  // API Server
  apiPort: process.env.API_PORT || 3002,

  // Pool settings
  poolFee: 1.0,
  payoutThreshold: {
    WTX: 0.1,
    BTC: 0.0001,
    LTC: 0.001,
    ALT: 0.01,
    XMR: 0.001,
    DASH: 0.001,
    ZEC: 0.001,
    KAS: 1.0
  },

  // Algorithm configurations with stratum ports and parent chain RPC
  algorithms: {
    sha256d: {
      name: 'SHA256D',
      stratumPort: 3336,
      parentChain: 'BTC',
      parentRpc: {
        host: process.env.BTC_RPC_HOST || '127.0.0.1',
        port: process.env.BTC_RPC_PORT || 8332,
        user: process.env.BTC_RPC_USER || 'bitcoin',
        pass: process.env.BTC_RPC_PASS || 'bitcoin'
      },
      difficulty: 1,
      enabled: true
    },
    scrypt: {
      name: 'Scrypt',
      stratumPort: 3337,
      parentChain: 'LTC',
      parentRpc: {
        host: process.env.LTC_RPC_HOST || '127.0.0.1',
        port: process.env.LTC_RPC_PORT || 9332,
        user: process.env.LTC_RPC_USER || 'litecoin',
        pass: process.env.LTC_RPC_PASS || 'litecoin'
      },
      difficulty: 16,
      enabled: true
    },
    ethash: {
      name: 'Ethash',
      stratumPort: 3333,
      parentChain: 'ALT',
      parentRpc: {
        host: process.env.ALT_RPC_HOST || '127.0.0.1',
        port: process.env.ALT_RPC_PORT || 8545,
        user: '',
        pass: ''
      },
      difficulty: 1000000,
      enabled: true
    },
    randomx: {
      name: 'RandomX',
      stratumPort: 3334,
      parentChain: 'XMR',
      parentRpc: {
        host: process.env.XMR_RPC_HOST || '127.0.0.1',
        port: process.env.XMR_RPC_PORT || 18081,
        user: process.env.XMR_RPC_USER || '',
        pass: process.env.XMR_RPC_PASS || ''
      },
      difficulty: 10000,
      enabled: true
    },
    x11: {
      name: 'X11',
      stratumPort: 3340,
      parentChain: 'DASH',
      parentRpc: {
        host: process.env.DASH_RPC_HOST || '127.0.0.1',
        port: process.env.DASH_RPC_PORT || 9998,
        user: process.env.DASH_RPC_USER || 'dash',
        pass: process.env.DASH_RPC_PASS || 'dash'
      },
      difficulty: 1,
      enabled: true
    },
    equihash: {
      name: 'Equihash',
      stratumPort: 3341,
      parentChain: 'ZEC',
      parentRpc: {
        host: process.env.ZEC_RPC_HOST || '127.0.0.1',
        port: process.env.ZEC_RPC_PORT || 8232,
        user: process.env.ZEC_RPC_USER || 'zcash',
        pass: process.env.ZEC_RPC_PASS || 'zcash'
      },
      difficulty: 1,
      enabled: true,
      // Additional Equihash 200,9 parent chains sharing port 3341
      additionalParents: {
        ZEN: {
          parentRpc: {
            host: process.env.ZEN_RPC_HOST || '127.0.0.1',
            port: process.env.ZEN_RPC_PORT || 18231,
            user: process.env.ZEN_RPC_USER || 'zen',
            pass: process.env.ZEN_RPC_PASS || 'zen'
          },
          enabled: !!process.env.ZEN_RPC_HOST
        },
        BTCZ: {
          parentRpc: {
            host: process.env.BTCZ_RPC_HOST || '127.0.0.1',
            port: process.env.BTCZ_RPC_PORT || 1979,
            user: process.env.BTCZ_RPC_USER || 'bitcoinz',
            pass: process.env.BTCZ_RPC_PASS || 'bitcoinz'
          },
          electrum: [
            'electrum1.btcz.rocks:50001',
            'electrum2.btcz.rocks:50001'
          ],
          enabled: !!process.env.BTCZ_RPC_HOST
        }
      }
    },
    kheavyhash: {
      name: 'kHeavyHash',
      stratumPort: 3342,
      parentChain: 'KAS',
      parentRpc: {
        host: process.env.KAS_RPC_HOST || '127.0.0.1',
        port: process.env.KAS_RPC_PORT || 16110,
        user: '',
        pass: ''
      },
      difficulty: 1,
      enabled: true
    }
  },

  // WATTx node RPC
  wattxRpc: {
    host: process.env.WATTX_RPC_HOST || '127.0.0.1',
    port: process.env.WATTX_RPC_PORT || 1337,
    user: process.env.WATTX_RPC_USER || 'wattx',
    pass: process.env.WATTX_RPC_PASS || 'wattx'
  }
};

// ============================================================================
// DATABASE
// ============================================================================

const db = new Database(path.join(__dirname, 'merged-pool.db'));

function initDatabase() {
  db.exec(`
    -- Workers table with multi-address support
    CREATE TABLE IF NOT EXISTS workers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      wtx_address TEXT NOT NULL,
      parent_address TEXT,
      worker_name TEXT NOT NULL,
      algorithm TEXT NOT NULL,
      first_seen INTEGER NOT NULL,
      last_seen INTEGER NOT NULL,
      total_shares INTEGER DEFAULT 0,
      valid_shares INTEGER DEFAULT 0,
      invalid_shares INTEGER DEFAULT 0,
      hashrate REAL DEFAULT 0,
      UNIQUE(wtx_address, parent_address, worker_name, algorithm)
    );

    -- Shares with algorithm tracking
    CREATE TABLE IF NOT EXISTS shares (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      wtx_address TEXT NOT NULL,
      parent_address TEXT,
      worker_name TEXT NOT NULL,
      algorithm TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      difficulty REAL NOT NULL,
      valid INTEGER NOT NULL,
      wtx_valid INTEGER DEFAULT 0,
      parent_valid INTEGER DEFAULT 0,
      block_height INTEGER
    );

    -- Blocks found (both WTX and parent chain)
    CREATE TABLE IF NOT EXISTS blocks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain TEXT NOT NULL,
      algorithm TEXT NOT NULL,
      height INTEGER NOT NULL,
      hash TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      reward REAL NOT NULL,
      finder_wtx_address TEXT NOT NULL,
      finder_parent_address TEXT,
      finder_worker TEXT,
      confirmed INTEGER DEFAULT 0,
      confirmations INTEGER DEFAULT 0,
      UNIQUE(chain, height)
    );

    -- Multi-coin balances
    CREATE TABLE IF NOT EXISTS balances (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      wtx_address TEXT NOT NULL,
      parent_address TEXT,
      coin TEXT NOT NULL,
      balance REAL DEFAULT 0,
      total_paid REAL DEFAULT 0,
      UNIQUE(wtx_address, parent_address, coin)
    );

    -- Payouts with coin type
    CREATE TABLE IF NOT EXISTS payouts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      address TEXT NOT NULL,
      coin TEXT NOT NULL,
      amount REAL NOT NULL,
      timestamp INTEGER NOT NULL,
      txid TEXT,
      status TEXT DEFAULT 'pending'
    );

    -- Pool stats per algorithm
    CREATE TABLE IF NOT EXISTS pool_stats (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      algorithm TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      hashrate REAL NOT NULL,
      workers INTEGER NOT NULL,
      wtx_blocks INTEGER DEFAULT 0,
      parent_blocks INTEGER DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_shares_timestamp ON shares(timestamp);
    CREATE INDEX IF NOT EXISTS idx_shares_wtx_address ON shares(wtx_address);
    CREATE INDEX IF NOT EXISTS idx_shares_algorithm ON shares(algorithm);
    CREATE INDEX IF NOT EXISTS idx_blocks_chain ON blocks(chain);
    CREATE INDEX IF NOT EXISTS idx_balances_address ON balances(wtx_address);
  `);
}

initDatabase();

// ============================================================================
// RPC HELPERS
// ============================================================================

async function rpcCall(rpcConfig, method, params = []) {
  const auth = rpcConfig.user ?
    Buffer.from(`${rpcConfig.user}:${rpcConfig.pass}`).toString('base64') : null;

  const headers = { 'Content-Type': 'application/json' };
  if (auth) headers['Authorization'] = `Basic ${auth}`;

  try {
    const response = await fetch(`http://${rpcConfig.host}:${rpcConfig.port}`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: Date.now(),
        method,
        params
      }),
      timeout: 10000
    });

    const data = await response.json();
    if (data.error) throw new Error(data.error.message);
    return data.result;
  } catch (error) {
    console.error(`RPC error (${method}):`, error.message);
    return null;
  }
}

// Monero uses different RPC format
async function moneroRpc(rpcConfig, method, params = {}) {
  try {
    const response = await fetch(`http://${rpcConfig.host}:${rpcConfig.port}/json_rpc`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '0',
        method,
        params
      }),
      timeout: 10000
    });

    const data = await response.json();
    if (data.error) throw new Error(data.error.message);
    return data.result;
  } catch (error) {
    console.error(`Monero RPC error (${method}):`, error.message);
    return null;
  }
}

// Kaspa uses gRPC, simplified HTTP wrapper
async function kaspaRpc(rpcConfig, method, params = {}) {
  try {
    const response = await fetch(`http://${rpcConfig.host}:${rpcConfig.port}/${method}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
      timeout: 10000
    });
    return await response.json();
  } catch (error) {
    console.error(`Kaspa RPC error (${method}):`, error.message);
    return null;
  }
}

// Ethereum-style RPC for Altcoinchain
async function ethRpc(rpcConfig, method, params = []) {
  try {
    const response = await fetch(`http://${rpcConfig.host}:${rpcConfig.port}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: Date.now(),
        method,
        params
      }),
      timeout: 10000
    });

    const data = await response.json();
    if (data.error) throw new Error(data.error.message);
    return data.result;
  } catch (error) {
    console.error(`ETH RPC error (${method}):`, error.message);
    return null;
  }
}

// ============================================================================
// MERGED MINING JOB MANAGER
// ============================================================================

class MergedJobManager {
  constructor(algorithm, algoConfig) {
    this.algorithm = algorithm;
    this.config = algoConfig;
    this.currentJob = null;
    this.jobs = new Map();
    this.jobCounter = 0;
  }

  async createJob() {
    try {
      // Get WATTx block template
      const wtxTemplate = await rpcCall(config.wattxRpc, 'getblocktemplate', [
        { rules: ['segwit'], capabilities: ['coinbasetxn'] }
      ]);

      if (!wtxTemplate) {
        console.log(`[${this.algorithm}] Failed to get WATTx template`);
        return null;
      }

      // Get parent chain template based on algorithm
      let parentTemplate = null;
      const parentChain = this.config.parentChain;

      switch (parentChain) {
        case 'BTC':
        case 'LTC':
        case 'DASH':
        case 'ZEC':
          parentTemplate = await rpcCall(this.config.parentRpc, 'getblocktemplate', [
            { rules: ['segwit'], capabilities: ['coinbasetxn'] }
          ]);
          break;
        case 'XMR':
          parentTemplate = await moneroRpc(this.config.parentRpc, 'get_block_template', {
            wallet_address: process.env.XMR_POOL_WALLET || '',
            reserve_size: 8
          });
          break;
        case 'ALT':
          parentTemplate = await ethRpc(this.config.parentRpc, 'eth_getWork');
          break;
        case 'KAS':
          parentTemplate = await kaspaRpc(this.config.parentRpc, 'getBlockTemplate', {});
          break;
      }

      const jobId = this.generateJobId();

      const job = {
        id: jobId,
        algorithm: this.algorithm,
        timestamp: Date.now(),

        // WATTx data
        wtx: {
          height: wtxTemplate.height,
          prevHash: wtxTemplate.previousblockhash,
          merkleRoot: wtxTemplate.merkleroot || '',
          bits: wtxTemplate.bits,
          target: wtxTemplate.target,
          coinbaseValue: wtxTemplate.coinbasevalue,
          template: wtxTemplate
        },

        // Parent chain data
        parent: parentTemplate ? {
          chain: parentChain,
          template: parentTemplate,
          height: this.getParentHeight(parentTemplate, parentChain),
          target: this.getParentTarget(parentTemplate, parentChain)
        } : null
      };

      this.jobs.set(jobId, job);
      this.currentJob = job;

      // Cleanup old jobs
      if (this.jobs.size > 10) {
        const oldest = this.jobs.keys().next().value;
        this.jobs.delete(oldest);
      }

      console.log(`[${this.algorithm}] New job ${jobId} - WTX height: ${job.wtx.height}, ${parentChain} height: ${job.parent?.height || 'N/A'}`);

      return job;
    } catch (error) {
      console.error(`[${this.algorithm}] Error creating job:`, error.message);
      return null;
    }
  }

  getParentHeight(template, chain) {
    if (!template) return 0;
    switch (chain) {
      case 'BTC':
      case 'LTC':
      case 'DASH':
      case 'ZEC':
        return template.height;
      case 'XMR':
        return template.height;
      case 'ALT':
        return parseInt(template[1], 16); // Work array format
      case 'KAS':
        return template.block?.header?.blueScore || 0;
      default:
        return 0;
    }
  }

  getParentTarget(template, chain) {
    if (!template) return null;
    switch (chain) {
      case 'BTC':
      case 'LTC':
      case 'DASH':
      case 'ZEC':
        return template.target;
      case 'XMR':
        return template.difficulty;
      case 'ALT':
        return template[2]; // Target in work array
      case 'KAS':
        return template.block?.header?.bits;
      default:
        return null;
    }
  }

  generateJobId() {
    return crypto.randomBytes(4).toString('hex') + (this.jobCounter++).toString(16).padStart(8, '0');
  }

  getJob(jobId) {
    return this.jobs.get(jobId);
  }
}

// ============================================================================
// STRATUM SERVER (per algorithm)
// ============================================================================

class StratumServer {
  constructor(algorithm, algoConfig) {
    this.algorithm = algorithm;
    this.config = algoConfig;
    this.clients = new Map();
    this.clientCounter = 0;
    this.jobManager = new MergedJobManager(algorithm, algoConfig);
    this.server = null;
    this.shareStats = { accepted: 0, rejected: 0, wtxBlocks: 0, parentBlocks: 0 };
  }

  start() {
    this.server = net.createServer((socket) => this.handleConnection(socket));

    this.server.listen(this.config.stratumPort, '0.0.0.0', () => {
      console.log(`[${this.algorithm}] Stratum server listening on port ${this.config.stratumPort}`);
    });

    // Job update loop
    this.startJobLoop();
  }

  async startJobLoop() {
    while (true) {
      await this.jobManager.createJob();
      if (this.jobManager.currentJob) {
        this.broadcastJob();
      }
      await new Promise(r => setTimeout(r, 30000)); // New job every 30s
    }
  }

  handleConnection(socket) {
    const clientId = ++this.clientCounter;
    const client = {
      id: clientId,
      socket,
      wtxAddress: null,
      parentAddress: null,
      workerName: 'default',
      subscribed: false,
      authorized: false,
      difficulty: this.config.difficulty,
      buffer: ''
    };

    this.clients.set(clientId, client);
    console.log(`[${this.algorithm}] Client ${clientId} connected`);

    socket.on('data', (data) => {
      client.buffer += data.toString();

      let lines = client.buffer.split('\n');
      client.buffer = lines.pop();

      for (const line of lines) {
        if (line.trim()) {
          this.handleMessage(client, line.trim());
        }
      }
    });

    socket.on('close', () => {
      this.clients.delete(clientId);
      console.log(`[${this.algorithm}] Client ${clientId} disconnected`);
    });

    socket.on('error', (err) => {
      console.log(`[${this.algorithm}] Client ${clientId} error:`, err.message);
      this.clients.delete(clientId);
    });
  }

  handleMessage(client, message) {
    try {
      const request = JSON.parse(message);
      const { id, method, params } = request;

      switch (method) {
        case 'mining.subscribe':
          this.handleSubscribe(client, id, params);
          break;
        case 'mining.authorize':
          this.handleAuthorize(client, id, params);
          break;
        case 'mining.submit':
        case 'submit':
          this.handleSubmit(client, id, params);
          break;
        case 'login':
        case 'getjob':
          this.handleLogin(client, id, params);
          break;
        case 'mining.extranonce.subscribe':
          this.send(client, { id, result: true, error: null });
          break;
        default:
          console.log(`[${this.algorithm}] Unknown method: ${method}`);
      }
    } catch (error) {
      console.error(`[${this.algorithm}] Error parsing message:`, error.message);
    }
  }

  handleSubscribe(client, id, params) {
    client.subscribed = true;
    const sessionId = crypto.randomBytes(8).toString('hex');

    this.send(client, {
      id,
      result: [
        [['mining.notify', sessionId], ['mining.difficulty', sessionId]],
        sessionId.slice(0, 8),
        4
      ],
      error: null
    });
  }

  handleAuthorize(client, id, params) {
    // Worker format: WTX_ADDRESS.PARENT_ADDRESS.worker_name
    // Or: WTX_ADDRESS.worker_name (parent address optional)
    const workerString = params[0] || '';
    const parts = workerString.split('.');

    if (parts.length >= 1) {
      client.wtxAddress = parts[0];

      if (parts.length >= 3) {
        client.parentAddress = parts[1];
        client.workerName = parts.slice(2).join('.');
      } else if (parts.length === 2) {
        // Could be WTX.worker or WTX.PARENT
        // Heuristic: if second part looks like an address, it's parent address
        if (parts[1].length > 20) {
          client.parentAddress = parts[1];
          client.workerName = 'default';
        } else {
          client.workerName = parts[1];
        }
      }
    }

    client.authorized = true;

    this.send(client, { id, result: true, error: null });
    console.log(`[${this.algorithm}] Client ${client.id} authorized: WTX=${client.wtxAddress}, ${this.config.parentChain}=${client.parentAddress || 'none'}, worker=${client.workerName}`);

    // Send current job
    if (this.jobManager.currentJob) {
      this.sendJob(client);
    }

    // Update worker in database
    this.updateWorker(client);
  }

  handleLogin(client, id, params) {
    // XMRig-style login (combines subscribe + authorize + getjob)
    const loginParams = typeof params[0] === 'string' ? JSON.parse(params[0]) : params[0];
    const login = loginParams?.login || '';

    const parts = login.split('.');
    client.wtxAddress = parts[0] || '';
    client.parentAddress = parts[1] || '';
    client.workerName = parts[2] || 'xmrig';
    client.subscribed = true;
    client.authorized = true;

    const job = this.jobManager.currentJob;
    const sessionId = crypto.randomBytes(8).toString('hex');

    this.send(client, {
      id,
      jsonrpc: '2.0',
      result: {
        id: sessionId,
        job: job ? this.formatJobForClient(job) : null,
        status: 'OK'
      },
      error: null
    });

    console.log(`[${this.algorithm}] Client ${client.id} logged in: WTX=${client.wtxAddress}, ${this.config.parentChain}=${client.parentAddress}`);
    this.updateWorker(client);
  }

  async handleSubmit(client, id, params) {
    const jobId = params.job_id || params[1];
    const nonce = params.nonce || params[4];
    const result = params.result || '';

    const job = this.jobManager.getJob(jobId);
    if (!job) {
      this.send(client, { id, result: null, error: [21, 'Job not found', null] });
      this.shareStats.rejected++;
      return;
    }

    // Validate share
    const validation = await this.validateShare(job, nonce, result, client);

    if (validation.valid) {
      this.shareStats.accepted++;
      this.send(client, { id, result: { status: 'OK' }, error: null });

      // Record share
      this.recordShare(client, job, validation);

      // Check if it's a block
      if (validation.wtxBlock) {
        console.log(`[${this.algorithm}] *** WTX BLOCK FOUND by ${client.wtxAddress} ***`);
        this.shareStats.wtxBlocks++;
        await this.submitWtxBlock(job, nonce, client);
      }

      if (validation.parentBlock) {
        console.log(`[${this.algorithm}] *** ${this.config.parentChain} BLOCK FOUND by ${client.parentAddress || client.wtxAddress} ***`);
        this.shareStats.parentBlocks++;
        await this.submitParentBlock(job, nonce, client);
      }
    } else {
      this.shareStats.rejected++;
      this.send(client, { id, result: null, error: [23, 'Invalid share', null] });
    }
  }

  async validateShare(job, nonce, result, client) {
    // Basic validation - in production this would verify the actual hash
    // For now, accept all shares and check targets

    const validation = {
      valid: true,
      wtxBlock: false,
      parentBlock: false,
      difficulty: client.difficulty
    };

    // Simulate block finding (1 in 10000 chance for testing)
    // In production, actually verify hash against targets
    if (Math.random() < 0.0001) {
      validation.wtxBlock = true;
    }
    if (job.parent && Math.random() < 0.0001) {
      validation.parentBlock = true;
    }

    return validation;
  }

  async submitWtxBlock(job, nonce, client) {
    try {
      // Submit to WATTx node
      const result = await rpcCall(config.wattxRpc, 'submitblock', [job.wtx.template]);

      if (result === null || result === undefined) {
        // Record block found
        db.prepare(`
          INSERT OR REPLACE INTO blocks (chain, algorithm, height, hash, timestamp, reward, finder_wtx_address, finder_parent_address, finder_worker)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run('WTX', this.algorithm, job.wtx.height, job.wtx.prevHash, Math.floor(Date.now() / 1000),
               job.wtx.coinbaseValue / 100000000, client.wtxAddress, client.parentAddress, client.workerName);

        console.log(`[${this.algorithm}] WTX block ${job.wtx.height} submitted successfully`);
      }
    } catch (error) {
      console.error(`[${this.algorithm}] Error submitting WTX block:`, error.message);
    }
  }

  async submitParentBlock(job, nonce, client) {
    try {
      const chain = this.config.parentChain;
      let result;

      switch (chain) {
        case 'BTC':
        case 'LTC':
        case 'DASH':
        case 'ZEC':
          result = await rpcCall(this.config.parentRpc, 'submitblock', [job.parent.template]);
          break;
        case 'XMR':
          result = await moneroRpc(this.config.parentRpc, 'submit_block', [job.parent.template]);
          break;
        case 'ALT':
          result = await ethRpc(this.config.parentRpc, 'eth_submitWork', [nonce, job.parent.template[0], job.parent.template[2]]);
          break;
        case 'KAS':
          result = await kaspaRpc(this.config.parentRpc, 'submitBlock', { block: job.parent.template.block });
          break;
      }

      // Record parent block found
      db.prepare(`
        INSERT OR REPLACE INTO blocks (chain, algorithm, height, hash, timestamp, reward, finder_wtx_address, finder_parent_address, finder_worker)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(chain, this.algorithm, job.parent.height, 'pending', Math.floor(Date.now() / 1000),
             0, client.wtxAddress, client.parentAddress, client.workerName);

      console.log(`[${this.algorithm}] ${chain} block ${job.parent.height} submitted`);
    } catch (error) {
      console.error(`[${this.algorithm}] Error submitting ${this.config.parentChain} block:`, error.message);
    }
  }

  recordShare(client, job, validation) {
    const timestamp = Math.floor(Date.now() / 1000);

    db.prepare(`
      INSERT INTO shares (wtx_address, parent_address, worker_name, algorithm, timestamp, difficulty, valid, wtx_valid, parent_valid, block_height)
      VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
    `).run(client.wtxAddress, client.parentAddress, client.workerName, this.algorithm,
           timestamp, validation.difficulty, validation.wtxBlock ? 1 : 0, validation.parentBlock ? 1 : 0, job.wtx.height);

    // Update balances based on share
    this.updateBalance(client, validation);
  }

  updateBalance(client, validation) {
    const shareValue = validation.difficulty / 1000000; // Simplified share value calc

    // Update WTX balance
    db.prepare(`
      INSERT INTO balances (wtx_address, parent_address, coin, balance)
      VALUES (?, ?, 'WTX', ?)
      ON CONFLICT(wtx_address, parent_address, coin) DO UPDATE SET
        balance = balance + ?
    `).run(client.wtxAddress, client.parentAddress, shareValue, shareValue);

    // Update parent coin balance if parent address provided
    if (client.parentAddress) {
      db.prepare(`
        INSERT INTO balances (wtx_address, parent_address, coin, balance)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(wtx_address, parent_address, coin) DO UPDATE SET
          balance = balance + ?
      `).run(client.wtxAddress, client.parentAddress, this.config.parentChain, shareValue, shareValue);
    }
  }

  updateWorker(client) {
    const timestamp = Math.floor(Date.now() / 1000);

    db.prepare(`
      INSERT INTO workers (wtx_address, parent_address, worker_name, algorithm, first_seen, last_seen)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(wtx_address, parent_address, worker_name, algorithm) DO UPDATE SET
        last_seen = ?
    `).run(client.wtxAddress, client.parentAddress, client.workerName, this.algorithm,
           timestamp, timestamp, timestamp);
  }

  formatJobForClient(job) {
    return {
      blob: job.wtx.prevHash + job.wtx.merkleRoot,
      job_id: job.id,
      target: job.wtx.target?.slice(0, 8) || 'ffffffff',
      height: job.wtx.height,
      seed_hash: job.wtx.prevHash,
      algo: this.algorithm === 'randomx' ? 'rx/0' : this.algorithm
    };
  }

  sendJob(client) {
    const job = this.jobManager.currentJob;
    if (!job) return;

    this.send(client, {
      jsonrpc: '2.0',
      method: 'job',
      params: this.formatJobForClient(job)
    });
  }

  broadcastJob() {
    for (const client of this.clients.values()) {
      if (client.authorized) {
        this.sendJob(client);
      }
    }
  }

  send(client, data) {
    try {
      client.socket.write(JSON.stringify(data) + '\n');
    } catch (error) {
      console.error(`[${this.algorithm}] Error sending to client ${client.id}:`, error.message);
    }
  }

  getStats() {
    const activeWorkers = new Set();
    for (const client of this.clients.values()) {
      if (client.authorized) {
        activeWorkers.add(`${client.wtxAddress}.${client.workerName}`);
      }
    }

    return {
      algorithm: this.algorithm,
      name: this.config.name,
      parentChain: this.config.parentChain,
      stratumPort: this.config.stratumPort,
      workers: activeWorkers.size,
      connections: this.clients.size,
      shares: this.shareStats,
      currentHeight: this.jobManager.currentJob?.wtx?.height || 0,
      parentHeight: this.jobManager.currentJob?.parent?.height || 0
    };
  }
}

// ============================================================================
// EXPRESS API
// ============================================================================

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

// WebSocket clients
const wsClients = new Set();
wss.on('connection', (ws) => {
  wsClients.add(ws);
  ws.on('close', () => wsClients.delete(ws));
});

function broadcast(type, data) {
  const msg = JSON.stringify({ type, data, timestamp: Date.now() });
  wsClients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

// Store stratum servers
const stratumServers = {};

// API: Pool overview stats
app.get('/api/stats', async (req, res) => {
  try {
    const stats = {
      pool: {
        name: 'WATTx Multi-Algorithm Merged Mining Pool',
        fee: config.poolFee,
        algorithms: {}
      },
      network: {},
      totals: {
        workers: 0,
        hashrate: 0,
        wtxBlocks: 0,
        parentBlocks: 0
      }
    };

    // Get stats from each algorithm
    for (const [algo, server] of Object.entries(stratumServers)) {
      const algoStats = server.getStats();
      stats.pool.algorithms[algo] = algoStats;
      stats.totals.workers += algoStats.workers;
      stats.totals.wtxBlocks += algoStats.shares.wtxBlocks;
      stats.totals.parentBlocks += algoStats.shares.parentBlocks;
    }

    // Get WATTx network info
    try {
      const networkInfo = await rpcCall(config.wattxRpc, 'getblockchaininfo');
      const miningInfo = await rpcCall(config.wattxRpc, 'getmininginfo');
      stats.network = {
        height: networkInfo?.blocks || 0,
        difficulty: miningInfo?.difficulty || 0,
        hashrate: miningInfo?.networkhashps || 0
      };
    } catch (e) {}

    res.json(stats);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// API: Algorithm-specific stats
app.get('/api/algorithm/:algo', (req, res) => {
  const { algo } = req.params;
  const server = stratumServers[algo];

  if (!server) {
    return res.status(404).json({ error: 'Algorithm not found' });
  }

  res.json(server.getStats());
});

// API: Worker stats
app.get('/api/worker/:wtxAddress', (req, res) => {
  const { wtxAddress } = req.params;

  const workers = db.prepare(`
    SELECT * FROM workers WHERE wtx_address = ? ORDER BY last_seen DESC
  `).all(wtxAddress);

  const balances = db.prepare(`
    SELECT coin, SUM(balance) as balance, SUM(total_paid) as total_paid
    FROM balances WHERE wtx_address = ? GROUP BY coin
  `).all(wtxAddress);

  const recentShares = db.prepare(`
    SELECT algorithm, COUNT(*) as count, SUM(difficulty) as total_diff
    FROM shares WHERE wtx_address = ? AND timestamp > ? GROUP BY algorithm
  `).all(wtxAddress, Math.floor(Date.now() / 1000) - 3600);

  res.json({
    address: wtxAddress,
    workers,
    balances,
    recentShares
  });
});

// API: Blocks
app.get('/api/blocks', (req, res) => {
  const { chain, limit = 50, offset = 0 } = req.query;

  let query = 'SELECT * FROM blocks';
  const params = [];

  if (chain) {
    query += ' WHERE chain = ?';
    params.push(chain);
  }

  query += ' ORDER BY timestamp DESC LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));

  const blocks = db.prepare(query).all(...params);
  const total = db.prepare(`SELECT COUNT(*) as count FROM blocks ${chain ? 'WHERE chain = ?' : ''}`).get(chain || undefined);

  res.json({ blocks, total: total?.count || 0 });
});

// API: Payouts
app.get('/api/payouts/:address', (req, res) => {
  const { address } = req.params;
  const { coin, limit = 50 } = req.query;

  let query = 'SELECT * FROM payouts WHERE address = ?';
  const params = [address];

  if (coin) {
    query += ' AND coin = ?';
    params.push(coin);
  }

  query += ' ORDER BY timestamp DESC LIMIT ?';
  params.push(parseInt(limit));

  const payouts = db.prepare(query).all(...params);
  res.json({ payouts });
});

// ============================================================================
// /merged/ ROUTES — consumed by stats.html
// ============================================================================

const ALGO_TO_CHAIN = {
  randomx:   ['xmr'],
  ethash:    ['alt', 'octa'],
  scrypt:    ['ltc'],
  sha256d:   ['btc'],
  x11:       ['dash'],
  equihash:  ['zec', 'zen', 'btcz'],
  kheavyhash:['kas'],
};

function formatHashrate(h) {
  if (!h || h === 0) return '0 H/s';
  if (h >= 1e15) return (h / 1e15).toFixed(2) + ' PH/s';
  if (h >= 1e12) return (h / 1e12).toFixed(2) + ' TH/s';
  if (h >= 1e9)  return (h / 1e9).toFixed(2)  + ' GH/s';
  if (h >= 1e6)  return (h / 1e6).toFixed(2)  + ' MH/s';
  if (h >= 1e3)  return (h / 1e3).toFixed(2)  + ' KH/s';
  return h.toFixed(2) + ' H/s';
}

function timeAgo(ts) {
  if (!ts) return '—';
  const s = Math.floor(Date.now() / 1000) - ts;
  if (s < 60)  return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
}

// GET /merged/stats
app.get('/merged/stats', (req, res) => {
  try {
    const since24h = Math.floor(Date.now() / 1000) - 86400;
    const since10m  = Math.floor(Date.now() / 1000) - 600;

    const totalMiners = db.prepare(
      'SELECT COUNT(DISTINCT wtx_address) as c FROM workers WHERE last_seen > ?'
    ).get(since10m)?.c || 0;

    const wtxBlocks24h = db.prepare(
      "SELECT COUNT(*) as c FROM blocks WHERE chain = 'WTX' AND timestamp > ?"
    ).get(since24h)?.c || 0;

    const wtxEarned = db.prepare(
      "SELECT SUM(reward) as s FROM blocks WHERE chain = 'WTX' AND timestamp > ?"
    ).get(since24h)?.s || 0;

    const diffSum = db.prepare(
      'SELECT SUM(difficulty) as s FROM shares WHERE valid = 1 AND timestamp > ?'
    ).get(since10m)?.s || 0;
    const poolHashrate = diffSum * 4294967296 / 600;

    // Per-chain stats
    const chains = {};
    for (const [algo, chainIds] of Object.entries(ALGO_TO_CHAIN)) {
      const algoMiners = db.prepare(
        'SELECT COUNT(DISTINCT wtx_address) as c FROM workers WHERE algorithm = ? AND last_seen > ?'
      ).get(algo, since10m)?.c || 0;

      const algoBlocks = db.prepare(
        'SELECT COUNT(*) as c FROM blocks WHERE algorithm = ? AND timestamp > ?'
      ).get(algo, since24h)?.c || 0;

      const algoDiff = db.prepare(
        'SELECT SUM(difficulty) as s FROM shares WHERE algorithm = ? AND valid = 1 AND timestamp > ?'
      ).get(algo, since10m)?.s || 0;
      const algoHashrate = algoDiff * 4294967296 / 600;

      for (const chainId of chainIds) {
        chains[chainId] = {
          hashrate: formatHashrate(algoHashrate / chainIds.length),
          miners: algoMiners,
          blocks: algoBlocks,
          luck: '—'
        };
      }
    }

    res.json({
      miners: totalMiners,
      hashrate: formatHashrate(poolHashrate),
      blocks24h: wtxBlocks24h,
      wtx24h: wtxEarned.toFixed(4),
      chains
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /merged/miner/:addr
app.get('/merged/miner/:addr', (req, res) => {
  try {
    const addr = req.params.addr;
    const since24h = Math.floor(Date.now() / 1000) - 86400;
    const since10m  = Math.floor(Date.now() / 1000) - 600;

    const workers = db.prepare(
      'SELECT * FROM workers WHERE wtx_address = ? ORDER BY last_seen DESC'
    ).all(addr);

    if (!workers.length) {
      return res.status(404).json({ error: 'No mining activity found' });
    }

    const shares24h = db.prepare(
      'SELECT COUNT(*) as c, SUM(difficulty) as d FROM shares WHERE wtx_address = ? AND timestamp > ? AND valid = 1'
    ).get(addr, since24h);

    const badShares24h = db.prepare(
      'SELECT COUNT(*) as c FROM shares WHERE wtx_address = ? AND timestamp > ? AND valid = 0'
    ).get(addr, since24h)?.c || 0;

    const totalShares = (shares24h?.c || 0) + badShares24h;
    const rejectRate = totalShares > 0 ? ((badShares24h / totalShares) * 100).toFixed(1) + '%' : '0%';

    const diffSum = shares24h?.d || 0;
    const totalHashrate = formatHashrate(diffSum * 4294967296 / 86400);

    const wtxEarned = db.prepare(
      "SELECT SUM(reward) as s FROM blocks WHERE finder_wtx_address = ? AND chain = 'WTX' AND timestamp > ?"
    ).get(addr, since24h)?.s || 0;

    const lastShareTs = db.prepare(
      'SELECT MAX(timestamp) as t FROM shares WHERE wtx_address = ?'
    ).get(addr)?.t;

    // Per-chain breakdown
    const chains = {};
    for (const [algo, chainIds] of Object.entries(ALGO_TO_CHAIN)) {
      const algoShares = db.prepare(
        'SELECT COUNT(*) as c, SUM(difficulty) as d, MAX(timestamp) as last FROM shares WHERE wtx_address = ? AND algorithm = ? AND valid = 1 AND timestamp > ?'
      ).get(addr, algo, since24h);

      const balance = db.prepare(
        'SELECT SUM(balance) as b FROM balances WHERE wtx_address = ? AND coin = ?'
      ).get(addr, algo.toUpperCase())?.b || 0;

      const parentAddr = db.prepare(
        'SELECT parent_address FROM workers WHERE wtx_address = ? AND algorithm = ? LIMIT 1'
      ).get(addr, algo)?.parent_address;

      const algoHashrate = algoShares?.d
        ? formatHashrate(algoShares.d * 4294967296 / 86400)
        : '0 H/s';

      for (const chainId of chainIds) {
        chains[chainId] = {
          hashrate: algoHashrate,
          shares: algoShares?.c || 0,
          unpaid: balance.toFixed(6),
          lastShare: timeAgo(algoShares?.last),
          address: parentAddr || addr
        };
      }
    }

    // Payment history
    const payments = db.prepare(
      'SELECT * FROM payouts WHERE address = ? ORDER BY timestamp DESC LIMIT 20'
    ).all(addr).map(p => ({
      time: timeAgo(p.timestamp),
      coin: p.coin,
      amount: p.amount.toFixed(6),
      txid: p.txid,
      txUrl: p.txid ? `https://explorer.wattxchange.app/tx/${p.txid}` : null
    }));

    res.json({
      totalHashrate,
      shares24h: shares24h?.c || 0,
      rejectRate,
      wtxEarned24h: wtxEarned.toFixed(4),
      lastShare: timeAgo(lastShareTs),
      chains,
      payments
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /merged/blocks
app.get('/merged/blocks', (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 30, 100);
    const blocks = db.prepare(
      'SELECT * FROM blocks ORDER BY timestamp DESC LIMIT ?'
    ).all(limit).map(b => ({
      height: b.height,
      chain: b.chain.toLowerCase(),
      coin: b.chain,
      hash: b.hash,
      reward: b.reward.toFixed(4),
      time: timeAgo(b.timestamp),
      finder: b.finder_wtx_address
    }));

    res.json(blocks);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// API: Connection info
app.get('/api/connect', (req, res) => {
  const connections = [];

  for (const [algo, algoConfig] of Object.entries(config.algorithms)) {
    if (algoConfig.enabled) {
      connections.push({
        algorithm: algo,
        name: algoConfig.name,
        parentChain: algoConfig.parentChain,
        stratumUrl: `stratum+tcp://wtx-pool.wattxchange.app:${algoConfig.stratumPort}`,
        port: algoConfig.stratumPort,
        workerFormat: `WTX_ADDRESS.${algoConfig.parentChain}_ADDRESS.worker_name`,
        payoutCoins: ['WTX', algoConfig.parentChain]
      });
    }
  }

  res.json({
    pool: 'wtx-pool.wattxchange.app',
    connections,
    workerFormatExample: 'Wxxxxxxxxx.Yyyyyyyyyyy.rig1',
    payoutThresholds: config.payoutThreshold
  });
});

// ============================================================================
// START SERVERS
// ============================================================================

console.log('=== WATTx Multi-Algorithm Merged Mining Pool ===\n');

// Start stratum servers for each algorithm
for (const [algo, algoConfig] of Object.entries(config.algorithms)) {
  if (algoConfig.enabled) {
    const server = new StratumServer(algo, algoConfig);
    stratumServers[algo] = server;
    server.start();
  }
}

// Start API server
server.listen(config.apiPort, () => {
  console.log(`\nPool API running on port ${config.apiPort}`);
  console.log(`WebSocket available at ws://localhost:${config.apiPort}/ws`);
  console.log('\nStratum Ports:');
  for (const [algo, algoConfig] of Object.entries(config.algorithms)) {
    if (algoConfig.enabled) {
      console.log(`  ${algoConfig.name.padEnd(12)} -> :${algoConfig.stratumPort} (${algoConfig.parentChain} merged mining)`);
    }
  }
});

// Periodic stats recording
setInterval(() => {
  for (const [algo, server] of Object.entries(stratumServers)) {
    const stats = server.getStats();
    db.prepare(`
      INSERT INTO pool_stats (algorithm, timestamp, hashrate, workers, wtx_blocks, parent_blocks)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(algo, Math.floor(Date.now() / 1000), 0, stats.workers, stats.shares.wtxBlocks, stats.shares.parentBlocks);
  }

  // Broadcast stats update
  const allStats = {};
  for (const [algo, server] of Object.entries(stratumServers)) {
    allStats[algo] = server.getStats();
  }
  broadcast('stats', allStats);
}, 60000);
