module.exports = {
  apps: [
    {
      name: 'wattx-pool',
      script: 'server.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '256M',
      env: {
        NODE_ENV: 'production',
        PORT: 3001,
      },
      error_file: '../logs/pm2-pool-error.log',
      out_file: '../logs/pm2-pool-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
    },
  ],
};
