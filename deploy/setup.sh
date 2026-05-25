#!/bin/bash
# WATTx Pool deployment setup script
# Run as root on Ubuntu 22.04

set -e

echo "=== Installing dependencies ==="
apt-get update -q
apt-get install -y nginx certbot python3-certbot-nginx nodejs npm

echo "=== Deploying pool backend ==="
mkdir -p /opt/wattx-pool/backend
cp -r /home/nuts/wattx-pool/backend/* /opt/wattx-pool/backend/
cd /opt/wattx-pool/backend
npm install --production

echo "=== Installing systemd service ==="
cp /home/nuts/wattx-pool/deploy/wattx-pool.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable wattx-pool
systemctl start wattx-pool

echo "=== Configuring nginx ==="
cp /home/nuts/wattx-pool/deploy/nginx-api.conf /etc/nginx/sites-available/wattx-api
ln -sf /etc/nginx/sites-available/wattx-api /etc/nginx/sites-enabled/wattx-api
nginx -t

echo "=== Obtaining SSL certificate ==="
echo "Make sure api.wattxchange.app DNS points to this server's IP first!"
read -p "Press enter when DNS is set..."
certbot --nginx -d api.wattxchange.app --non-interactive --agree-tos -m nucash.mining@gmail.com

systemctl reload nginx

echo ""
echo "=== Done ==="
echo "Pool API: https://api.wattxchange.app/merged/stats"
echo "Service:  systemctl status wattx-pool"
echo "Logs:     journalctl -u wattx-pool -f"
