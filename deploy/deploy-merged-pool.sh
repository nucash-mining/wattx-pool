#!/bin/bash
# Deploy merged-pool.js to Oracle VPS
# Run from your local machine: bash deploy-merged-pool.sh

set -e
VPS="129.80.40.193"
REMOTE_DIR="/opt/wattx-pool/backend"

echo "=== Syncing backend files ==="
ssh root@$VPS "mkdir -p $REMOTE_DIR"
scp wattx-pool/backend/merged-pool.js root@$VPS:$REMOTE_DIR/
scp wattx-pool/backend/package.json  root@$VPS:$REMOTE_DIR/
scp wattx-pool/deploy/wattx-pool.service root@$VPS:/etc/systemd/system/

echo "=== Installing dependencies ==="
ssh root@$VPS "cd $REMOTE_DIR && npm install --production"

echo "=== Opening firewall ports ==="
ssh root@$VPS "
  # Oracle Cloud also requires security list rules in the console
  iptables -I INPUT -p tcp --dport 3334 -j ACCEPT   # RandomX / XMR
  iptables -I INPUT -p tcp --dport 3333 -j ACCEPT   # Ethash / ALT
  iptables -I INPUT -p tcp --dport 3336 -j ACCEPT   # SHA256d / BTC
  iptables -I INPUT -p tcp --dport 3337 -j ACCEPT   # Scrypt / LTC
  iptables -I INPUT -p tcp --dport 3340 -j ACCEPT   # X11 / DASH
  iptables -I INPUT -p tcp --dport 3341 -j ACCEPT   # Equihash / ZEC
  iptables -I INPUT -p tcp --dport 3342 -j ACCEPT   # kHeavyHash / KAS
  iptables-save > /etc/iptables/rules.v4
"

echo "=== Starting service ==="
ssh root@$VPS "
  systemctl daemon-reload
  systemctl enable wattx-pool
  systemctl restart wattx-pool
  sleep 2
  systemctl status wattx-pool --no-pager
"

echo ""
echo "=== Done ==="
echo "Check logs: ssh root@$VPS 'journalctl -u wattx-pool -f'"
echo ""
echo "IMPORTANT: Also open ports 3333-3342 in the Oracle Cloud Security List:"
echo "  OCI Console → Networking → VCN → Security Lists → Ingress Rules"
