#!/usr/bin/env bash
# Orange Pi (Armbian, arm64) validator setup — T-023.
# Run as root on the Pi. Idempotent-ish; re-runnable.
set -euo pipefail

VANTAGE="${VANTAGE:-rf-home-1}"
NATS_URL="${NATS_URL:-nats://10.99.0.1:4222}"
SINGBOX_VERSION="${SINGBOX_VERSION:-1.13.21}"
SB_SHA_ARM64="3e30b876c9a93c19e503e2a2d6249cf05e6a26766553d4b61e1daf48223f304f"

if [[ "$(uname -m)" != "aarch64" ]]; then
  echo "expected aarch64, got $(uname -m)" >&2
  exit 1
fi

apt-get update
apt-get install -y --no-install-recommends wireguard curl ca-certificates tar xz-utils

# unprivileged user
id proxyfarm >/dev/null 2>&1 || useradd -r -m -s /usr/sbin/nologin proxyfarm
mkdir -p /var/lib/proxyfarm /etc/proxyfarm
chown proxyfarm:proxyfarm /var/lib/proxyfarm

# sing-box (pinned, §15) — GPL binary, never linked
cd /tmp
curl -sfL -o singbox.tgz \
  "https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/sing-box-${SINGBOX_VERSION}-linux-arm64.tar.gz"
echo "${SB_SHA_ARM64}  singbox.tgz" | sha256sum -c -
tar -xzf singbox.tgz
install -m 0755 "sing-box-${SINGBOX_VERSION}-linux-arm64/sing-box" /usr/local/bin/sing-box

# worker binary: copy the CI-built proxyfarm-worker arm64 binary here
if [[ -f ./proxyfarm-worker ]]; then
  install -m 0755 ./proxyfarm-worker /usr/local/bin/proxyfarm-worker
else
  echo "NOTE: place the arm64 proxyfarm-worker binary at /usr/local/bin/proxyfarm-worker" >&2
fi

# systemd unit (vantage baked in)
sed "s/rf-home-1/${VANTAGE}/" proxyfarm-validator.service > /etc/systemd/system/proxyfarm-validator.service
systemctl daemon-reload

# wireguard client config — fill keys first, then:
#   cp wg-client.conf.example /etc/wireguard/wg0.conf && chmod 600 /etc/wireguard/wg0.conf
#   systemctl enable --now wg-quick@wg0

echo "done. next steps:"
echo "  1. /etc/wireguard/wg0.conf (keys + server endpoint), wg-quick@wg0"
echo "  2. place NATS validator credentials at /etc/proxyfarm/validator.creds (0600, proxyfarm:proxyfarm)"
echo "     server side: nsc add user -a ProxyFarm -n rf-home-1 --allow-pub 'jobs.validator.${VANTAGE}' ..."
echo "  3. systemctl enable --now proxyfarm-validator"
echo "  4. on the cluster: register the vantage via POST /api/admin/vantages (or wait for heartbeat self-registration)"
