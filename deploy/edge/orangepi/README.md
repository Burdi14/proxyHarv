# Edge: Orange Pi validator (T-023)

The RF vantage point is a stock Orange Pi (Armbian, arm64) on a home ISP.
It dials **out** to the VPS over WireGuard (`wg-client.conf.example`) and
speaks to cluster NATS only through the tunnel — no public ports (§9.1).

Files:

| File | Purpose |
|---|---|
| `install.sh` | Pi bootstrap: wireguard, curl, pinned sing-box 1.13.21 (SHA-verified), `proxyfarm` user |
| `proxyfarm-validator.service` | systemd unit: `worker --role=validator --vantage=rf-home-1` |
| `wg-client.conf.example` | WireGuard client template (fill keys + server endpoint) |

Credentials: the validator uses its own NATS nkey (`/etc/proxyfarm/validator.creds`,
0600) with rights limited to `jobs.validator.rf-home-1` + `results.*` (§9.2).
Revoking that one credential isolates a compromised Pi without touching the
cluster. Heartbeats land in the `REGISTRY` KV; the coordinator registers the
vantage automatically (`UpsertVantage`) and starts publishing validation jobs.
