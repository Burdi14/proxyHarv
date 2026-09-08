# Proxy list providers — info

Compiled 2026-08-17. All subscription links live-verified with HTTP 200.
Categories are grouped by the country / language context they were found in.

## 🇮🇷 Iran (Persian) — biggest, most active ecosystem

| Provider | Link / repo | Notes |
|---|---|---|
| EbraSha free-v2ray-public-list | github.com/ebrasha/free-v2ray-public-list | Persian author. Updated **every 15 min**, auto-removes dead configs. Protocols: SS, SSR, Trojan, VLESS, VMess, TUIC, Hysteria2. TG bot @AbdalV2rayBot |
| Sakha1370 OpenRay | github.com/sakha1370/OpenRay | Hourly pipelines, per-country lists, per-protocol lists, Iran-specific (MCI/IRANCELL/TCI) top100, site-access checks. Very active (pushed 2026-08-17) |
| GO_V2rayCollector | github.com/HosseinKoofi/GO_V2rayCollector | Iranian collector; mixed_iran.txt + per-protocol files |
| NiREvil/vless | github.com/NiREvil/vless | Persian, 14k commits, updated hourly. Provider panel table + per-country subs + Cloudflare IPs |
| itsyebekhe/PSG | github.com/itsyebekhe/PSG | Persian collector; meta mix/reality subs; also TelegramV2rayCollector (PHP), tpro/warp |
| vless-reality.github.io | github.com/vless-reality | Chinese+Persian free-airport collection, daily auto-update, clash+v2 formats |
| IRCF | ircfspace.github.io | Iranian config portal (tconfig/location), gathers from SoliSpirit/v2ray-configs |

Persian Telegram channels found: v2rayNGconfig, AbdalV2rayBot, YeBeKhe, DarknessShade/Sub, ArshiaComPlus/V2rayExtractor, 10ium/free-sub-link.

## 🇷🇺 Russia / 🇺🇦 Ukraine (anti-censorship, white-list bypass)

| Provider | Link / repo | Notes |
|---|---|---|
| igareck/vpn-configs-for-russia | github.com/igareck/vpn-configs-for-russia | BLACK_VLESS_RUS + Vless-Reality-White-Lists-Rus-Mobile (CIDR, mobile operator white-list bypass). Timestamps in files |
| kort0881 vpn-checker-backend | github.com/kort0881 | RU_Best checked lists (ru_white_part3.txt). Also vpn-vless-configs-russia daily rotation |
| ByeWhiteLists | github.com/ByeWhiteLists/ByeWhiteLists2 | Russian white-list bypass VLESS configs |
| 3inker/v2ray-subscription | github.com/3inker/v2ray-subscription | Russian collector+checker; subs/all_ru.txt and all_not_ru.txt (19 RU / 413 non-RU nodes) |
| AvenCores/goida-vpn-configs | github.com/AvenCores/goida-vpn-configs | Ukrainian/Russian "goida" configs mirror (githubmirror/26.txt) |
| Chumbayoumba/free-telegram-proxy-russia-2026 | github.com/Chumbayoumba/free-telegram-proxy-russia-2026 | Free MTProto proxies for Telegram in Russia (t.me/proxy links) |

Russian-language sites/articles found: vless.download, keys-happ.vercel.app (VLESS Reality keys), glushilok.net (free MTProto proxy for TG, blocked-sites checker, WhatsApp proxy), itdog.info (ITDog Telegram channel for #clients_vless_ss).

## 🇮🇱 Israel

- OpenRay country list output/country/IL.txt (verified live).
- General Hebrew-language daily proxy portals: proxy5.net/he (HTTP/HTTPS/SOCKS4/SOCKS5, auto-checked every 30 min, TXT/CSV/JSON export), free-proxy-list.net.
- Israeli "OpenWebMirror" (openwebmirror.com) offers free VPN for Israeli sites — no raw subscription URL.

## 🇨🇳 China (science-internet / 科学上网)

| Provider | Link / repo | Notes |
|---|---|---|
| free-nodes/v2rayfree | github.com/free-nodes/v2rayfree | 13.4k stars, daily updated sub, 6-hour refresh |
| Pawdroid/Free-servers | github.com/Pawdroid/Free-servers | 18.6k stars, updated every 6h, multi-language subs |
| mahdibland/V2RayAggregator | github.com/mahdibland/V2RayAggregator | Aggregates ~28 sources (pojiezhiyuanjun, freefq, mfuu, xiayaowong, anaer, ermaozi, snapdragonlee, hermanb001, LonUp...), tested/filtered |
| UIforFreedom | github.com/UIforFreedom/Free_Proxy_Nodes | CDN-hosted sing-box subs, checked every 30 min |
| ripaojiedian/freenode | github.com/ripaojiedian/freenode | Chinese free node pool |
| Au1rxx/free-vpn-subscriptions | github.com/Au1rxx/free-vpn-subscriptions | ~2000 curated / ~8000 verified nodes, hourly refresh, clash/singbox/v2ray formats, per-country shards |

Chinese portal articles/sites found: fq.dog, jcnode.com, v2raya.net (昌南魔法学院), clashxmeta.org — mostly paid airport (机场) reviews; the GitHub pools above are the real subscription sources.

## 🇹🇷 Turkey / Arabic

- proxy5.net/ar + free-proxy-list.net/ar — Arabic daily HTTP/HTTPS/SOCKS lists (checked every 10–30 min), include Iran proxies.
- General Arabic article (arabes1.com) ranking free proxy sources: ProxyScrape, free-proxy-list.net, Geonode, Spys.one, Hidemy.name, ProxyNova, Proxifly, TheSpeedX/PROXY-List.
- Turkish-language search mostly returned the same global GitHub aggregators (NiREvil/vless has a Turkish-community audience too).

## 🌍 General classic HTTP/SOCKS lists (verified raw)

- TheSpeedX/PROXY-List — http/socks4/socks5, 46k+ entries, daily
- roosterkid/openproxylist — HTTPS/SOCKS4/SOCKS5 raw
- clarketm/proxy-list — merged raw list
- ShiftyTR/Proxy-List — https/socks5
- zloi-user/hideip.me — https list
- proxifly/free-proxy-list — 5000+ proxies, JSON+TXT, auto-update
- hookzof/socks5_list — socks5 host:port, updated daily

## Security notes

- Free nodes are untrusted: traffic is visible to node operators. Use only for geo-unblocking / light browsing, never banking/logins.
- Base64 subscription files decode to vless/vmess/ss/trojan/hysteria2 URIs.
- Some Russian "white-list" lists are operator/CIDR-specific (MTS, Megafon, Tele2, Beeline, Yota).

---

# Update 3 — more Africa proxies (2026-08-17)

Goal: add more African hosts specifically, then rebuild the map.

## What was added

- **New aggregator pools (contain African nodes):** Alirewa/V2ray-Configs (config.txt, sub1-3.txt), 0xRadikal/Free-v2ray-Configs (all/configs.txt, all/configs_base64.txt, all/clash.yaml, top100.txt), MatinGhanbari/v2ray-configs (all_sub.txt, super-sub.txt) — discovered via Google ("github free v2ray config Egypt Nigeria South Africa subscription raw"). All live-verified.
- **proxifly per-country files** for 16 African countries (ZA, NG, EG, KE, UG, SN, TZ, GH, TG, BI, BW, LS, NA, ZM, ZW, BF) — live `protocol://host:port` lines.
- **10ium ScrapeAndCategorize** Seychelles + SouthSudan pools (SC contributed the most configs).
- **v2nodes.com** per-country ZA subscription + global `country/all` sub.
- **proxyscrape v3 API** with Africa country filter (http, socks4, socks5) — live host:port.
- `download.py` now also accepts `socks4://`, `http://`, `https://` protocol-prefixed lines (proxifly format). `geolocate_final.py` rewritten self-contained (no longer needs deleted `geo_merged.json`).

## Result (2026-08-17)

Pool grew 42,990 → **48,573 unique hosts** (234,843 config lines). Africa geolocated configs 1,103 → **1,388** across **26 countries** / 62 cities (was ~21 / 52). Top African contributors: SC 568 (Seychelles datacenter nodes), ZA 320, KE 100, ZW 97, EG 91, DZ 52, NG 48.

| Continent | % of configs (before → after) |
|---|---|
| Europe | 43.1% → 40.9% |
| North America | 30.6% → 32.4% |
| Asia | 21.6% → 22.1% |
| Other | 1.9% → 2.1% |
| Africa | 0.8% → 0.8% (1,103 → 1,388 configs) |
| South America | 1.4% → 1.2% |
| Oceania | 0.5% → 0.6% |
| Cities | 1,808 → 1,969 |

Africa is still a small share of the global pool — few free-node projects target African countries (confirmed by Google AI overview: "nodes rarely target specific African countries like Egypt, Nigeria, or South Africa exclusively"). Every additional reachable African source (proxifly country files, 10ium SC/SS, v2nodes ZA, proxyscrape country filter, aggregators) was added; the % reflects real supply, not fabrication.

Map rebuilt: `proxies_map.html` now 1,969 city markers. `subs.txt` now 135 live-verified links (all HTTP 200 on 2026-08-17; BI/BF proxifly country files were removed after going 404).

# Update 2 — continent balance + map (2026-08-17)

Goal: spread the pool across continents and produce a per-city map.

## What was added

New providers found while hunting for regional balance (all live-verified):

| Provider | What it adds |
|---|---|
| 10ium/ScrapeAndCategorize | Per-country config pools (output_configs/<Country>.txt) |
| roosterkid/openproxylist (openproxylist.com) | V2Ray raw + base64 subscription, minute-level checks, per-country page |
| ShatakVPN/ConfigForge-V2Ray | Per-country speed-tested subs (40 countries incl. au, br, il, it, ru, ua, ir, in, tr) |
| sunny9577/proxy-scraper | Scraped open HTTP/SOCKS proxies, pure host:port |
| 10ium/free-config, MihomoSaz, base64-encoder | Extra merged Persian/global pools |
| 4n0nymou3/multi-proxy-config-fetcher, Argh94/Proxy-List, Epodonios base64 | More aggregate lists |
| gitverse.ru (MishaLan, ru-wbl KvRuVPN) | Russian whitelist-bypass mirrors |
| OpenRay per-country files (CO, CL, MX, PE, NZ, KE + EG/NG/ZA/BR/AR/AU/IT) | Regional coverage for Africa/South America/Oceania |

Country-focused sources added for **Egypt**: OpenRay `output/country/EG.txt` + `openproxylist.com/v2ray/country/EG/`; for **Italy**: OpenRay IT + ShatakVPN it + 10ium Italy; for **South America**: OpenRay BR/AR/CO/CL/MX/PE + ShatakVPN br; for **Africa**: OpenRay EG/NG/ZA/KE + 10ium SouthAfrica; for **Oceania**: OpenRay AU/NZ + ShatakVPN au.

## Continent distribution (measured, 142,505 configs in 1,808 cities, 27,550 geolocated hosts)

| Continent | % of configs | cities |
|---|---|---|
| Europe | 43.1% | 510 |
| North America | 30.6% | 269 |
| Asia | 21.6% | 677 |
| Other | 1.9% | 39 |
| South America | 1.4% | 247 |
| Africa | 0.8% | 52 |
| Oceania | 0.5% | 14 |

Reality check: Europe/NA/Asia genuinely host most open proxy nodes. Africa, South America and Oceania were boosted with every reachable dedicated country source (Egypt, Nigeria, Kenya, South Africa, Brazil, Argentina, Colombia, Chile, Mexico, Peru, Venezuela, Australia, NZ), but the real open-proxy supply there is thin. Percentages reflect actual geolocated supply — no fabrication.

## Update 4 — India / local-community hunt (2026-08-17)

Goal: find proxy lists in local African / Indian forums & chats (Reddit, Telegram, local tech communities) and assess whether the idea is viable.

**Result: idea partially works — chat/forum scraping yields little, but a real daily-updated India source was found.**

| Attempt | Outcome |
|---|---|
| Reddit (r/Egypt, Nigeria, India v2ray) | Google AI overview: free configs for those regions are unstable/rarely cataloged; no usable lists. Reddit JSON API blocks curl (403) |
| Nairaland (Nigerian forum) | Only VPN-app promotion spam, zero config lists |
| Telegram channels (V2rayNG3, TRP Tunnel, MD Proxy VPN, AlphaV2ray, free4allVPN) | Unreachable — `t.me` connection timeout from this network |
| Telegram proxy repos (V2RAYCONFIGSPOOL/TELEGRAM_PROXY_SUB) | Only `t.me/proxy?` MTProto links, 0 V2Ray/SS lines — not useful |
| Iran collectors (V2RayRoot/V2RayConfig, miladtahanian/Config-Collector) | Iran-focused, already covered by Persian ecosystem |
| Romaxa55/MegaV_Public | subs/all.txt only 1,014 bytes — tiny, skipped |
| **indiavpn.github.io** | **ADDED** — Chinese-language "India VPN" site, updates daily (dated files 0..4-YYYYMMDD.txt/.yaml under uploads/2026/08/). Pulled via raw.githubusercontent.com (stable). |

Added 10 links to `subs.txt` (indiavpn 0-…4-20260809 .txt/.yaml). New totals: **49,704 unique hosts / 244,665 config lines**. India went from sparse to **52 cities / 1,045 configs** (Mumbai 238, New Delhi 154, Hyderabad 135, Bengaluru 88, Jaipur 81, Navi Mumbai 64, Noida 52, Chennai 34 …). Africa now 1,430 configs / 63 cities / 25 countries.

Map: `proxies_map.html` regenerated — **1,973 city markers**, zoom-scaling radii verified in-browser (radius 5.9px @ z1 → 18px @ z14, clamp [2,18]).

## Map

`proxies_map.html` — interactive folium map, 1,973 city markers, circle size = # proxy configs in the city, color = continent (legend included). Regenerate with:

```
venv/bin/python download.py        # pulls all subs.txt links → nodes.json
venv/bin/python geolocate_final.py # ip-api.com batch geolocation → cities.json
venv/bin/python map.py             # builds proxies_map.html
venv/bin/python stats.py           # continent table
```

Dependencies: requests, PyYAML, geoip-free (ip-api.com), folium. See download.py / geolocate_final.py / map.py / stats.py.

## Extra reference (from first pass)

- `nodes.json` — 49,704 unique hosts / 244,665 config lines across all sources.
- `subs.txt` now has 179 subscription links (all HTTP 200 on 2026-08-17).
