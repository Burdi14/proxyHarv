# ProxyFarm — архитектура

Статус: спроектировано, ждет реализации.
Дата: 2026-08-17. Язык реализации: Go. Оркестрация: k3s (Kubernetes).

---

## 1. Цель и контекст

Веб-приложение для коммьюнити:

1. интерактивная карта мира с проверенными прокси-узлами;
2. клик по городу → выбор узла → получение конфига в формате share-link (`vless://`, `vmess://`, `ss://`, `trojan://`, `hysteria2://`) или подписки целиком;
3. постоянный сканирующий пайплайн, который из ~50k найденных хостов (244k конфигов) отбирает малый список **гарантированно работающих** узлов;
4. двухэтапная проверка с разных vantage-точек: облако (VPS вне РФ) → домашняя сеть РФ (Orange Pi).

Зафиксированные решения:

| Вопрос | Решение |
|---|---|
| Доступ | Публичный, read-only, без регистрации. Приоритет — безопасность инфраструктуры, не ограничение пользователей |
| Карта | Только узлы, прошедшие проверку (статус ниже) |
| Сервер | Один VPS вне РФ (k3s, все control-компоненты) |
| Клиенты конфигов | v2rayN / v2rayNG (share-links + base64-подписка) |
| Валидатор РФ | Orange Pi (Armbian, arm64), подключение наружу через WireGuard |

---

## 2. Обзор

```
                    ┌────────────────────────────────────────────────┐
                    │  VPS вне РФ — k3s (single-node, до поры)        │
                    │                                                │
 subs (DB) ──────▶  │  fetcher (CronJob)                             │
                    │    │ raw.import (stream)                        │
                    │    ▼                                           │
                    │  coordinator (Deployment, leader-lease)         │
                    │    │ jobs.l0 / jobs.l1 / jobs.l2  (streams)     │
                    │    ▼                                           │
                    │  worker --role=tester (Deployment × N, KEDA)    │
                    │    │ results.* (streams)                        │
                    │    ▼                                           │
                    │  coordinator → postgres (единственный писатель) │
                    │    │ publishes (KV, immutable vN)               │
                    │    ▼                                           │
 Пользователь ◀───  │  api (Deployment) + Leaflet-фронтенд (static)   │
                    │    HTTPS only, read-only, rate-limited          │
                    └───────────────┬────────────────────────────────┘
                                    │ WireGuard tunnel (no public ports)
                    ┌───────────────▼────────────────┐
                    │  Orange Pi (Armbian, РФ ISP)   │
                    │  worker --role=validator       │
                    │  --vantage=rf-home-1           │
                    └────────────────────────────────┘
```

Принципы:

- **Воркеры stateless.** tester и validator — один бинарник `worker`, роль задается флагом. Скейлятся подами или голым systemd — без разницы.
- **Единственный писатель в БД — coordinator.** Воркеры говорят только с NATS. Пай ничего не знает о postgres.
- **Очередь одна — NATS JetStream.** Отдельного jobs-модуля в БД нет (уточнение к ранней версии: таблица `jobs` не нужна, JetStream покрывает и batch-pull, и redelivery, и DLQ).
- **Публикации иммутабельны.** Новая версия списка = новый ключ `publishes/vN`; откат = вернуться на `vN-1`.

---

## 3. Компоненты

| Компонент | Kind | Обязанности |
|---|---|---|
| `fetcher` | CronJob | Читает `sources` из БД, качает подписки, парсит share-links → пачки событий в `raw.import` |
| `coordinator` | Deployment (replica=1, lease) | Потребляет `raw.import`/`results.*`; ведет узлы в БД; строит план проверок; агрегирует скор; публикует версии списков; следит за жизнью vantage-точек |
| `worker (tester)` | Deployment, autoscale | Пирамида проверок L0/L1/L2 (см. §4) через sing-box; публикует метрики в Prometheus |
| `worker (validator)` | edge (systemd на Пай) | То же, что tester, но из РФ-сети, по джобам `jobs.validator.rf-*`; только L1/L2 |
| `api` | Deployment | REST для карты и конфигов, SSE о новых публикациях, base64-подписка для v2rayN, админ-эндпоинты под токеном |
| `frontend` | ConfigMap/static | Leaflet + vanilla JS (или SvelteKit позже): карта городов, панель узлов, QR/copy |
| `postgres` | StatefulSet | Единственный источник правды |
| `nats` | StatefulSet | JetStream: streams + KV; НЕ публикуется в интернет |

---

## 4. Пайплайн проверки (пирамида)

Ключевая правка относительно ранней версии: **не гонять полный sing-box-проб на все 50k хостов**. Три ступени по стоимости:

| Ступень | Что делает | Инструмент | Пропускная | Стоимость |
|---|---|---|---|---|
| **L0 prefilter** | TCP-dial host:port с таймаутом 3s (для TCP-транспорта) | чистый Go (`net.Dialer`) | тысячи/мин | копейки |
| **L1 probe** | Полный протокол: поднять sing-box outbound → HTTP GET `generate_204` через туннель → latency | sing-box (exec, batch) | десятки/мин | средне |
| **L2 speedtest** | Скачивание N МБ через туннель | sing-box + `speed.cloudflare.com/__down` | единицы/мин | дорого |

Правила:

- **L0** проходит только TCP-транспорт (tcp/grpc/ws/httpupgrade/h2). QUIC/UDP-протоколы (hysteria2, tuic) идут сразу в L1 — UDP-dial ненадежен как префильтр.
- **L1** — определение «жив». Отсюда узел попадает на карту (класс `cloud`).
- **L2** — только топ-K кандидатов по скору (например, 300 шт. на цикл), нужно для ранжирования, не для aliveness.
- **validator (РФ)** гоняет L1 для всех `cloud`-живых + L2 для топ-50. Прошедшие получают класс `rf` — основной фильтр карты.

Политика ретестов (координатор):

| Статус узла | Период перепроверки |
|---|---|
| жив (L1 ok) | 6 ч |
| жив < 24ч | 3 ч |
| умер | backoff 1ч → 2ч → 4ч → … → 48ч (cap) |
| мертв > 14 дней | удалить из БД (остается в истории проверок) |

Публикация списка: по расписанию (каждые 6 ч) ИЛИ при изменении > 20% состава — что раньше. Каждая публикация: версия, checksum, размер, классы узлов.

---

## 5. Транспорт: NATS JetStream

Streams (retention=interests/workQueue где уместно, дубликаты отсекаются по `Nats-Msg-Id = job_id`):

| Stream | Издатель → потребитель | Содержимое |
|---|---|---|
| `RAW` | fetcher → coordinator | распарсенные share-links пачками по 100 |
| `JOBS_L0` | coordinator → tester | батч {node_id, host, port, transport} |
| `JOBS_L1` | coordinator → tester | батч {node_id, singbox_outbound_json} |
| `JOBS_L2` | coordinator → tester | {node_id, outbound_json, bytes} |
| `JOBS_VAL` | coordinator → validator (`jobs.validator.rf-home-1`) | топ-N cloud-живых |
| `RESULTS` | tester/validator → coordinator | {node_id, vantage, stage, ok, latency_ms, speed_mbps, error, ts} |

KV buckets:

| Bucket | Назначение |
|---|---|
| `PUBLISHES` | иммутабельные версии списков (vN): base64-подписка + meta JSON + checksum; api делает KV watch → SSE |
| `REGISTRY` | реестр vantage-точек: heartbeat воркеров, конфиг; coordinator видит офлайн валидатора |

Доставка: batch-pull (fetch 20, ack all), `ack_wait=5m`, `max_deliver=3` → потом в DLQ-стрим для разбора. Идемпотентность результатов — UNIQUE(node_id, vantage, stage, ts_bucket) в БД.

Sing-box исполняется **внешним бинарником** (exec на батч из N outbound-портов): GPL-код sing-box не линкуется в наш бинарник, версия пинится в образе, checksum проверяется сборкой.

---

## 6. Модель данных (postgres)

```sql
sources(id, url, kind, enabled, last_fetch_at, last_status, note)

nodes(
  id, uri_hash UNIQUE,      -- дедуп по канонизированному uri
  uri, protocol, transport, -- vless/vmess/ss/trojan/hy2 ; tcp/ws/grpc/quic/...
  host, host_ip,            -- host из uri + резолв (кэш)
  cc, city, lat, lon,       -- гео из mmdb
  status,                   -- unknown|alive|dead
  best_class,               -- none|cloud|rf
  score,                    -- агрегат (см. §7)
  first_seen, last_seen, last_l1_ok_at
)

node_checks(
  id, node_id, vantage, stage,      -- vantage = 'cloud-eu-1' | 'rf-home-1'
  ok, latency_ms, speed_mbps, error_code, ts,
  UNIQUE(node_id, vantage, stage, ts_bucket)   -- идемпотентность redelivery
)

vantages(id, kind, region, enabled, last_heartbeat_at)
publishes(version, created_at, class_filter, node_count, checksum, kv_key)
```

Ретеншн: `node_checks` — партиционирование по месяцу, сырые проверки храним 30 дней (агрегаты в `nodes` живут вечно); JetStream limits — 1–5 ГБ на стрим.

Гео: **локальный DB-IP City Lite mmdb** в coordinator (MMDB-формат, читается тем же ридером что GeoLite2; ключей и регистрации не требует; лицензия CC-BY — attribution в футере карты). Обновление mmdb — CronJob `dbip-update` (прямая ссылка `download.db-ip.com/free/dbip-city-lite-YYYY-MM.mmdb.gz`, фолбэк на прошлый месяц). MaxMind GeoLite2 остаётся drop-in альтернативой без правок кода.

---

## 7. Скор и попадание на карту

```
score = w1 · recency(last_l1_ok_at)
      + w2 · success_rate(последние 10 L1 на vantage)
      + w3 · (1 / latency_ms)
      + w4 · normalize(speed_mbps)          -- если L2 был
      + w5 · rf_bonus                       -- прошел РФ-валидацию
```

Веса — конфиг coordinator (ConfigMap), стартовые: 0.2/0.3/0.2/0.2/0.1.

Статусы на карте (легенда): `rf` (зеленый) — проверено из РФ; `cloud` (синий) — проверено из облака. Фильтр по классу/протоколу/стране в UI. Мертвые не показываются (решение зафиксировано).

---

## 8. API (публичный, read-only)

| Endpoint | Что отдает |
|---|---|
| `GET /api/cities?class=rf\|cloud` | агрегаты живых: city, lat, lon, count, best_latency, protocols[] |
| `GET /api/nodes?city=&class=&proto=` | топ узлов: id, proto, cc/city, latency, speed, last_check, class |
| `GET /api/node/{id}` | share-link + ready-to-import JSON (sing-box outbound) |
| `GET /api/sub?class=rf&proto=&limit=50` | **base64-подписка для v2rayN/v2rayNG** — один URL, автoupdate клиентом |
| `GET /api/publishes` | список версий |
| `GET /api/events` | SSE: событие `publish` при новой версии |
| `POST /api/admin/*` | CRUD sources/vantages; заголовок `X-Admin-Token` (Secret), не публично |

Имена узлов в подписке: `{CC}-{City}-{latency}ms-{class}` — читаемо в v2rayN.

Rate limit: nginx ingress `limit-rps` на /api/*; статика без лимита.

Фронтенд: Leaflet (CDN), CircleMarker с масштабируемым радиусом (опыт из `proxies_map.html`), кластеризация `leaflet.markercluster` при >2k точек.

---

## 9. Безопасность (приоритет по требованию)

Атакующая поверхность наружу — **только HTTPS api**. Все остальное не публикуется.

1. **Сеть**
   - postgres, nats: `NetworkPolicy` — доступны только из namespace; никаких NodePort/LoadBalancer.
   - Orange Pi ↔ NATS: WireGuard-туннель (Pi инициирует исходящее подключение, на сервере только один WG-порт UDP). Публичного NATS-порта нет.
   - ingress: TLS (cert-manager, Let's Encrypt), HSTS.
2. **Идентификация воркеров**
   - Валидатору — свои nkey/credentials NATS (можно отозвать независимо). Компрометация Пай ≠ компрометация кластера: права только на свои subject-ы `jobs.validator.rf-home-1`, `results`.
3. **Пробы через недоверенные прокси**
   - Через туннель запрашиваем только нейтральные URL: собственный `probe.{domain}/generate_204` + `speed.cloudflare.com`. Никаких кред, кук, логина.
   - Ответ 204 ожидаем с exact-match заголовком; произвольный контент = не валиден (защита от подменяющих прокси).
   - L2-трафик ограничен (N МБ на узел), чтобы не раздавать чужой трафик.
4. **Гигиена сканирования**
   - Ограничение конкурентных коннектов tester'а (например, 200), случайный порядок узлов: не выглядим как сканер-«штопор» для абьюз-жалоб на VPS.
5. **Контейнеры**
   - distroless/static образы, non-root, read-only rootfs, `allowPrivilegeEscalation: false`, seccomp RuntimeDefault.
   - Версии sing-box пинятся SHA256 в Dockerfile.
6. **Приватность пользователей**
   - api не логирует IP клиентов (access-log off для /api/*), публикация — статические данные. Мы не храним кто и что скачал.
7. **Секреты**
   - k8s Secrets (+ sealed-secrets если появится GitOps). В репо — никогда.
8. **Бэкапы**
   - pg_dump CronJob раз в сутки → S3-совместимое хранилище; снапшоты `PUBLISHES` KV (это и есть продукт) туда же. RPO 24ч — приемлемо: списки регенерируются пайплайном.

---

## 10. Эксплуатация и скейлинг

- **k3s single-node** на старте: всё в одном, KEDA-объекты в манифестах с `min=max=2` (безопасно).
- Рост → добавить ноды кластера: `tester` по-настоящему скейлится KEDA по метрике JetStream consumer lag (`natsjetstream` scaler). Пайплайн изменений не требует.
- Новая vantage-точка (другой провайдер РФ, другой город, чужой VPS): поставить `worker --role=validator` + выдать nkey → зарегистрировать в `vantages` → coordinator сам начнет слать джобы. Это и есть «расширение нодами».
- Мониторинг: kube-prometheus-stack; метрики воркеров (проб/сек, latency проб, ошибки по типам), алерты: consumer lag > N, публикация старше 12ч, валидатор офлайн > 1ч.
- Логи: stdout → Loki. Никаких чувствительных данных в логах (только node_id, не uri с секретами серверов).

---

## 11. Аудит архитектуры: что проверено и что поправлено

| # | Пункт | Вердикт |
|---|---|---|
| 1 | Контрол-плейн + stateless-воркеры + один писатель в БД | ✅ подтверждено |
| 2 | «Проверять все узлы sing-box'ом» | ❌ исправлено: пирамида L0/L1/L2 — иначе 50k полных проб не влезают в цикл |
| 3 | `subs.txt` в ConfigMap | ❌ исправлено: таблица `sources` + админ-API, добавление без редеплоя |
| 4 | Геолокация через ip-api.com | ❌ исправлено: локальный GeoLite2 mmdb (без rate-limit и внешней зависимости) |
| 5 | Таблица `jobs` в БД (было в раннем варианте) | ❌ упразднено: JetStream единственная очередь |
| 6 | NATS «за TLS наружу» | ❌ исправлено: WireGuard-туннель, публичных портов кроме 443/22(WG) нет |
| 7 | sing-box как библиотека | ⚠️ лицензия GPL: только exec внешнего бинарника, без линковки |
| 8 | HPA/KEDA на single-node | ✅ оставить в манифестах, min=max до появления второй ноды |
| 9 | Доступ «для коммьюнити» | ✅ публичный read-only + rate limit; безопасность инфраструктуры ≠ регистрация пользователей |
| 10 | Карта всех найденных | ✅ исправлено по решению: только прошедшие L1 (+класс cloud/rf) |
| 11 | MaxMind (нужен аккаунт + license key) | ✅ заменён: DB-IP City Lite — без ключей и регистрации, тот же MMDB-ридер, CC-BY attribution в футере |

---

## 12. Риски и открытые вопросы

- **Живучесть публичных нод** — конфиги из открытых подписок умирают быстро (часы). Пайплайн с 6ч-циклом это покрывает, но подписка в v2rayN должна обновляться часто (указываем `profile-update-interval` в uri, где поддерживается).
- **Одна РФ-точка** — валидация «работает из РФ» на деле «работает из моего провайдера». Лечится добавлением валидаторов (архитектура уже позволяет), в подписке честно пишем класс.
- **Подписки-источники протухают** — fetcher отмечает `last_status`, coordinator алертит при >30% мертвых источников.
- **Открытый вопрос:** квоты VPS (трафик L2-тестов) — прикинуть на практике, настроить `bytes` в L2 и периодичность.
- **Открытый вопрос:** нужен ли второй валидатор вне дома (другой РФ-провайдер) с первого дня — рекомендую с MVP не тянуть, но держать в роадмапе.

---

## 13. План реализации

| Этап | Содержимое | Критерий готовности |
|---|---|---|
| **M0. Ядро** | `internal/parse` (share-links → sing-box outbound JSON), `cmd/worker` L0+L1, `internal/natsb` | воркер локально проверяет 100 конфигов из `nodes.json` |
| **M1. Пайплайн** | fetcher, coordinator, postgres, JetStream streams | полный цикл на VPS без k8s (compose) — публикация v1 |
| **M2. Карта** | api + Leaflet, `/api/sub`, SSE | тыкаю в карту → получаю рабочий vless:// |
| **M3. k8s** | k3s, все Deployment/StatefulSet, KEDA, NetworkPolicy, TLS | прод на VPS, алерты в мониторинге |
| **M4. Валидатор** | Пай + WireGuard + nkey, класс `rf` | на карте появляются `rf`-узлы |
| **M5. Коммьюнити** | rate limits, подписка-страница, второй валидатор, L2 тюнинг | отдали ссылку людям |

Текущие Python-скрипты (`download.py`, `geolocate_final.py`, `stats.py`, `map.py`) остаются как референс и источник сидовых данных (`sources` сеется из `subs.txt`).

---

## 14. Контракты данных (норматив: реализация не имеет свободы трактовки)

Секции 14–21 написаны для передачи задач исполнителям (в т.ч. LLM-агентам). При расхождении «раннего текста» и §14–21 — прави́ло: **верны §14–21**.

### 14.1 Общие правила сериализации

- JSON, UTF-8, поля в `snake_case`. Отступов нет (compact) в NATS-сообщениях; API может отдавать pretty.
- Время — строка RFC3339 UTC (`2026-08-17T12:34:56Z`). Unix-timestamp не используется.
- Идентификаторы: `node_id`/`source_id` — int64 из БД; `job_id` — UUIDv4.
- Обязательные поля всегда присутствуют (nullable → `null`, не отсутствие поля).

### 14.2 Канонизация `uri_hash` (дедуп)

`uri_hash = "sha256:" + hex(sha256(canonical))`, canonical строится так:

1. Обрезать пробелы и BOM (`\ufeff`).
2. scheme → lowercase. Fragment (`#...`) — отбросить (это имя узла).
3. Query: разбить по `&`, пустые пары выбросить, **отсортировать пары как сырые строки** (без decode/re-encode), склеить `&`.
4. Userinfo — как есть (до последнего `@` в authority).
5. Host → lowercase; port: отбросить, если равен дефолту scheme (`http:80`, `https:443`, `socks:1080`), иначе оставить; IPv6 — с квадратными скобками.
6. Path — как есть (может быть пустым).
7. `canonical = scheme"://"[userinfo"@"]host"[":"port]path["?"query]` (компонент добавляется только если непустой).
8. **Исключение `vmess://`**: base64 (std ИЛИ urlsafe, добить `=`), JSON; canonical = `vmess|v|add|port|id|net|path|host|tls|sni|alpn|scy` (пустые поля — пустые строки; `ps` игнорируется; add → lowercase). Ошибка декода → `opaque|<raw uri>`.

Эталон: `fixtures/golden_uri_hash.json` — 22 реальных кейса (vless/vmess/ss/trojan/hysteria2/socks/anytls + BOM-кейс). Тест парсера обязан гонять все 22 и сверять canonical и hash посимвольно.

### 14.3 Схемы сообщений JetStream

`RAW` (fetcher → coordinator), пачка ≤100:

```json
{"event":"nodes.found","source_id":123,"fetched_at":"2026-08-17T00:00:00Z",
 "nodes":[{"uri":"vless://...","uri_hash":"sha256:...","protocol":"vless","transport":"ws","host":"1.2.3.4","port":443}]}
```

`JOBS_L0` / `JOBS_L1` (coordinator → tester; для L1 `outbound` = готовый sing-box outbound JSON):

```json
{"job_id":"<uuid>","stage":"L0","created_at":"...","vantage":"cloud-eu-1",
 "nodes":[{"node_id":1,"host":"1.2.3.4","port":443,"transport":"tcp"},
          {"node_id":2,"outbound":{"type":"vless","server":"...","server_port":443}}]}
```

`RESULTS` (tester/validator → coordinator):

```json
{"job_id":"<uuid>","vantage":"cloud-eu-1","stage":"L1",
 "results":[{"node_id":1,"ok":true,"latency_ms":245,"speed_mbps":null,"error_code":null,"ts":"..."}]}
```

`Nats-Msg-Id` (dedup JetStream): для jobs — `job_id`; для results — `job_id:vantage:stage`.

### 14.4 Конфигурация стримов

| Stream | Subjects | Retention | Ack | MaxDeliver | AckWait | DupWindow | Limits |
|---|---|---|---|---|---|---|---|
| `RAW` | `raw.import` | interest | explicit | 3 | 2m | 2m | 1 ГБ |
| `JOBS_L0` | `jobs.l0.>` | workqueue | explicit | 3 | 5m | 2m | 5 ГБ |
| `JOBS_L1` | `jobs.l1.>` | workqueue | explicit | 3 | 5m | 2m | 5 ГБ |
| `JOBS_L2` | `jobs.l2.>` | workqueue | explicit | 3 | 10m | 2m | 1 ГБ |
| `JOBS_VAL` | `jobs.validator.>` | workqueue | explicit | 3 | 5m | 2m | 1 ГБ |
| `RESULTS` | `results.>` | interest | explicit | 3 | 2m | 2m | 5 ГБ |
| `DLQ` | `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>` | limits | none | — | — | — | 1 ГБ |

KV: `PUBLISHES` (history=10), `REGISTRY` (history=1, ttl=2h — heartbeat воркеров).

### 14.5 DDL postgres (миграция 0001_init.sql — дословно)

```sql
CREATE TABLE sources(
  id BIGSERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL DEFAULT 'sub',
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_fetch_at TIMESTAMPTZ,
  last_status TEXT,
  note TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE nodes(
  id BIGSERIAL PRIMARY KEY,
  uri_hash TEXT NOT NULL UNIQUE,
  uri TEXT NOT NULL,
  protocol TEXT NOT NULL,          -- vless|vmess|ss|trojan|hysteria2|socks|anytls|tuic|hysteria
  transport TEXT NOT NULL DEFAULT 'tcp',
  host TEXT NOT NULL,
  host_ip TEXT,
  cc TEXT, city TEXT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  status TEXT NOT NULL DEFAULT 'unknown',   -- unknown|alive|dead
  best_class TEXT NOT NULL DEFAULT 'none',  -- none|cloud|rf
  score DOUBLE PRECISION NOT NULL DEFAULT 0,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_l1_ok_at TIMESTAMPTZ
);
CREATE INDEX nodes_status_class_idx ON nodes(status, best_class);
CREATE INDEX nodes_geo_idx ON nodes(cc);

CREATE TABLE node_checks(
  id BIGSERIAL,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  vantage TEXT NOT NULL,
  stage TEXT NOT NULL,             -- L0|L1|L2
  ok BOOLEAN NOT NULL,
  latency_ms INTEGER,
  speed_mbps DOUBLE PRECISION,
  error_code TEXT,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  ts_bucket INTEGER NOT NULL,      -- epoch_seconds / 300
  UNIQUE(node_id, vantage, stage, ts_bucket)
) PARTITION BY RANGE (ts);

CREATE TABLE vantages(
  id TEXT PRIMARY KEY,             -- 'cloud-eu-1', 'rf-home-1'
  kind TEXT NOT NULL,              -- cloud|edge
  region TEXT,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_heartbeat_at TIMESTAMPTZ
);

CREATE TABLE publishes(
  version BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  class_filter TEXT NOT NULL,
  node_count INTEGER NOT NULL,
  checksum TEXT NOT NULL,
  kv_key TEXT NOT NULL
);
```

Партиции `node_checks` создаются ежемесячно (CronJob/миграция); raw-проверки старше 30 дней — drop партицией.

### 14.6 Sing-box L1/L2 (исполнение пробы)

- Один процесс sing-box **на узел** (не на батч), semaфор одновременных процессов = 32 (config `PROBE_CONCURRENCY`).
- Порт для socks-inbound: аллокатор из диапазона 20000–20999 на воркер, `listen 127.0.0.1`.
- Конфиг: `{"log":{"level":"error"},"inbounds":[{"type":"socks","listen":"127.0.0.1","listen_port":P}],"outbounds":[<outbound из jobs.l1>],"route":{"final":"probe"}}` (outbound обязан иметь `"tag":"probe"`).
- L1: `curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 10 --socks5-hostname 127.0.0.1:P http://probe.<domain>/generate_204`. Успех: код 204 и time_total → `latency_ms`.
- Цели-фолбэки (по порядку): `http://probe.<domain>/generate_204`, `http://cp.cloudflare.com/generate_204`, `http://www.gstatic.com/generate_204`.
- L2: `curl -s -o /dev/null -w '%{speed_download}' --max-time 30 --socks5-hostname ... 'https://speed.cloudflare.com/__down?bytes=25000000'`; `speed_mbps = bytes*8/1e6/time_s`.
- Таймаут процесса sing-box (wall-clock) = L1: 20s, L2: 45s; по превышении — SIGKILL, код `E_INTERNAL`.

### 14.7 Примеры API-ответов

`GET /api/cities?class=rf` →

```json
{"cities":[{"city":"Frankfurt","cc":"DE","lat":50.11,"lon":8.68,"count":7,"best_latency_ms":94,"protocols":["vless","trojan"]}]}
```

`GET /api/node/42` →

```json
{"id":42,"protocol":"vless","cc":"DE","city":"Frankfurt","class":"rf",
 "latency_ms":94,"speed_mbps":31.2,"last_check":"2026-08-17T09:00:00Z",
 "share_link":"vless://...","outbound":{"type":"vless","server":"..."}}
```

`GET /api/sub?class=rf&limit=50` → `text/plain; charset=utf-8`, тело — base64 строк со строк-линков, по одному в строке, имена `DE-Frankfurt-94ms-rf`. Заголовок `Profile-Update-Interval: 6`.

---

## 15. Пины версий (не повышать без явной задачи)

| Что | Версия |
|---|---|
| Go | 1.23 |
| module | `module proxyfarm` (корневой go.mod) |
| nats-server | 2.10.x |
| nats.go | v1.37+ |
| pgx/v5 | v5.6+ |
| oschwald/geoip2-golang | v1.11 |
| sing-box (бинарник, НЕ go-import) | 1.13.x, SHA256 в Dockerfile |
| Leaflet | 1.9.4 |
| leaflet.markercluster | 1.5.3 |
| Postgres | 16 |

Запрещено: вводить новые зависимости без задачи; линковать sing-box как библиотеку (GPL).

## 16. Соглашения

**Коды ошибок** (enum, только эти строки): `E_DIAL_TIMEOUT`, `E_DIAL_REFUSED`, `E_TLS`, `E_PROTO_HANDSHAKE`, `E_HTTP_STATUS`, `E_CONTENT_MISMATCH`, `E_SLOW`, `E_PROCESS_TIMEOUT`, `E_INTERNAL`.

**Логи** — одна строка JSON в stdout: `{"ts":"...","lvl":"info","cmp":"worker","msg":"probe done","node_id":1,"job_id":"...","stage":"L1","ms":245,"code":null}`. В логи никогда не пишутся полные `uri` (секреты серверов) — только `node_id`.

**Метрики Prometheus**: `proxyfarm_probe_requests_total{stage,vantage,code}`, `proxyfarm_probe_duration_seconds{stage}` (histogram), `proxyfarm_job_batches_total{stream,result}`, `proxyfarm_publish_timestamp` (gauge).

## 17. Дефолты (применяются, если параметр не задан явно)

| Параметр | Значение |
|---|---|
| L0 timeout | 3s |
| L1 timeout (curl/процесс) | 10s / 20s |
| L2 bytes / timeout | 25 МБ / 30s+45s |
| Батч: RAW / L0 / L1 / L2 / VAL | 100 / 100 / 50 / 10 / 50 узлов |
| PROBE_CONCURRENCY | 32 |
| Ретесты | §4 (6ч жив; 3ч свежие<24ч; dead backoff 1→48ч) |
| Веса скора | 0.2 / 0.3 / 0.2 / 0.2 / 0.1 |
| Топ-K на L2 / топ-N валидатору | 300 / 50 за цикл |
| Публикация | каждые 6ч или Δ состава >20% |
| DNS-кэш в coordinator | TTL 1ч |

## 18. Ловушки (обязательные краевые случаи)

1. `vmess://` — base64 может быть std ИЛИ urlsafe, с/без padding; JSON может содержать числа-как-строки (`"port":"443"`) — приводить к строке.
2. BOM `\ufeff` в начале строк подписок (реально встречается, кейс в фикстурах).
3. SS: userinfo бывает `base64(method:password)`, бывают plugin-параметры в query — не пытаться декодировать при канонизации (правило §14.2).
4. IPv6-хосты в скобках; host:port split по **последнему** двоеточию.
5. `ws path` с percent-encoding — не декодировать (заполняется в outbound как есть).
6. Отсечение дефолтного порта — только для http/https/socks (vless:443 ≠ vless без порта — оставлять).
7. JetStream redelivery: результат может прийти дважды — идемпотентность через UNIQUE-ключ §14.5, а не через память.
8. NATS `AckSync` для results; падение координатора посреди батча безопасно (дубль отфильтрует БД).
9. `--socks5-hostname` (резолв DNS через прокси), не `--socks5` — иначе тестим локальный DNS, а не узел.
10. Curl exit code 28 (timeout) → `E_DIAL_TIMEOUT` на L0/L1; на L2 → `E_SLOW`.
11. Часовые пояса: всё UTC; в postgres только `TIMESTAMPTZ`.
12. Публикация base64: строки join `\n`, base64 std (не urlsafe), без переносов внутри.

## 19. Фикстуры

- `fixtures/sample_uris.txt` — 22 реальных uri из живых подписок (все протоколы + BOM-кейс).
- `fixtures/golden_uri_hash.json` — эталон canonical/uri_hash к каждому. Генератор правила — §14.2; тест `internal/parse` сверяет 22/22.
- `fixtures/subs_seed.sql` — сид `sources` из текущего `subs.txt` (создаётся задачей T-010).

## 20. Задачник (каждая задача = один PR; агент берёт минимальный незаблокированный ID)

| ID | M | Задача | Артефакты | Dep | DoD |
|---|---|---|---|---|---|
| T-001 | 0 | скелет репо, go.mod, Makefile, golangci-lint | корень, Makefile | — | `make lint` зелёный |
| T-002 | 0 | internal/parse: uri→outbound + uri_hash по §14.2 | internal/parse/* | T-001 | golden 22/22 |
| T-003 | 0 | internal/geo: mmdb + DNS-кэш | internal/geo/* | T-001 | тесты с testdata mmdb |
| T-004 | 0 | internal/probe L0 (dialer) | internal/probe/l0.go | T-001 | unit-тест на listen-сокет |
| T-005 | 0 | internal/probe L1 (sing-box exec) | internal/probe/l1.go | T-002 | интеграционный тест на 1 реальном uri (skip env CI) |
| T-006 | 0 | internal/probe L2 | internal/probe/l2.go | T-005 | unit на парсер curl-вывода |
| T-007 | 0 | cmd/worker: роли, vantage, семафор, метрики | cmd/worker/main.go | T-004..T-006 | `go run ./cmd/worker --role=tester --dry-run` |
| T-008 | 1 | internal/model + миграции 0001 | internal/model/*, migrations/ | T-001 | `make migrate` на тестовой pg |
| T-009 | 1 | internal/natsb: стримы/KV/lease по §14.4 | internal/natsb/* | T-001 | тест против nats:2.10 в докере |
| T-010 | 1 | cmd/fetcher + сид sources | cmd/fetcher, fixtures/subs_seed.sql | T-002, T-008 | полный прогон subs.txt → RAW |
| T-011 | 1 | coordinator: RAW→узлы→jobs | cmd/coordinator | T-008..T-010 | e2e: uri доходит до jobs.l1 |
| T-012 | 1 | coordinator: results→score | internal/score | T-011 | unit на весах §17 |
| T-013 | 1 | publishes vN + REGISTRY | cmd/coordinator | T-012 | KV v1 создан, checksum совпал |
| T-014 | 1 | compose e2e | deploy/compose | T-007..T-013 | `docker compose up` → публикация v1 |
| T-015 | 2 | cmd/api REST §8 | cmd/api | T-008 | http-тесты всех эндпоинтов |
| T-016 | 2 | /api/sub base64 + interval | cmd/api | T-015 | v2rayN импортирует по ссылке |
| T-017 | 2 | web-карта (Leaflet+cluster) | web/* | T-015 | карта грузится, клик→линк |
| T-018 | 2 | SSE publishes | cmd/api | T-013 | браузер получает событие |
| T-019 | 3 | Dockerfile distroless + пин sing-box | docker/ | T-007 | образ <60 МБ, юзер non-root |
| T-020 | 3 | k8s манифесты + NetPol + KEDA | deploy/k8s | T-014, T-019 | `kustomize build` без ошибок |
| T-021 | 3 | ingress TLS + rate limit | deploy/k8s | T-020 | cert-manager issue, 429 при переборе |
| T-022 | 3 | мониторинг/алерты | deploy/k8s/monitoring | T-020 | алерт «публикация >12ч» горит при остановке |
| T-023 | 4 | edge: WG + nkey + systemd | deploy/edge/orangepi | T-014 | Пай коннектится, джобы валидируются |
| T-024 | 5 | admin API (sources/vantages) | cmd/api | T-015 | CRUD под X-Admin-Token |

## 21. Правила агента-исполнителя

1. Взять **один** минимальный ID без незакрытых зависимостей. Один PR = один ID.
2. Контракты §14–§18 не пересматриваются и «не улучшаются». Нечто не описано → применить дефолт §17; дефолта нет → **спросить**, не выдумывать.
3. Создавать только файлы своей задачи; чужие модули не трогать. go.mod пополнять только библиотеками из §15.
4. DoD из таблицы — единственный критерий готовности; тесты обязательны, проходят локально (`make test`).
5. Новых фреймворков, ORM, фич «про запас» — нет. Ветвь `task/T-0NN`.
6. Секреты/uri с ключами в логи и коммиты не попадают (§16).
7. Найдена ошибка в спеке → зафиксировать issue, не молча обходить.

---

## Приложение A. Структура репозитория

```
proxyfarm/
  cmd/{fetcher,coordinator,api,worker}/main.go
  internal/
    model/        # сущности + pgx-запросы
    parse/        # share-links → outbound JSON (vless/vmess/ss/trojan/hy2)
    probe/        # L0 dialer, L1 singbox-exec, L2 download
    score/        # агрегация скора
    natsb/        # streams, KV, lease, nkey- helpers
    geo/          # mmdb reader, резолвер с кэшем
  migrations/     # 0001_init.sql и далее
  fixtures/       # sample_uris.txt, golden_uri_hash.json, subs_seed.sql
  deploy/
    k8s/          # ns, secrets, postgres, nats, deployments, keda, cronjobs, ingress, netpol
    compose/      # M1-локальный прогон без k8s
    edge/orangepi/  # systemd unit + wg-client conf + install.sh
  web/            # Leaflet фронтенд (static)
  docker/         # multi-stage: build → distroless
  ARCHITECTURE.md
```
