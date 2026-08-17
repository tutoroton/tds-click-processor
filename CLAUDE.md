# Click Processor — edge routing decision engine

## What this is

FastAPI app (`app.main:app`, port 8100) on the **edge routing plane**: the CF Worker calls
`POST /decide` for every click and gets back a destination URL. This is the product's hot path —
the whole `/decide` response carries a **<10 ms** budget (the budget table and the open PERF-1
question about it live in rule `architecture`), so anything added here is paid by every click in
the fleet. It also owns the click's first durable copy: `/decide` XADDs to the node-local
`stream:clicks`, and a background shipper task drains that stream to the central collector.
It is deployed **per edge node** (`docker-compose.node.yml` + `deploy/`), not to a central host.

## Boundary

| Owns | Never does |
|---|---|
| Rule/criteria evaluation, flow scope-cascade, offer-target selection | Config CRUD (admin-api's) |
| `click_id` handling, dedup, `is_unique`/`is_returning` | Any SQL — see hazard 1 |
| Param slot resolution, macro substitution, redirect URL build | Conversions / postbacks (process-service's) |
| Node-local `stream:clicks`, the shipper, the disk park | Analytics reads (stats-service's) |
| Receiving pushed config into local Redis (`POST /admin/sync`) | Writing anything a browser reads |

Full ownership + failure-isolation table: rule `microservice-boundaries`.

## Shape

**`app/` has NO packages — 29 flat modules** (measured: 0 subdirectories; admin-api has 60
packages). There is no router/service/repository layering here and adding one would be a
service-wide change, not a local tidy: the subtree ships as a lean public edge image (see
hazard 6), and the whole decide path is meant to be readable in one `ls`.

| Module | Owns |
|---|---|
| `main.py` (2497) | FastAPI app, lifespan, `/decide` · `/health` · `/stats` · `/admin/sync` · `/admin/seed`, the stream write |
| `router.py` (2662) | The routing engine — Redis-only lookups, criteria matching, URL emission |
| `cascade.py` · `action_executor.py` | Flow scope-cascade resolution; `action_type` → concrete URL |
| `shipper.py` (1841) | Consumer-group drain of `stream:clicks` → central collector; retries, deadletter, PEL reclaim |
| `disk_queue.py` (1681) | Append-only NDJSON segment engine — spill + durable park, group-commit fsync, orphan adoption |
| `watermark.py` | Sampled edge Redis `used_memory%` → spill decision |
| `resolution.py` · `parameters.py` · `param_rules.py` · `macros.py` | Param slot value-chain, canonical slot registry, URL macro substitution (all pure, no I/O) |
| `identity.py` · `identity_token.py` · `sticky.py` · `history.py` | Returning-user layer — dark by default (hazard 8) |
| `redis_client.py` | The three pools (hazard 2) |
| `config.py` (841) | Every setting, `env_prefix="TDS_"`; heavily commented — read it before adding a knob |
| `sync_client.py` | Applies pushed config to local Redis (write-then-delete) |
| `telemetry.py` · `observability.py` · `diag.py` · `*_metrics.py` · `_percentile_window.py` | Throttled Sentry ops, periodic metrics, `X-Test-Id` diagnostic tracing |
| `enrichment.py` · `ua_parser.py` · `models.py` | Attribution chain, device_detector wrapper + LRU, request/response schemas |

New code: a new pure helper → its own flat module next to `resolution.py`; new routing behaviour →
the module that already owns that stage, not a new layer.

## Local hazards

| # | Hazard | Evidence |
|---|---|---|
| 1 | **No PostgreSQL, ever — not even a driver.** `requirements.txt` is 7 lines and contains no PG client. All routing data arrives via `/admin/sync` into local Redis. "Just read the DB for this one field" is not a small change here; it is impossible | `requirements.txt`; `router.py:1-5` *"All lookups are Redis-only, no SQL"* |
| 2 | **Three Redis clients, deliberately not interchangeable** — `get_redis()` (routing, short `socket_timeout` sized for `/decide`), `get_identity_redis()` (separate `noeviction` instance), `get_shipper_redis()` (same instance as routing, longer timeouts). Reusing the routing pool for a blocking read is the exact defect that produced ~592K Sentry events, and a **boot-time guard now fails the whole service** if `redis_shipper_socket_timeout_seconds` is retuned below `BATCH_TIMEOUT_MS` | `redis_client.py:11`, `:54`, `:102`, incident writeup at `:90-98`; guard `shipper.py:125-153` `_assert_shipper_pool_timeout_margin`, called at `:185` |
| 3 | **The disk park is written BEFORE the ACK and is the sole authority on whether the ACK may happen.** If `enqueue_parked_click` returns False the message stays un-ACKed in the PEL. The `stream:clicks-deadletter` ring underneath it is `maxlen=`-capped operator visibility, never a copy | `shipper.py:479-497`; the falsified premise is spelled out at `:460-478` |
| 4 | **Nothing may do a per-click `INFO`, or any unbounded work, on the decide path.** The memory watermark is sampled by a dedicated ~1s background task; the click path reads only the cached `should_spill()` | `watermark.py:22-27`, `:125`; `main.py:246`, `:1697` |
| 5 | **`WEB_CONCURRENCY` means N sibling PROCESSES, not threads.** Module-level state (pools, metric windows, disk segments) is per-worker, not per-node; disk segments are named `{boot_epoch}-{pid}-{seq}` and a dead worker's segments need orphan adoption to ever drain. `device_detector` is GIL-bound, so each worker also carries its own ~20 MB parser | `disk_queue.py:8-11`, `:42-56`, `:207-222`; `Dockerfile:13-20` |
| 6 | **This directory is published to a PUBLIC mirror on every `stage` merge** (`tutoroton/tds-click-processor`), which freshly-provisioned nodes `git clone`. A gitleaks scan of the subtree gates the push. Test deps are kept out of `requirements.txt` on purpose so the edge image stays lean | `.github/workflows/click-processor-mirror.yml:1-13`, `:41-50`; `requirements-dev.txt:1-9` |
| 7 | **`tds-ctl` here is a bash NODE-management script** (`deploy \| stop \| restart \| status \| logs \| seed \| redis-cli \| sync \| destroy`) — not the admin CLI of the same name that this repo's design docs describe (`tds-ctl campaigns create`, `tds-ctl login`). That CLI has no file in the tree; this one does | `tds-ctl:1-14`, `:209-222`; vs `docs/DESIGN-ORG-HIERARCHY-AND-ADMIN-API.md:343-351`, `docs/design/ENTITY-MODEL-v2.md:120` |
| 8 | **The returning-user layer is DARK by default.** `returning_resolver_enabled` is a master kill-switch checked before any identity Redis I/O; `identity.py`, `sticky.py`, `history.py` do not run in production today. Do not read them as live behaviour | `config.py:574-598` |

## Run and verify

```bash
cd services/click-processor
pip install -r requirements.txt -r requirements-dev.txt   # pytest/fakeredis are NOT in the image
TDS_ENVIRONMENT=local python3 -m pytest tests/unit/ -v    # 91 files — verified: 1680 passed in 17s
TDS_ENVIRONMENT=local python3 -m pytest tests/unit/test_param_contract.py -q   # param value-chain contract
```

| Fact | Consequence |
|---|---|
| `make test` at the repo root runs **admin-api only** (`Makefile:210`) | it never touches this service — run the command above |
| No `conftest.py`, no `pytest.ini` — the suite is pure-unit on `fakeredis` | no Redis, no PG, no containers needed; it runs in seconds |
| `TDS_ENVIRONMENT=local` is required | the config validators enforce secret/central-URL presence outside local envs |
| `tests/test_update_sh_rollback.sh` drives `deploy/update.sh` against stub `docker compose`/`curl` | gated on `TDS_RUN_NODE_INTEGRATION=1`; run it when you touch `deploy/` |
| CI mirrors exactly this invocation | `.github/workflows/pr-validation.yml:429-459` |

## Read next

| Task | Load |
|---|---|
| Any change in this service | skill `click-processing`, skill `data-flow-model` |
| Param slots / aliases / macros / value-chain | `docs/design/PARAMETER-SYSTEM.md` (SoT), then skill `adding-a-parameter` |
| Shipper / disk queue / watermark / retries | skill `resilience-patterns`, skill `observability` |
| Anything reaching the collector or a partner URL | skill `outbound-http-safety` |
| Redis keyspace, node containers, compose | skill `infrastructure-ops` |
| Config pushed from admin-api (`/admin/sync`) | skill `sync-protocol` |
| `X-Test-Id` tracing, diagnostic mode | skill `diagnostic-tracing` |
| Deploying / provisioning a node running this | skill `provisioning-edge-node`, skill `deploying-to-staging` |
| Sentry, error budgets, alert rules | skill `observability`, skill `sentry-release-tracking` |
| "This routing behaviour looks wrong" | rule `bug-triage-discipline` first — `grep -ril` in `.roadmap/decisions/` |
