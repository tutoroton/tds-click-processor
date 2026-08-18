# click-processor — edge routing decision engine

**Port `8100` · deployed per edge node, not centrally.**

A FastAPI app (`app.main:app`) on the routing plane. The CF Worker calls `POST /decide` for every
click and gets back a destination URL.

> **This is the product's hot path.** The whole `/decide` response carries a **< 10 ms budget**, so
> anything added here is paid by every click in the fleet.

It also owns the click's **first durable copy**: `/decide` XADDs to the node-local `stream:clicks`,
and a background shipper drains that stream to the central collector.

## Boundary

| Owns | Never does |
|---|---|
| The routing decision and its latency budget | Writes configuration — config arrives via sync from central Redis |
| `stream:clicks` on the node, and the shipper that drains it | Talks to PostgreSQL |
| The node-local routing Redis read (sub-millisecond) | Aggregates or reports — that is `stats-service` |

## Run

```bash
cd services/click-processor
docker compose up                       # central/dev shape
docker compose -f docker-compose.node.yml up   # the edge-node shape actually deployed
pytest tests/
```

`tds-ctl` in this directory is the node operator's helper.

## Hazards

The routing Redis is **not interchangeable** with the work-plane Redis. Losing `stream:clicks`
before the shipper has ACKed loses clicks outright — see `CLAUDE.md` here, and
`.claude/rules/architecture.md` for the budget table.
