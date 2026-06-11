# AGENTS.md — Windmill Go Tools

## CRITICAL: Read RULES.md first

`RULES.md` contains two hard rules:
1. **NEVER read, open, or modify `.env` or `.env.*` files.**
2. **Use Bahasa Indonesia in responses.**

## Project Nature

This is a **collection of standalone Windmill scripts** — not a traditional app or service.
Each script detects discrepancies (stock, order, point, product, voucher) between XMS systems
by querying multiple databases and emitting Mattermost-compatible Markdown reports.

There is **no shared library**, no test suite, no CI pipeline, and no production deployment.
Each tool is self-contained with copy-pasted helper functions.

## Dual-Layer Entrypoint Pattern

Every tool follows this structure:

```
<domain>/<tool-name>/main.go         # package inner — exports Main(...) for Windmill platform
<domain>/<tool-name>/cmd/main.go     # package main   — local runner (loads .env via godotenv)
```

- To **run locally**: `go run ./<domain>/<tool-name>/cmd`
- The Windmill platform calls the `inner.Main()` function directly, passing resource-path strings
  (e.g. `u/sucahyo/catalyst_uam_postgresql_prod`) that get resolved at runtime via `wmill.GetResource()`.

## Running Scripts

```bash
# 1. Copy env template (only .env.example is safe to read)
cp .env.example .env
# Fill in the actual DSN values in .env manually.

# 2. Run any tool via its cmd entrypoint:
go run ./stock-discrepancy/xmsc-xmsl/cmd
go run ./stock-discrepancy/xmsl-reseller/cmd
go run ./stock-discrepancy/xmsl-uf/cmd
go run ./courier/cmd
go run ./order/fulfillment-discrepancy/cmd
go run ./order/address-discrepancy/xmsc-uf/cmd
go run ./order/status-discrepancy/xmsc-uf/cmd
go run ./order/status-discrepancy/xmsc-uf-excel/cmd
go run ./point/point-completed-order/cmd
go run ./point/point-missing-earn-sla/cmd
go run ./point/point-redeem-not-deducted/cmd
go run ./point/point-redeem-refund/cmd
go run ./product/status/xmsc-uf/cmd
go run ./user/inactive-user/cmd
go run ./voucher/missing_voucher_usage_orders/cmd
```

## DSN Resolution Pattern (in every tool)

Each tool's resolver accepts either:
- A **direct DSN** (`postgres://...` or `host=...`) — used for local dev via `.env`
- A **Windmill resource path** (`u/username/resource_name`) — resolved by the Windmill platform at runtime

When a resource path fails to resolve, the function falls back to a hardcoded default resource path.

## Key Environment Variables

`.env.example` only defines 3; other tools reference additional variables. Agents should never read
`.env` directly — use grep on `.env.example` or source code to discover required env vars:

| Variable | Used by |
|---|---|
| `XMS_CATALYST_VOILA_DSN` | Most tools (Catalyst PostgreSQL, `search_path=voila`) |
| `XMS_LEGACY_DSN` | stock-discrepancy/xmsc-xmsl, xmsl-reseller, courier |
| `RESELLER_DSN` | stock-discrepancy/xmsl-reseller |
| `VOILA_SHIPMENT_DSN` | courier |
| `PROM_PUSHGATEWAY_URL` | stock-discrepancy/xmsc-xmsl, order/fulfillment-discrepancy |
| `ES_USERNAME`, `ES_PASSWORD` | product/status/xmsc-uf, stock-discrepancy/xmsl-uf |
| `VAULT_ADDR`, `VAULT_GITHUB_TOKEN`, `VAULT_PATH` | stock-discrepancy/xmsl-uf, product/status/xmsc-uf |

## Gotchas

- **Root `cmd` and `main` are stale Mach-O binaries**, not source directories. Ignore them.
- **`vendor/` is tracked in git** despite `.gitignore` listing `vendor/`. Do not delete it.
- **No tests exist** — `go test ./...` will find nothing. Don't try to run tests.
- **No CI exists** — there is no `.github/`, Dockerfile, or pipeline config.
- **The module name is `windmill`** (not the full repo path). Imports use `windmill/...` as prefix.
- **Database `search_path=voila`** is required in most Catalyst DSNs — omitting it queries the wrong schema.
- **dblink** is used extensively in `point/` tools and `stock-discrepancy/xmsl-reseller` to query cross-database from Catalyst.
- **MongoDB** (`go.mongodb.org/mongo-driver`) is used in `order/` tools for Voila UF comparisons.
- **Elasticsearch** (raw HTTP via Vault credentials) is used in `product/status/xmsc-uf` and `stock-discrepancy/xmsl-uf`.
- **Excel export**: `order/status-discrepancy/xmsc-uf-excel` outputs `.xlsx` via `excelize/v2` instead of Markdown.
- **The `user/inactive-user` tool is a write operation** — it updates user status to INACTIVE across 3 databases. Review carefully before running.
- **Prometheus Pushgateway** is optional — tools that push metrics silently skip if the URL is empty.
