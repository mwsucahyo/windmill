# AGENTS.md — Windmill Go Tools

## CRITICAL: Read RULES.md first

`RULES.md` contains two hard rules:
1. **NEVER read, open, or modify `.env` or `.env.*` files.**
2. **Use Bahasa Indonesia in responses.**

## Project Nature

Collection of **17 standalone Windmill scripts** (no shared lib, no tests, no CI).
Each detects discrepancies between XMS systems and emits Mattermost Markdown (or `.xlsx`).
Each tool is self-contained with copy-pasted helpers — never assume a shared utility exists.

## Entrypoint Pattern

```
<domain>/<tool>/main.go       # package inner — exports Main(...) for Windmill platform
<domain>/<tool>/cmd/main.go   # package main   — local runner (loads .env via godotenv)
```

Run locally: `go run ./<domain>/<tool>/cmd`

The Windmill platform calls `inner.Main()` directly, passing resource-path strings
(e.g. `u/sucahyo/catalyst_uam_postgresql_prod`) resolved at runtime via `wmill.GetResource()`.

## Running Locally

```bash
cp .env.example .env      # only .env.example is safe to read
go run ./stock-discrepancy/xmsc-xmsl/cmd
go run ./brand/cmd
go run ./courier/cmd
go run ./voucher/missing_voucher_usage_orders/cmd
```

| Domain | Tools |
|---|---|
| `stock-discrepancy/` | `xmsc-xmsl`, `xmsl-reseller`, `xmsl-uf` |
| `office/` | `office` |
| `order/` | `fulfillment-discrepancy`, `address-discrepancy/xmsc-uf`, `status-discrepancy/xmsc-uf`, `status-discrepancy/xmsc-uf-excel` |
| `point/` | `point-completed-order`, `point-missing-earn-sla`, `point-redeem-not-deducted`, `point-redeem-refund` |
| others | `product/status/xmsc-uf`, `user/inactive-user`, `courier`, `brand`, `voucher/missing_voucher_usage_orders`, `office` |

## DSN Resolution

Every tool's resolver accepts either a **direct DSN** (local dev via `.env`) or a **Windmill resource path**
(`u/username/resource_name`). Unresolvable paths fall back to a hardcoded default in the source.

## Environment Variables

`.env.example` defines 4 variables. Never read `.env` directly — grep on source code or `.env.example`:

| Variable | Used by |
|---|---|
| `XMS_CATALYST_VOILA_DSN` | Most tools (Catalyst PostgreSQL, `search_path=voila`), `office` |
| `XMS_CATALYST_UAM_DSN` | `user/inactive-user` |
| `XMS_CATALYST_JAMTANGAN_DSN` | `user/inactive-user` |
| `XMS_LEGACY_DSN` | `stock-discrepancy/xmsc-xmsl`, `xmsl-reseller`, `courier`, `office` |
| `RESELLER_DSN` | `stock-discrepancy/xmsl-reseller` |
| `VOILA_SHIPMENT_DSN` | `courier` |
| `VOILA_VOUCHER_DSN` | `voucher/missing_voucher_usage_orders` |
| `VOILA_ACCOUNT_DSN` | `point/point-completed-order` |
| `VOILA_UF_MONGO_URI` | `order/address-discrepancy/xmsc-uf`, `order/status-discrepancy/xmsc-uf`, `order/status-discrepancy/xmsc-uf-excel`, `office` |
| `ES_URL` / `ES_USERNAME` / `ES_PASSWORD` | `product/status/xmsc-uf`, `stock-discrepancy/xmsl-uf` |
| `VAULT_ADDR` / `VAULT_GITHUB_TOKEN` / `VAULT_PATH` | `stock-discrepancy/xmsl-uf`, `product/status/xmsc-uf` |
| `PROM_PUSHGATEWAY_URL` | `stock-discrepancy/xmsc-xmsl`, `order/fulfillment-discrepancy` |
| `TARGET_EMAIL` | `user/inactive-user` (test email for dry-run-like local use) |

`Main()` signatures and required env vars vary per tool — read the relevant `cmd/main.go` to see exactly what it expects at runtime.

## Gotchas

- **Root `cmd` and `main` are stale Mach-O binaries** — never treat them as source dirs.
- **`vendor/` is tracked in git** despite `.gitignore` listing it. Do not delete.
- **Module name is `windmill`** — imports use `windmill/...` as prefix.
- **No tests** — `go test ./...` returns nothing.
- **`brand/main.go` uses `package inner`** with the standard `Main(...)` export, same as every other tool (despite being flat in `brand/`).
- **Naming inconsistency**: most tools use kebab-case (`xmsc-xmsl`), but `voucher/missing_voucher_usage_orders` uses snake_case.
- **`search_path=voila`** required in most Catalyst DSNs — wrong schema otherwise.
- **`dblink`** is used in `point/` tools and `stock-discrepancy/xmsl-reseller` for cross-database queries from Catalyst.
- **MongoDB** (`go.mongodb.org/mongo-driver`) in `order/` tools for Voila UF comparisons.
- **Elasticsearch** (raw HTTP via Vault) in `product/status/xmsc-uf` and `stock-discrepancy/xmsl-uf`.
- **Excel export**: `order/status-discrepancy/xmsc-uf-excel` writes `.xlsx` via `excelize/v2`, not Markdown.
- **`user/inactive-user` is a write operation** — updates users to INACTIVE across 3 databases. Review carefully.
- **Prometheus Pushgateway** is optional — silently skipped if URL is empty.
- **`loadEnv` pattern**: most `cmd/main.go` files try `godotenv.Load()` at multiple depths (`.`, `../`, `../../`, etc.). `user/inactive-user/cmd` uses a single fixed path `"../../.env"` instead.
- **Makefile** exists for Grafana/Loki stack (`make up/down`) and running `stock-discrepancy/xmsc-xmsl` with log capture (`make run`). Not needed for core tool usage.
