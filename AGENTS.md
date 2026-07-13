# AGENTS.md — Windmill Go Tools

## CRITICAL: Read RULES.md first

`RULES.md` contains two hard rules:
1. **NEVER read, open, or modify `.env` or `.env.*` files.**
2. **Use Bahasa Indonesia in responses.**

## Project

Collection of **18 standalone Windmill scripts** (no tests, no CI, no shared lib).
Each compares data across XMS systems (Catalyst, Legacy, UF, Reseller, etc.) and
outputs Mattermost Markdown (or `.xlsx`). Every tool is self-contained — never
assume a shared utility exists.

## Entrypoint Pattern

```
<domain>/<tool>/main.go       # package inner — exports Main(...) for Windmill
<domain>/<tool>/cmd/main.go   # package main   — local runner (loads .env via godotenv)
```

Run locally: `go run ./<domain>/<tool>/cmd`

Windmill calls `inner.Main(dsn1, dsn2, ...)` directly, passing resource-path
strings resolved at runtime via `wmill.GetResource()`. Each tool's `Main()`
signature differs — read the relevant `main.go` first.

**Flat tools** (no domain subdir): `brand/`, `courier/`, `office/`.

## DSN Resolution

Every resolver accepts either a **direct DSN** (`postgres://...`, local dev via
`.env`) or a **Windmill resource path**. Unresolvable paths fall back to a
hardcoded default in source.

## Environment

| Variable | Used by |
|---|---|
| `XMS_CATALYST_VOILA_DSN` | Most tools (Catalyst PostgreSQL, `search_path=voila`) |
| `XMS_CATALYST_UAM_DSN` | `user/inactive-user` |
| `XMS_CATALYST_JAMTANGAN_DSN` | `user/inactive-user` |
| `XMS_LEGACY_DSN` | `stock-discrepancy/xmsc-xmsl`, `xmsl-reseller`, `courier`, `office`, `order-not-sync` |
| `RESELLER_DSN` | `stock-discrepancy/xmsl-reseller` |
| `VOILA_SHIPMENT_DSN` | `courier` |
| `VOILA_VOUCHER_DSN` | `voucher/missing_voucher_usage_orders` |
| `VOILA_ACCOUNT_DSN` | all `point/` tools |
| `VOILA_UF_MONGO_URI` | `order/address-discrepancy/xmsc-uf`, `order/status-discrepancy/*`, `office` |
| `ES_URL` / `ES_USERNAME` / `ES_PASSWORD` | `product/status/xmsc-uf`, `stock-discrepancy/xmsl-uf` |
| `VAULT_ADDR` / `VAULT_GITHUB_TOKEN` / `VAULT_PATH` | `stock-discrepancy/xmsl-uf`, `product/status/xmsc-uf` |
| `PROM_PUSHGATEWAY_URL` | `stock-discrepancy/xmsc-xmsl`, `order/fulfillment-discrepancy` |
| `TARGET_EMAIL` | `user/inactive-user` (local test email) |

## Tools

| Domain | Path |
|---|---|
| `stock-discrepancy/` | `xmsc-xmsl`, `xmsl-reseller`, `xmsl-uf` |
| `order/` | `fulfillment-discrepancy`, `order-not-sync` |
| `order/address-discrepancy/` | `xmsc-uf` |
| `order/status-discrepancy/` | `xmsc-uf`, `xmsc-uf-excel` |
| `point/` | `point-completed-order`, `point-missing-earn-sla`, `point-redeem-not-deducted`, `point-redeem-refund` |
| `product/status/` | `xmsc-uf` |
| `user/` | `inactive-user` |
| `voucher/` | `missing_voucher_usage_orders` |
| (flat) | `brand`, `courier`, `office` |

## Gotchas

- **Root `cmd` and `main`** are stale Mach-O arm64 binaries — never treat as source dirs.
- **`vendor/`** tracked in git despite `.gitignore`. Do not delete.
- **Module name is `windmill`** — imports use `windmill/...` prefix.
- **No tests** — `go test ./...` returns nothing.
- **`gorm`** (with `pgx` driver) is the universal ORM. MongoDB (`mongo-driver`) in `order/` and `office/`. Elasticsearch via raw HTTP + Vault in `xmsl-uf` and `product/status/xmsc-uf`.
- **`dblink`** used in `point/`, `order-not-sync`, `xmsl-reseller`, and `voucher` for cross-DB queries from Catalyst.
- **All helpers are copy-pasted** — `resolveDSN`, `resolveVariable`, `connectDB` are duplicated in every tool's `main.go`. Never assume a shared utility.
- **`search_path=voila`** required in Catalyst DSNs.
- **`missing_voucher_usage_orders`** uses snake_case — all other tools use kebab-case.
- **`user/inactive-user`** is a write operation (updates 3 databases). Review carefully.
- **Excel export**: `xmsc-uf-excel` writes `.xlsx` via `excelize/v2`, not Markdown.
- **Prometheus Pushgateway** optional — silently skipped if URL empty.
- **`loadEnv` depth varies**: most try `.`, `../`, `../../`, `../../../`. `user/inactive-user/cmd` uses fixed `"../../.env"`.
- **Makefile** runs `stock-discrepancy/xmsc-xmsl` with log capture (`make run`). `make up/down/restart` reference a missing `grafana/` dir and will fail.
