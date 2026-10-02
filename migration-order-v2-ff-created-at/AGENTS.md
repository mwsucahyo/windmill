# AGENTS.md — migration-order-v2-ff-created-at

Backfill tool for orders already migrated by `migration-order-v2`. **Writes** to
Catalyst PostgreSQL (updates `tr_fulfillment.created_at`) and to MongoDB (fix flag on
`migration_order_v2_log`). Single flattened file.

## CRITICAL: Read root RULES.md

Hard rules: never read/open/modify `.env` or `.env.*` (except `.env.example`);
respond in Bahasa Indonesia.

## Why

`migration-order-v2` originally inserted created fulfillments with
`created_at = NOW()`. The correct value is the order's own timestamp:
`tr_order.processed_at` → `tr_order.completed_at` → `tr_order.created_at` (first
non-nil/non-zero). This tool fixes rows created by earlier runs. (The main tool now
sets `created_at` correctly, so this is only for historical data.)

## Entrypoint

- `main.go` — package `inner`, import path `windmill/migration-order-v2-ff-created-at`.
  Exports flat scalar params (Windmill-friendly):

  ```go
  func Main(xmsCatalystDSN, mongoResourceOrURI, schema, orderNumbers, ffCase, environment string) (interface{}, error)
  ```

- `cmd/main.go` — local runner; loads `.env` and passes env values.

## Inputs

- `schema` (**required**): `voila` or `jamtangan` (switches resource + code prefix).
- `orderNumbers` (optional): comma-separated order numbers, e.g.
  `26080412517777,26080412517788`; empty = all.
- `ffCase` (optional): a single `case` value, e.g. `CREATE_CARRY_OUT`. Empty = the
  create cases listed in `createFFCases`.
- `environment` (optional): `dev` | `stg` | `prod`; default `dev`.

## Resource resolution

Same `resourceByEnv` table and resolvers as `migration-order-v2`: direct DSN/URI when
provided (local), otherwise `wmill.GetResource` on the per-env resource path.

## Algorithm

1. Query Mongo `migration_order_v2_log` with:
   `schema`, `status = "OK"`, `created_at_fixed_at` **not exists**,
   `case = ffCase` (or `$in createFFCases`), optional `order_number`.
   Sorted by `migrated_at` asc.
2. For each log: load `tr_order` by `order_id` and compute the resolved timestamp.
3. Update only migration-created fulfillments:
   `UPDATE tr_fulfillment SET created_at = <resolved> WHERE id IN (<log.fulfillment_ids>) AND created_at >= <migrated_at - 24h>`.
4. If rows were updated, flag the log document:
   `created_at_fixed_at`, `created_at_fixed_count`, `created_at_fixed_to`.

## Identifying migration-created rows

`log.fulfillment_ids` contains **all** fulfillments touched (created **and** existing
for `MIXED_*` cases). Only recently-created rows are targeted with the time guard
`created_at >= migrated_at - 24h`:

- Migration-created rows have `created_at = migrated_at` (± clock/timezone), inside
  the window.
- Pre-existing rows belong to orders completed **≥ 5 days before** migration (the
  main tool's selection guard), far outside the 24h window.

The guard also makes re-runs idempotent: after a row is fixed its `created_at` is old,
so it no longer matches.

## Mongo flag

Log docs that had at least one fulfillment updated get
`created_at_fixed_at` / `created_at_fixed_count` / `created_at_fixed_to`. Logs with
nothing to fix are **not** flagged and remain eligible for a future run. Mongo is
**required** — the tool errors out when the URI cannot be resolved.

## Local run

`go run ./migration-order-v2-ff-created-at/cmd` — requires `XMS_CATALYST_DSN`,
`XMS_CATALYST_MONGO_URI`, and `MIGRATION_SCHEMA`.

Runner env vars: `XMS_CATALYST_DSN`, `XMS_CATALYST_MONGO_URI`, `MIGRATION_SCHEMA`,
`MIGRATION_ENVIRONMENT` (default `dev`), `MIGRATION_ORDER_NUMBERS` (optional,
comma-separated), `MIGRATION_CASE` (optional).

## Gotchas

- Depends on Mongo logs written by `migration-order-v2`. Orders migrated before
  logging existed, or without a `CREATE_*`/`MIXED_CREATE_*` case, cannot be found.
- Only `created_at` is changed; `updated_at` is left untouched.
- The 24h window assumes the >5-day selection guard; with `is_testing`/dev data where
  orders may be fresh, the guard can misclassify — use on prod data.
- Multi-tenant raw SQL: schema-prefixed table names via `fmt.Sprintf`.
- No tests — verify via code review and a dry run against a small order set.
