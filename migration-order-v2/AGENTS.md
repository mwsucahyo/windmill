# AGENTS.md — migration-order-v2-windmill

Data migration tool for XMS Catalyst orders (Order V2). **Writes** to Catalyst
PostgreSQL (updates `tr_order`, inserts `tr_fulfillment`, `tr_fulfillment_product`,
`tr_fulfillment_item_code`) inside a per-order transaction. Single flattened file —
unlike its structured sibling `../migration-order-v2/` (repository/usecase split).

## CRITICAL: Read root RULES.md

Hard rules: never read/open/modify `.env` or `.env.*`; respond in Bahasa Indonesia.

## Entrypoint

- `main.go` — package `inner`, import path `windmill/migration-order-v2-windmill`.
  Exports `Main(migrationParams struct{Schema, OrderNumbers, StartDate, EndDate, Environment string})`.
- `cmd/main.go` — package `main` local runner.

## DSN resolution — Windmill-only

`Main` never receives a DSN or Mongo URI: `xmsCatalystDSN` and `mongoResourceOrURI`
are always empty strings in `main.go`. DSN is resolved **only** via
`wmill.GetResource()` from hardcoded resource paths. Environment is chosen via
`migrationParams.Environment` (default `"dev"`; valid: `dev`, `stg`, `prod`) in
`resourceByEnv` (main.go):

| Env | voila | jamtangan | mongo |
|---|---|---|---|
| dev (default) | `u/mirza/catalyst_xms_postgresql_voila_dev` | `u/mirza/catalyst_xms_postgresql_jt_dev` | `f/flows_engineering/xms_catalyst_mongo_dev` |
| stg | `u/mirza/catalyst_xms_postgresql_voila_stg` | `u/mirza/catalyst_xms_postgresql_jt_stg` | `f/flows_engineering/xms_catalyst_mongo_stg` |
| prod | `u/mirza/catalyst_xms_postgresql_voila_prod` | `u/mirza/catalyst_xms_postgresql_jt_prod` | `f/voila_anomalies/voila_mongodb_prod` |

Note the structured sibling `migration-order-v2/` tool points only at **prod**
resources with `..._jamtangan_prod` / `f/voila_anomalies/voila_mongodb_prod` — do not
cross-wire.

## Local run

`go run ./migration-order-v2-windmill/cmd` — the runner checks that
`XMS_CATALYST_DSN`, `XMS_CATALYST_MONGO_URI`, `MIGRATION_SCHEMA` are set, but never
passes their values into `inner.Main`, so it always fails with
`catalyst dsn could not be resolved`. Effective runtime is Windmill only.

Runner env vars: `MIGRATION_SCHEMA`, `MIGRATION_ENVIRONMENT` (default `dev`),
`MIGRATION_START_DATE`, `MIGRATION_END_DATE`, `MIGRATION_ORDER_NUMBERS`. At least one
of start date, end date, or order numbers is required.

## Gotchas

- Mongo logging is **dead code here**: `mongoResourceOrURI` is always `""` → Mongo
  client stays `nil` → `saveLog` no-ops. No `migration_order_v2_log` documents are
  written in this version.
- `ProcessingStatusCompleted = 19`. Orders are selected with `status_id = 5`,
  `order_version = 1`, not deleted, completed more than 5 days ago
  (`completed_at + INTERVAL '5 days' < NOW()`).
- Schema switch: `schema == "jamtangan"` selects the jamtangan resource and the
  fulfillment code prefix `"J"` (else `"V"`).
- Multi-tenant raw SQL: every query is `fmt.Sprintf`-built with the schema-prefixed
  table name; enum casts use `%s.processing_method_enum`.
- No tests in this repo — verify via a code review of the SQL and business logic.
