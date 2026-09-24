# AGENTS.md — migration-order-v2

Data migration tool for XMS Catalyst orders (Order V2). **Writes** to Catalyst
PostgreSQL (updates `tr_order`, inserts `tr_fulfillment`, `tr_fulfillment_product`,
`tr_fulfillment_item_code`) inside a per-order transaction. Single flattened file —
the structured `migration-order-v2-windmill/` sibling (repository/usecase split) has
been removed.

## CRITICAL: Read root RULES.md

Hard rules: never read/open/modify `.env` or `.env.*` (except `.env.example`);
respond in Bahasa Indonesia.

## Entrypoint

- `main.go` — package `inner`, import path `windmill/migration-order-v2`
  (NOT `migration-order-v2-windmill`). Exports:

  ```go
  func Main(migrationParams struct {
      Environment  string `json:"environment"`
      IsTesting    bool   `json:"is_testing"`
      Schema       string `json:"schema"`
      OrderNumbers string `json:"order_numbers"`
      StartDate    string `json:"start_date"`
      EndDate      string `json:"end_date"`
      Limit        int    `json:"limit"`
  }, xmsCatalystDSN, mongoResourceOrURI string) (interface{}, error)
  ```

- `cmd/main.go` — package `main` local runner. **It does pass** the env values into
  `inner.Main` (`cmd/main.go:78`).

## Input validation (`Main`, main.go:1496)

- `Environment` defaults to `"dev"`; valid values `dev`, `stg`, `prod` (else error).
- `Schema` is required (`voila` or `jamtangan`; `jamtangan` also switches resources
  and code prefix).
- At least one of `StartDate`, `EndDate`, `OrderNumbers` is required.
- `Limit` is optional; `0` means no `LIMIT`.

## Resource resolution

`resourceByEnv` (main.go:27) hardcodes resources per environment:

| Env | voila | jamtangan | mongo |
|---|---|---|---|
| dev (default) | `u/mirza/catalyst_xms_postgresql_voila_dev` | `u/mirza/catalyst_xms_postgresql_jt_dev` | `f/flows_engineering/xms_catalyst_mongo_dev` |
| stg | `u/mirza/catalyst_xms_postgresql_voila_stg` | `u/mirza/catalyst_xms_postgresql_jt_stg` | `f/flows_engineering/xms_catalyst_mongo_stg` |
| prod | `u/mirza/catalyst_xms_postgresql_voila_prod` | `u/mirza/catalyst_xms_postgresql_jt_prod` | `f/flows_engineering/xms_catalyst_mongo_prod` |

Both resolvers accept **either** a direct connection string (local) **or** a Windmill
resource path (Windmill):

- `resolveDSN(provided, resourcePath)` (main.go:1466) — if `provided` starts with
  `postgres://`/`postgresql://` it is used directly; otherwise
  `wmill.GetResource(resourcePath)` is called (uses the `dsn` key, else builds from
  `user/password/host/port/dbname`). Returns `""` when neither works.
- `resolveMongoURI(provided, resourcePath)` (main.go:1378) — if `provided` starts with
  `mongodb://`/`mongodb+srv://` it is used directly; otherwise `wmill.GetResource`
  builds a URI from `credential.username/password`, `servers[0].host/port`, `db`.
  Returns `""` when the resource cannot be resolved and no usable direct value exists.

In Windmill, call `Main` with `xmsCatalystDSN` and `mongoResourceOrURI` empty and let
resource resolution run. Locally the runner sends `XMS_CATALYST_DSN` and
`XMS_CATALYST_MONGO_URI` directly, so those must be real connection strings.

> Watch-out: prod mongo in code is `f/flows_engineering/xms_catalyst_mongo_prod`.
> Confirm this resource exists in Windmill — if it does not, `mongoRepo` stays `nil`
> and logging no-ops.

## Local run

`go run ./migration-order-v2/cmd` — the runner loads `.env` and requires
`XMS_CATALYST_DSN`, `MIGRATION_SCHEMA`, `XMS_CATALYST_MONGO_URI`, and one of the
date/order filters.

Runner env vars: `XMS_CATALYST_DSN`, `XMS_CATALYST_MONGO_URI`, `MIGRATION_SCHEMA`,
`MIGRATION_ENVIRONMENT` (default `dev`), `MIGRATION_IS_TESTING` (`"true"`),
`MIGRATION_START_DATE`, `MIGRATION_END_DATE`, `MIGRATION_ORDER_NUMBERS`,
`MIGRATION_LIMIT`.

## Selection (`queryOrders`, main.go:769)

```
o.is_deleted = false
o.deleted_at IS NULL
o.order_version = 1
o.status_id = 5
o.completed_at + INTERVAL '5 days' < NOW()   -- only when is_testing = false
[optional] o.created_at range from start/end date
[optional] o.order_number IN (...)
ORDER BY o.id [LIMIT n]
```

`is_testing = true` drops the 5-day guard (dev/local convenience).

## Mongo logging

`saveLog` (main.go:736) writes to the `migration_order_v2_log` collection whenever
`mongoURI` resolves (db name via `extractDBName`, default `"voila"`). `mongo.Connect`
is lazy, so a non-empty URI alone makes `mongoRepo` non-nil. If Mongo cannot be
resolved, `saveLog` no-ops with a `[DEBUG]` line. `MigrationLog.OrderVersion` is
hardcoded to `2` for every outcome, including SKIP/ERROR.

> The old note claiming logging is "dead code" was **wrong** — verified against the
> current code (main.go:1532-1551).

## Skip deduplication (Opsi C)

`processOrders` loads the set of order IDs already logged as `SKIPPED` for the
current schema via `findSkippedOrderIDs` (`Distinct` on `order_id` where
`schema = <schema>` and `status = "SKIPPED"`), then passes them to `queryOrders`,
which adds `o.id NOT IN (...)` (main.go:828-830). This prevents orders that always
end in SKIP from being re-selected on every run.

Caveats:

- Only works when Mongo resolves; with `mongoRepo == nil` no exclusion happens and
  skips recur.
- A skipped order is locked **permanently** per schema — there is no automatic reset
  if it later becomes migratable (e.g. a non-COMPLETED fulfillment becomes
  COMPLETED). Remove the corresponding `migration_order_v2_log` SKIPPED document(s)
  to allow reprocessing.
- Exclusion is by literal `NOT IN` list built with `joinIDs`; it grows with the
  number of skipped orders.

## Couple handling (jamtangan only)

`tr_order_item.is_couple` is unreliable: jamtangan couple orders can have it `false`.
The authoritative signal is `ms_product.couple_ids` (array of child product IDs).

- `resolveCoupleInfo` (called from `queryOrderItems`, gated to
  `schema == "jamtangan"`) loads `couple_ids` for each order item's `product_id` and
  sets `OrderItem.CoupleIDs`; `IsCouple` is overwritten to `true` when non-empty.
- Coverage (`queryCoveredVariants`): a couple item counts as covered when **all** of
  its `couple_ids` are present among the order's
  `tr_fulfillment_product.variant_id`. Assumes child `product_id == variant_id`
  (holds for current jamtangan data).
- Update (`updateFulfillmentProductOrderItemID`): sets `is_couple = true` and fills
  `order_item_id` on every child row (`variant_id IN couple_ids`).
- Create (`insertFulfillmentProducts`): expands a couple item into one row per child —
  `qty = item.Qty`, `price = ms_product_variant_price.our_price` (child variant),
  `variant_sku`/`variant_name` from `ms_product_variant`,
  `product_name`/`sku_universal`/`brand_id` from `ms_product`, and `image_url` from
  `ms_product_image`.

## Gotchas

- `ProcessingStatusCompleted = 19`.
- **SKIP no longer recurs** thanks to the Opsi C dedup above, but the underlying
  cause remains: the SKIP paths (main.go:352-359) roll back the transaction,
  discarding `updateOrder`'s `order_version = 2`, so the order stays
  `order_version = 1`. The dedup relies on the Mongo SKIPPED log, not on Postgres
  state.
- The other SKIP branch (main.go:260, `isRejectedNoFF`) is effectively dead:
  `queryOrders` forces `status_id = 5`, but `isRejectedNoFF` checks for status in
  `{6,4,8,1}`.
- Coverage mismatch: `queryCoveredVariants` counts fulfillments with **any** status
  (`deleted_at IS NULL`, main.go:1154-1158), while `queryFulfillments` only returns
  `status = 'COMPLETED' AND is_replaced = false` (main.go:999). This can trigger the
  "no items need fulfillment" SKIP for orders whose only fulfillments are not
  COMPLETED.
- Fulfillment code: `"V"` (`"J"` for jamtangan) + `yymmdd` + last 3 digits of order
  number + 4-digit increment parsed from the last `tr_fulfillment.code`.
- Child `image_url` comes from `ms_product_image.url` where `"type" = 'MAIN'`
  (first row by `idx, id`, `is_deleted = false`, `deleted_at IS NULL`), resolved in
  `resolveCoupleChildren`.
- Couple coverage/create assumes child `product_id == variant_id`. If jamtangan ever
  splits them, `resolveCoupleChildren` and `queryCoveredVariants` must map via
  `ms_product_variant`.
- Multi-tenant raw SQL: every query is `fmt.Sprintf`-built with the schema-prefixed
  table name; enum casts use `%s.processing_method_enum`, array casts
  `%s.fulfillment_status`.
- Unused helpers (defined, never called): `resolveSubStatusByOrderStatus`,
  `resolveSubStatusByID`, `generateFulfillmentSeq`, `updateItemCodeValue`.
  `Order.CompletedAt` is selected but not used in logic.
- No tests in this repo — verify via code review of the SQL and business logic.
