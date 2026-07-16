# Migration Order V2

## Overview

Tool untuk migrasi data Order V2 di XMS Catalyst. Memproses order dan fulfillment berdasarkan aturan bisnis yang sudah ditentukan, multi-tenant (schema `voila` / `jamtangan`).

## Dependencies

- Go 1.24+
- PostgreSQL (via GORM + pgx)
- MongoDB (via mongo-driver)
- Module: `windmill/migration-order-v2`

## File Structure

```
migration-order-v2/
├── docs.md                           # Dokumentasi ini
├── main.go                           # Package inner — entrypoint, DSN, Mongo init
├── cmd/
│   └── main.go                       # Package main — runner lokal
├── repository/
│   ├── order.repo.go                 # Order, OrderItem models + query/write order
│   ├── fulfilment.repo.go            # Fulfillment, SubStatus models + query/write FF
│   └── mongo.repo.go                 # MigrationLog model + save ke MongoDB
└── usecase/
    └── usecase.go                    # Business logic: decision tree, processing methods
```

## Entrypoint

**Windmill Input Parameters:**

| Parameter | Tipe | Contoh | Deskripsi |
|---|---|---|---|
| `xmsCatalystDSN` | `string` | resource path | DSN atau Windmill resource |
| `schema` | `string` | `"voila"` / `"jamtangan"` | Schema database target |
| `startDate` | `string` | `"2026-07-01"` | Filter tanggal awal |
| `endDate` | `string` | `"2026-07-15"` | Filter tanggal akhir |
| `orderNumbers` | `string` | `"INV-001,INV-002"` atau `""` | Filter opsional, comma-separated |
| `mongoResourceOrURI` | `string` | `"f/voila_anomalies/..."` atau `""` | Resource path Windmill atau URI MongoDB |

```go
func Main(xmsCatalystDSN, schema, startDate, endDate, orderNumbers, mongoResourceOrURI string) (interface{}, error)
```

**Local Runner:**

```sh
cp .env.example .env
# isi XMS_CATALYST_VOILA_DSN atau XMS_CATALYST_JAMTANGAN_DSN
# isi XMS_CATALYST_MONGO_URI (optional, untuk log migrasi ke Mongo)

go run ./migration-order-v2/cmd
```

## MongoDB Logging

Setiap order yang diproses (SUCCESS / ERROR / SKIPPED) akan dicatat ke collection `migration_order_v2_log` di MongoDB.

### Document Structure

```json
{
  "order_id": 123456789,
  "order_number": "26071526214306",
  "schema": "voila",
  "case": "CREATE_HOME_DELIVERY",
  "status": "SUCCESS",
  "action": "CREATE FF",
  "detail": "home_delivery, 2 items",
  "processing_method": "HOME_DELIVERY",
  "fulfillment_ids": [987654],
  "order_version": 2,
  "filter": {
    "start_date": "2026-07-01",
    "end_date": "2026-07-15",
    "order_numbers": ""
  },
  "migrated_at": "2026-07-16T14:30:00Z"
}
```

| Field | Tipe | Deskripsi |
|---|---|---|
| `order_id` | `int64` | ID order dari `tr_order` |
| `order_number` | `string` | Nomor order |
| `schema` | `string` | Schema tempat migrasi (`voila` / `jamtangan`) |
| `case` | `string` | Kategori: `CREATE_CARRY_OUT` / `CREATE_HOME_DELIVERY` / `UPDATE_FF` / `SKIP` |
| `status` | `string` | Status: `SUCCESS` / `ERROR` / `SKIPPED` |
| `action` | `string` | Aksi: `CREATE FF` / `UPDATE FF` / `SKIP` / `UPDATE ORDER` |
| `detail` | `string` | Detail pesan |
| `processing_method` | `string` | Processing method yang dipakai |
| `fulfillment_ids` | `[]int64` | Array ID fulfillment terkait |
| `order_version` | `int` | `2` — versi setelah migrasi |
| `filter` | `object` | Filter migrasi yang digunakan |
| `migrated_at` | `time.Time` | Timestamp migrasi (ISO 8601 UTC) |

### Error Handling

- Jika MongoDB sedang down, migration **tetap lanjut** tanpa rollback
- Error Mongo hanya di `fmt.Printf("[WARN] ...")` sebagai warning
- Jika `mongoResourceOrURI` kosong, logging ke Mongo di-skip (graceful)

## Relasi Status

### ms_order_status

Tabel master status order.

| id | code | name | priority |
|---|---|---|---|
| 1 | pending | Pending | 2 |
| 2 | on_process | On Process | 5 |
| 3 | shipped | Shipped | 4 |
| 4 | cancel | Canceled | 1 |
| 5 | completed | Completed | 6 |
| 6 | rejected | Rejected | 7 |
| 7 | pre_order | Pre Order | 8 |
| 8 | draft | Draft | 9 |
| 9 | delivered | Delivered | 3 |

### ms_order_sub_status

Tabel sub-status yang menghubungkan `tr_fulfillment.processing_status_id` dengan `ms_order_status.id`.

Relasi:
- `tr_fulfillment.processing_status_id` = `ms_order_sub_status.id`
- `ms_order_sub_status.order_status_id` = `ms_order_status.id`

Sub-status yang relevan:

| id | code | order_status_id | Keterangan |
|---|---|---|---|
| 19 | completed | 5 | Completed — status final untuk fulfillment |
| 2 | fulfillment_on_process | 2 | On Process |
| 3 | shipping_in_progress | 3 | Shipped |
| 4 | waiting_for_shipping | 3 | Shipped |
| 5 | apg_failed | 6 | Rejected |
| 6 | fulfillment_rejected | 6 | Rejected |
| 7 | out_of_stock | 6 | Rejected |
| 8 | po_confirmed | 7 | Pre Order |
| 9 | awaiting_product | 7 | Pre Order |
| 10 | awaiting_full_payment | 7 | Pre Order |
| 11 | pending_pre_order | 1 | Pending |
| 17 | delivered | 9 | Delivered |
| 20 | canceled | 4 | Canceled |

### Alur Mapping Status

**CASE 1 & 2 (CREATE FF):**
```
tr_order.status_id = 5 (Completed)
  → ms_order_sub_status WHERE order_status_id = 5 → id = 19 (completed)
  → tr_fulfillment.processing_status_id = 19

status_ids = [5]       (ms_order_sub_status[19].order_status_id)
sub_status_ids = [19]  (ms_order_sub_status[19].id)
```

**CASE 3 (FF exists):**
```
tr_fulfillment.processing_status_id = X
  → ms_order_sub_status WHERE id = X → ambil order_status_id dan id

Untuk setiap fulfillment (harus unique):
  status_ids[] = ms_order_sub_status[X].order_status_id
  sub_status_ids[] = ms_order_sub_status[X].id
```

### Format Kolom

`tr_order.status_ids` dan `tr_order.sub_status_ids` adalah **`_int4` array** (PostgreSQL native array), bukan JSON string.

Di Go, digunakan `pq.Int64Array` atau `[]int64` dengan driver pgx.

## Database Schema (Target Tables)

### tr_order
```sql
CREATE TABLE {schema}.tr_order (
    id bigserial NOT NULL,
    order_number varchar NULL,
    status_id int4 NOT NULL,
    sub_status_id int4 NOT NULL,
    shipping_method {schema}.shipping_method NULL,
    sales_channel_code varchar NULL,
    payment_status varchar NULL,
    processed_at timestamptz NULL,
    office_id int4 NOT NULL,
    status_ids _int4 NULL,           -- ARRAY, target migrasi
    sub_status_ids _int4 NULL,       -- ARRAY, target migrasi
    ...
);
```

### tr_fulfillment
```sql
CREATE TABLE {schema}.tr_fulfillment (
    id bigserial NOT NULL,
    code varchar NOT NULL,            -- format: <buc><yymmdd><order_number><seq>
    order_id int8 NOT NULL,
    status {schema}.fulfillment_status NULL,
    channel {schema}.sales_channel NOT NULL,
    store_name varchar NULL,
    office_id int4 NOT NULL,
    payment_status varchar DEFAULT 'PAID',
    payment_date timestamptz DEFAULT now(),
    package_type varchar DEFAULT '',
    order_number varchar DEFAULT '',
    is_visible bool DEFAULT true,
    processing_method {schema}.processing_method_enum NULL,
    processing_status_id int4 NULL,
    sent_date timestamptz NULL,
    sla_min varchar(10) NULL,
    sla_max varchar(10) NULL,
    sla_measurement varchar(10) NULL,
    pickup_office_id int4 DEFAULT 0,
    pickup_office_name varchar NULL,
    pickup_pic_id int4 DEFAULT 0,
    pickup_pic_name varchar NULL,
    pickup_customer_name varchar NULL,
    receiver_pic_id int4 DEFAULT 0,
    receiver_pic_name varchar NULL,
    receiver_note varchar NULL,
    ...
);
```

### tr_fulfillment_product
```sql
CREATE TABLE {schema}.tr_fulfillment_product (
    id serial4 NOT NULL,
    fulfillment_id int8 NOT NULL,
    order_item_id int8 NULL,
    variant_id int4 NOT NULL,
    variant_sku varchar NOT NULL,
    variant_name varchar NULL,
    qty int4 NOT NULL,
    product_name varchar NOT NULL,
    brand_name varchar NOT NULL,
    price int8 DEFAULT 0,
    po_status {schema}.fulfillment_product_po_status NULL,  -- 'PO_CONFIRMED'
    po_processed_at timestamptz NULL,
    ...
);
```

### tr_fulfillment_item_code
```sql
CREATE TABLE {schema}.tr_fulfillment_item_code (
    id bigserial NOT NULL,
    fulfillment_id int8 NULL,
    variant_id int4 NOT NULL,
    item_code varchar NULL,
    order_id int8 NULL,
    order_item_id int8 NULL,
    fulfillment_product_id int4 NULL,
    ...
);
```

## Business Logic

### Decision Tree

```
1. Query orders berdasarkan filter (date range + optional order numbers)
2. Loop per order:
   a. Cek apakah tr_fulfillment sudah ada untuk order ini
      ├── YES → CASE 3: UPDATE order + UPDATE fulfillment (per processing method)
      └── NO  → Cek shipping_method & is_consign
                 ├── CARRY_OUT + !consign → CASE 1: CREATE FF (OFFLINE)
                 └── HOME_DELIVERY/SHIPPING + consign → CASE 2: CREATE FF (consign)
```

### CASE 1 — CARRY_OUT, Non-Consignment, No Fulfillment

**Action:** UPDATE `tr_order` + INSERT `tr_fulfillment` + INSERT `tr_fulfillment_product` + INSERT `tr_fulfillment_item_code`

**Status Mapping:**
- `tr_order.status_id = 5` (Completed)
- `processing_status_id = 19` (ms_order_sub_status: completed → order_status_id = 5)
- `status_ids = [5]`
- `sub_status_ids = [19]`

**Fulfillment Fields:**

| Field | Value |
|---|---|
| `code` | `{buc}{yymmdd}{order_number}{nextval('tr_fulfillment_id_seq')}` |
| `status` | `'COMPLETED'` |
| `channel` | `'OFFLINE'` |
| `store_name` | `ms_office.name` WHERE `id = tr_order.office_id` |
| `office_id` | `tr_order.office_id` |
| `payment_status` | `tr_order.payment_status` |
| `payment_date` | `tr_order.processed_at` |
| `package_type` | `ms_product_category.name` dari semua produk (unique, delimiter `,`) |
| `processing_method` | `tr_order.shipping_method` |
| `processing_status_id` | `19` |
| `is_visible` | `FALSE` |

### CASE 2 — HOME_DELIVERY/SHIPPING, Consignment, No Fulfillment

**Action:** UPDATE `tr_order` + INSERT `tr_fulfillment` + INSERT `tr_fulfillment_product` + INSERT `tr_fulfillment_item_code`

**Status Mapping:** Sama dengan CASE 1.

**Fulfillment Fields (perbedaan dari CASE 1):**

| Field | Value |
|---|---|
| `channel` | `tr_order.sales_channel_code` |
| `store_name` | `ms_office.name` WHERE `id = tr_order_consign.office_id` |
| `office_id` | `tr_order_consign.office_id` |
| `processing_method` | `'HOME_DELIVERY'` |
| `is_visible` | `FALSE` |

### CASE 3 — Fulfillment Already Exists

**Action:** UPDATE `tr_order` + UPDATE `tr_fulfillment` (per processing method)

**Status Mapping:**
- `status_ids` = distinct `ms_order_sub_substatus.order_status_id` dari semua fulfillment
- `sub_status_ids` = distinct `ms_order_sub_status.id` dari semua fulfillment

**Fulfillment Fields:**

| Field | Value |
|---|---|
| `processing_method` | `tr_order.shipping_method` |
| `processing_status_id` | `19` (Completed) — hanya jika `is_replaced = false` |
| `sent_date` | `NOW()` jika processing_status_id = delivered |
| `sla_min/max/measurement` | Estimasi pengiriman |

### Processing Method Functions (terpisah per method)

| Function | Method | is_visible | Notes |
|---|---|---|---|
| `processCarryOut()` | `CARRY_OUT` | `FALSE` | |
| `processHomeDelivery()` | `HOME_DELIVERY` | `TRUE` | |
| `processConsignment()` | `CONSIGNMENT` | `FALSE` | |
| `processPreOrder()` | `PRE_ORDER` | `FALSE` | |
| `processPickupInStore()` | `PICKUP_IN_STORE` | lihat catatan | `FALSE` jika shipping_method = OTHER_STORE, else `TRUE` |
| `processDefault()` | lainnya | `TRUE` | fallback |

### Fulfillment Code Format

```
{business_unit_code}{yymmdd}{order_number}{seq}
```

- `business_unit_code`: dari ms_business_unit (TODO: konfirmasi)
- `yymmdd`: tanggal proses (`NOW()`)
- `order_number`: dari `tr_order.order_number`
- `seq`: `nextval('tr_fulfillment_id_seq')`

## Fulfillment Product & Item Code

Untuk CASE 1 & 2, setiap `tr_order_item` di-order akan dibuatkan:

**tr_fulfillment_product:**
| Field | Source |
|---|---|
| `fulfillment_id` | ID dari hasil INSERT fulfillment |
| `order_item_id` | `tr_order_item.id` |
| `variant_id` | `tr_order_item.variant_id` |
| `variant_sku` | `tr_order_item.variant_sku` |
| `variant_name` | `tr_order_item.variant_name` |
| `qty` | `tr_order_item.qty` |
| `product_name` | `tr_order_item.product_name` |
| `brand_name` | dari `tr_order_item.brand_id` → `ms_brand.name` |
| `price` | `tr_order_item.selling_price` |
| `po_status` | `'PO_CONFIRMED'` |
| `po_processed_at` | `NOW()` |

**tr_fulfillment_item_code:**
| Field | Source |
|---|---|
| `fulfillment_id` | ID dari hasil INSERT fulfillment |
| `order_item_id` | `tr_order_item.id` |
| `fulfillment_product_id` | ID dari hasil INSERT fulfillment_product |
| `variant_id` | `tr_order_item.variant_id` |
| `item_code` | TODO — perlu konfirmasi sumber |

## Execution

### Transaction per Order

Setiap order diproses dalam **satu transaksi** (BEGIN/COMMIT). Jika terjadi error, rollback per order (tidak mengganggu order lain).

```go
for _, o := range orders {
    tx := db.Begin()

    switch determineCase(o) {
    case 1:
        updateOrder(tx, schema, o, statusIDs, subStatusIDs)
        ffID := insertFulfillment(tx, schema, o, fulfillmentData)
        for _, item := range items {
            fpID := insertFulfillmentProduct(tx, schema, ffID, item)
            insertFulfillmentItemCode(tx, schema, ffID, fpID, item)
        }
    case 2:
        // similar to case 1 with consignment data
    case 3:
        updateOrder(tx, schema, o, statusIDs, subStatusIDs)
        processFn := getProcessor(o.ShippingMethod)
        processFn(tx, schema, o, fulfillment)
    }

    tx.Commit()
}
```

### Error Handling

- Error per order → di-skip, dicatat di laporan, transaksi di-rollback
- Tool lanjut ke order berikutnya
- Output laporan: jumlah success, error, skipped

### Perhatian Khusus

> **ORDER YANG SUDAH PERNAH DI-REJECT DAN DI-EDIT TIDAK ADA FF NYA**
>
> Untuk CASE 3, jika order tidak memiliki fulfillment sama sekali (tidak ada baris di `tr_fulfillment` untuk `order_id` tersebut), maka order akan di-skip dengan status "SKIP — rejected/edited order, no fulfillment found". Ini perlu penanganan khusus karena `status_ids` dan `sub_status_ids` tidak bisa dihitung dari fulfillment.

## Output

### Format Markdown

```markdown
##### Migration Order V2 — voila, 2026-07-01 to 2026-07-15

| Order ID | Order Number | Action | Status | Detail |
|---|---|---|---|---|
| 123 | INV-001 | CREATE FF | OK | fulfillment_id=456, 2 items |
| 124 | INV-002 | UPDATE FF | OK | processing_status_id=19 |
| 125 | INV-003 | SKIP | ERROR | no FF found (rejected order) |

**Summary:** 10 processed, 8 success, 2 error, 1 skipped
```

### Nil vs Data

Output `nil, nil` jika tidak ada order dalam filter.

## Architecture

```
┌──────────────┐     ┌─────────────────────────────┐     ┌──────────────┐
│   Windmill   │────▶│    migration-order-v2       │────▶│  PostgreSQL   │
│   (trigger)  │     │    (Go script, read+write)   │     │  (Catalyst)  │
└──────────────┘     │                             │     │ voila/jamtangan│
                     │ Read:  tr_order              │     └──────────────┘
                     │        tr_order_item         │
                     │        tr_fulfillment        │
                     │        ms_order_status       │
                     │        ms_order_sub_status   │
                     │        ms_product_category   │
                     │        ms_office             │
                     │        tr_order_consign      │
                     │        ms_brand              │
                     │                              │
                     │ Write: tr_order (UPDATE)     │
                     │        tr_fulfillment        │
                     │        tr_fulfillment_product│
                     │        tr_fulfillment_item_code│
                     └──────────────────────────────┘
```

## Open Items

| # | Item | Status |
|---|---|---|
| 1 | Business unit code sumbernya dari mana? | ✅ `nextval('tr_fulfillment_id_seq')` |
| 2 | `item_code` di `tr_fulfillment_item_code` dari mana? | ❓ Perlu konfirmasi |
| 3 | PO status? | ✅ `'PO_CONFIRMED'` |
| 4 | Payment date? | ✅ `tr_order.processed_at` |
| 5 | Multi-tenant? | ✅ parameter `schema` |
