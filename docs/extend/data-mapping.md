# Extend Commerce (Lxir) -- Optiply Data Mapping

## Integration Overview

| Field | Value |
|---|---|
| Platform | Extend Commerce (Lxir) |
| API Type | REST (JSON) |
| Base URL | `https://s05.extend.se/RESTAPI/v1_0/{client}/` |
| Auth | Custom header: `ExtendBasicAuthorization: Basic <base64(username:password)>` |
| Swagger | https://developer.lxir.se/RESTAPI |
| Pagination | Page-number based (`?pageNumber=N`, starts at 1, empty array = end) |
| Scope | Full Generic: Products, Suppliers, SupplierProducts, SellOrders, BuyOrders, BuyOrderLines |
| Client | `MAXGAMING` (configurable) |

## Authentication

Authentication uses a custom HTTP header (not standard `Authorization`):

```
ExtendBasicAuthorization: Basic <base64(username:password)>
```

The Base64 value is computed from `username:password` concatenated with a colon.

## Warehouses (MAXGAMING Tenant)

| Code | Name | Role | Sync |
|------|------|------|------|
| MAXGAMING | Optimus (Main Warehouse) | Primary | Yes |
| MAXGAMING2 | Johanneslund (Main Warehouse) | Primary (active) | Yes |
| MAXRMA | Aterkop (BI Warehouse) | RMA/Returns | Exclude |

The `warehouse_codes` config parameter controls which warehouses are included.
Recommendation: set to `["MAXGAMING", "MAXGAMING2"]` to exclude MAXRMA.

## Field Mapping: Products

### Products -> Optiply Products

The Products list endpoint returns one row per product-per-warehouse. The tap deduplicates by `productNumber`
and aggregates `availableBalance` across warehouses. The tap also fetches detail per product to get `productSuppliers`.

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `productNumber` | `remoteId` | string | Unique product identifier |
| `productName` | `name` | string | Max 255 chars |
| `gtinNumberList` | `eanCode` | string | GTIN/EAN barcode |
| `manufacturerProductNumber` | `articleCode` | string | Manufacturer SKU |
| `cost` | `price` | float | Purchase cost |
| `enabled` | `status` | string | true -> "enabled", false -> "disabled" |
| sum(`availableBalance`) for MAXGAMING+MAXGAMING2 | `stockLevel` | int | Aggregated across allowed warehouses |
| `createDate` | `created_at` | datetime | ISO 8601 |
| `createDate` | `updated_at` | datetime | No changedDate in list response |
| false | `unlimitedStock` | bool | Always false for warehouse products |

### Stock Aggregation

Products appear once per warehouse in the list endpoint. The ETL:
1. Groups by `productNumber`
2. Sums `availableBalance` across configured warehouses (default: MAXGAMING + MAXGAMING2)
3. Excludes MAXRMA (RMA/returns warehouse)
4. Takes metadata (name, EAN, price, enabled) from first occurrence

## Field Mapping: Suppliers

### Product Suppliers -> Optiply Suppliers

Suppliers are derived from the product detail endpoint's `productSuppliers` array.
Each unique `supplierAgreementNumber` becomes a Supplier.

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `supplierAgreementNumber` | `remoteId` | string | Integer cast to string |
| `supplierAgreement` | `name` | string | Agreement name, max 255 chars |
| `manufacturingLeadTimeHour / 24` | `deliveryTime` | int | Rounded days, null if 0 |
| current UTC | `updated_at` | datetime | No date on supplier agreements |

## Field Mapping: SupplierProducts

### Product Suppliers -> Optiply SupplierProducts

Each product-supplier pair from the detail endpoint becomes a SupplierProduct.

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `{productNumber}_{supplierAgreementNumber}` | `remoteId` | string | Composite key |
| `supplierProductName` or `productName` | `name` | string | Fallback to product name |
| `supplierProductNumber` | `skuCode` | string | Supplier's product code |
| `price / (1 + vatPercent/100)` | `price` | float | Purchase price excl. VAT |
| `productNumber` | `productId` | int | Remote FK -> product remoteId, resolved via snapshot |
| `supplierAgreementNumber` | `supplierId` | int | Remote FK -> supplier remoteId, resolved via snapshot |
| `manufacturingLeadTimeHour / 24` | `deliveryTime` | int | Rounded days, null if 0 |
| `createDate` (product's) | `updated_at` | datetime | Fallback date |

## Field Mapping: SellOrders

### CustomerOrders -> Optiply SellOrders

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `orderNumber` | `remoteId` | string | |
| `orderDate` | `placed` | datetime | |
| `totalPrice` | `totalValue` | float | Already excl. VAT |
| `changeDate` | `updated_at` | datetime | |
| `changeDate` (if status completed) | `completed` | datetime | Set when status in Delivered/Invoiced/Completed/FullyDelivered |
| `changeDate` (if Cancelled) | `deleted_at` | datetime | |

### Completed Status Mapping

| Extend orderStatus | Optiply `completed` | Optiply `deleted_at` |
|---|---|---|
| Incoming | null | null |
| Processing | null | null |
| Delivered | changeDate | null |
| Invoiced | changeDate | null |
| Completed | changeDate | null |
| FullyDelivered | changeDate | null |
| Cancelled | null | changeDate |

### CustomerOrder Rows -> Optiply SellOrderLines

Lines are embedded in SellOrders via `get_sell_order_withlines_payload`.

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `{orderNumber}-{position}` | `remoteId` | string | Composite key |
| `salesData.quantity` | `quantity` | int | Cast from float |
| `product.productNumber` | `productId` | int | Remote FK, resolved via snapshot |
| `orderNumber` | `sellOrderId` | string | Remote FK |
| `salesData.quantity * salesData.unitPrice` | `subtotalValue` | float | Excl. VAT (vatPercent is 0.0) |
| `changeDate` | `updated_at` | datetime | Line-level or order-level fallback |

**Filter:** Rows where `supplyMode == "Freight"` are excluded (shipping/freight lines).

## Field Mapping: BuyOrders

### PurchaseOrder -> Optiply BuyOrder (buy_orders.csv)

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `header.purchaseNumber` | `remoteId` | string | e.g. "RP-10748" |
| `header.supplier.supplierNumber` | `supplierId` | int | Links to Supplier remoteId, resolved via snapshot |
| `header.createDate` | `placed` | datetime | ISO 8601 with timezone |
| `header.createDate` | `updated_at` | datetime | No explicit updated_at in Extend |
| sum(rows: qty * unitPrice) | `totalValue` | float | Excl. VAT, computed from `purchaseDataProductUnit` |
| Received AND !isOpen -> max(rows.statusChangeDate) | `completed` | datetime | Null when still open |
| status == "Cancelled" | `deleted_at` | datetime | Current UTC when Cancelled |

### PurchaseOrder Rows -> Optiply BuyOrderLines (buy_order_lines.csv)

| Extend Field | Optiply Field | Type | Notes |
|---|---|---|---|
| `{purchaseNumber}-{position}` | `remoteId` | string | e.g. "RP-10748-10" |
| `row.productNumber` | `productId` | int | Links to Product remoteId, resolved via snapshot |
| `header.purchaseNumber` | `buyOrderId` | int | Links to BuyOrder, resolved via snapshot |
| `row.purchaseDataPurchaseUnit.quantity` | `quantity` | int | Cast from float to int |
| `row.purchaseDataProductUnit.quantity * unitPrice` | `subtotalValue` | float | Excl. VAT |
| `row.expectedDeliveryDate` or `row.statusChangeDate` | `updated_at` | datetime | expectedDeliveryDate preferred, statusChangeDate fallback |

## Status Mapping

### PurchaseOrder Status

| Extend Status | isOpen | isReceived | Optiply Mapping |
|---|---|---|---|
| Ordered | true | false | Active (no completed date) |
| PartiallyReceived | true | false | Active (no completed date) |
| Received | false | true | Completed (set completed datetime) |
| Cancelled | false | false | Deleted (set deleted_at) |

## Config Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `api_url` | string | `https://s05.extend.se/RESTAPI` | Base URL |
| `client` | string | (required) | Client shortname e.g. MAXGAMING |
| `username` | string | (required) | API username |
| `password` | string | (required) | API password |
| `start_date` | datetime | (none) | Earliest date to sync from |
| `warehouse_codes` | array | (all) | Filter by warehouse codes (tap-level) |
| `allowed_warehouses` | array | MAXGAMING, MAXGAMING2 | Warehouses for stock aggregation (ETL-level) |
| `sync_products` | bool | true | Enable/disable product sync |
| `sync_suppliers` | bool | true | Enable/disable supplier sync |
| `sync_supplier_products` | bool | true | Enable/disable supplier product sync |
| `sync_sell_orders` | bool | true | Enable/disable sell order sync |
| `sync_buy_orders` | bool | true | Enable/disable buy order sync |
| `default_warehouse` | string | MAXGAMING2 | Default warehouse for target writes |

## Bidirectional Guard

When the target writes PurchaseOrders back to Extend, it sets:
- `reference`: `Optiply-{buyOrderId}`
- `notes`: `"Created by Optiply"`

The ETL filters out any POs where `externalOrderNumber` or `reference` contains "optiply" (case-insensitive) to prevent feedback loops.

## Price Handling

- All prices are **excluding VAT** (`vatPercent` is typically 0.0)
- Products: `cost` field is the purchase cost
- SupplierProducts: `price / (1 + vatPercent/100)` to convert to excl. VAT
- SellOrders: `totalPrice` is already excl. VAT
- BuyOrders: `totalValue` = sum of `purchaseDataProductUnit.quantity * purchaseDataProductUnit.unitPrice` across all rows
- BuyOrderLines: `subtotalValue` per line = `purchaseDataProductUnit.quantity * purchaseDataProductUnit.unitPrice`
- Currency in source data is typically SEK but may vary per supplier

## Entity Order

Per Optiply standards, the ETL follows: **Products > Suppliers > SupplierProducts > SellOrders > BuyOrders > BuyOrderLines** > ReceiptLines

API operation order: DELETE > POST > PATCH

## Tap Streams

| Stream | Endpoint | Response Format | Replication Key | Notes |
|---|---|---|---|---|
| `products` | `GET /Products?pageNumber=N` | Bare array | `createDate` | Deduplicates by productNumber, fetches detail per product |
| `customer_orders` | `GET /CustomerOrders?pageNumber=N&createDateFrom=...` | Bare array | `changeDate` | Always requires createDateFrom to avoid timeout |
| `purchase_orders` | `GET /PurchaseOrders?pageNumber=N` | `{purchaseOrderList: [...]}` | `createDate` | Fetches detail per PO |
