# Extend Commerce (Lxir) -- API Collection

All examples use placeholder credentials. Replace `{client}`, `{base64_credentials}`, and resource identifiers with actual values.

Base64 credentials: `echo -n "username:password" | base64`

---

## List Products (all)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/Products?pageNumber=1" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

Response (bare array, one entry per product-per-warehouse):
```json
[
  {
    "productNumber": "1",
    "productName": "Wave: 1 Microphone",
    "createDate": "2020-06-16T10:29:04.88+02:00",
    "productUnit": "ST",
    "cost": 782.09,
    "currency": "SEK",
    "countryOfOrigin": "CN",
    "supplyMode": "Warehouse",
    "manufacturer": "Elgato",
    "manufacturerProductNumber": "10MAA9901",
    "gtinNumberList": "0840006618065",
    "warehouse": "MAXRMA",
    "statisticalCategory1": "Video & Streaming",
    "statisticalCategory2": "Microphones",
    "statisticalCategory3": null,
    "availableBalance": 0,
    "enabled": true,
    "productGroupsAndCategories": {
      "companyGroup": "Askås",
      "financialCategory": "Produkter"
    }
  }
]
```

**Note:** Same product appears multiple times (once per warehouse). Aggregate `availableBalance` across warehouses.

---

## List Products (specific warehouse)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/Products?pageNumber=1&warehouse=MAXGAMING2" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

---

## Get single Product (detail with suppliers)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/Products/{productNumber}" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

Response:
```json
{
  "productNumber": "1",
  "productData": {
    "productName": "Wave: 1 Microphone",
    "cost": 782.09,
    "currency": "SEK",
    "enabled": true,
    "productUnit": "ST",
    "productGTINumber": "0840006618065",
    "manufacturer": "Elgato",
    "manufacturerProductNumber": "10MAA9901",
    "productDates": {
      "createdDate": "2020-06-16T10:29:04.88+02:00",
      "changedDate": "2025-03-11T12:10:03.807+01:00"
    }
  },
  "productSuppliers": [
    {
      "supplierAgreement": "Exertis",
      "supplierProductNumber": "10MAA9901",
      "supplierProductName": "Wave 1 Microphone",
      "price": 1031.0,
      "vatPercent": 25.0,
      "manufacturingLeadTimeHour": 0.0,
      "supplierAgreementNumber": 6,
      "supplierCurrency": "SEK"
    }
  ]
}
```

---

## List CustomerOrders

**CRITICAL:** Always use `createDateFrom` -- this endpoint times out without a date filter.

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/CustomerOrders?pageNumber=1&createDateFrom=2026-01-01T00:00:00" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

Response (bare array):
```json
[
  {
    "orderNumber": "5002110",
    "orderNumberExternal": "5002110",
    "orderType": "Normal",
    "orderStatus": "Incoming",
    "orderDate": "2026-03-13T17:07:13.5+01:00",
    "askedDeliveryDate": "2026-03-13T00:00:00+01:00",
    "slaDate": null,
    "customerNumber": "3450292",
    "customerName": "Jush Ejenguele Mpah",
    "totalPrice": 190.89,
    "changeDate": "2026-03-13T17:07:14.2+01:00"
  }
]
```

---

## Get single CustomerOrder (detail with rows)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/CustomerOrders/{orderNumber}" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

Response:
```json
{
  "orderHeader": {
    "orderNumber": "5002110",
    "orderType": "Normal",
    "orderStatus": "Incoming",
    "orderDate": "2026-03-13T17:07:13.5+01:00",
    "totalPrice": 190.89,
    "changeDate": "2026-03-13T17:07:41.157+01:00"
  },
  "orderRows": [
    {
      "position": 10,
      "orderRowStatus": "Incoming",
      "product": {
        "productNumber": "37537",
        "productName": "Slice 75 HE - White"
      },
      "salesData": {
        "quantity": 1.0,
        "unit": "ST",
        "unitPrice": 156.9,
        "vatPercent": 0.0,
        "currency": "EUR"
      },
      "supplyMode": "Warehouse",
      "warehouse": "MAXGAMING2",
      "changeDate": "2026-03-13T17:07:41.157+01:00"
    },
    {
      "position": 20,
      "orderRowStatus": "Incoming",
      "product": {
        "productNumber": "FR01",
        "productName": "Frakt"
      },
      "salesData": {
        "quantity": 1.0,
        "unit": "ST",
        "unitPrice": 33.99,
        "vatPercent": 0.0,
        "currency": "EUR"
      },
      "supplyMode": "Freight",
      "warehouse": null,
      "changeDate": "2026-03-13T17:07:41.157+01:00"
    }
  ]
}
```

**Note:** Filter out rows where `supplyMode == "Freight"` (shipping lines like "Frakt").

---

## List PurchaseOrders (all)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/PurchaseOrders?pageNumber=1" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

Response:
```json
{
  "purchaseOrderList": [
    {
      "purchaseNumber": "RP-10748",
      "status": "Ordered",
      "createDate": "2026-03-13T15:46:46.247+01:00",
      "warehouse": "MAXGAMING2",
      "isOpen": true,
      "isReceived": false,
      "externalOrderNumber": "",
      "supplierNumber": "4343128",
      "supplierName": "Shanghai Gaer Electronics Technology Co., Ltd.",
      "supplierOrderNumber": "",
      "shippedDate": null
    }
  ]
}
```

---

## List PurchaseOrders (open only, specific warehouse)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/PurchaseOrders?pageNumber=1&IsOpen=true&warehouse=MAXGAMING2" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

---

## List PurchaseOrders (with date range)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/PurchaseOrders?pageNumber=1&createDateFrom=2026-01-01T00:00:00&createDateTo=2026-03-31T23:59:59" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

---

## Get single PurchaseOrder (detail)

```bash
curl -s -X GET \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/PurchaseOrders/{purchaseNumber}" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Accept: application/json"
```

Response:
```json
{
  "header": {
    "purchaseNumber": "RP-10748",
    "externalOrderNumber": "",
    "supplierOrderNumber": "",
    "status": "Ordered",
    "createDate": "2026-03-13T15:46:46.247+01:00",
    "warehouse": "MAXGAMING2",
    "deliveryAddress": {
      "name1": "MAXFPS AB",
      "name2": "Johanneslund (Main Warehouse)",
      "address1": "Johanneslundsvägen 14",
      "postalCode": "19461",
      "city": "Upplands Väsby",
      "countryCode": "SE"
    },
    "supplier": {
      "supplierNumber": "4343128",
      "supplierName": "Shanghai Gaer Electronics Technology Co., Ltd.",
      "supplierAgreementNumber": 4343127,
      "paymentTerms": "100% förskott",
      "deliveryMethod": "CONS_675",
      "deliveryMethodName": "UPS Standard",
      "transportCondition": "EXW",
      "forwarder": "UPS"
    },
    "buyerContact": {
      "name": "Anders Hjelm",
      "email": "anders@maxgaming.se"
    },
    "reference": "",
    "notes": "",
    "shippedDate": null,
    "requestedDeliveryDate": null
  },
  "rows": [
    {
      "position": 10,
      "subPosition": 0,
      "rowStatus": "Ordered",
      "statusChangeDate": "2026-03-13T15:46:46.663+01:00",
      "productNumber": "37131",
      "productName": "Keyboard Storage Case  (87 Keys) - Grey",
      "supplierProductNumber": "6940394611434",
      "purchaseDataPurchaseUnit": {
        "quantity": 24.0,
        "unit": "ST",
        "gtiNumber": "6940394611434"
      },
      "purchaseDataProductUnit": {
        "quantity": 24.0,
        "unit": "ST",
        "unitPrice": 7.86,
        "vatPercent": 0.0,
        "currency": "USD"
      },
      "expectedDeliveryDate": "2026-03-29T11:39:19+02:00",
      "originalExpectedDeliveryDate": "2026-03-29T11:39:19+02:00",
      "shipDate": "2026-03-22T09:39:19+01:00",
      "requestedDeliveryDate": null
    }
  ],
  "shipments": []
}
```

---

## Create PurchaseOrder (POST -- target use only)

```bash
curl -s -X POST \
  "https://s05.extend.se/RESTAPI/v1_0/{client}/PurchaseOrders" \
  -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "header": {
      "warehouse": "MAXGAMING2",
      "supplier": {
        "supplierNumber": "4343128"
      },
      "reference": "Optiply-12345",
      "requestedDeliveryDate": "2026-04-01T00:00:00",
      "notes": "Created by Optiply"
    },
    "rows": [
      {
        "position": 10,
        "productNumber": "37131",
        "purchaseDataPurchaseUnit": {
          "quantity": 24.0,
          "unit": "ST"
        },
        "purchaseDataProductUnit": {
          "quantity": 24.0,
          "unit": "ST",
          "unitPrice": 7.86,
          "currency": "SEK"
        },
        "expectedDeliveryDate": "2026-04-01T00:00:00"
      }
    ]
  }'
```

---

## Pagination Example

Page through all POs until empty:

```bash
PAGE=1
while true; do
  RESPONSE=$(curl -s -X GET \
    "https://s05.extend.se/RESTAPI/v1_0/{client}/PurchaseOrders?pageNumber=$PAGE" \
    -H "ExtendBasicAuthorization: Basic {base64_credentials}" \
    -H "Accept: application/json")

  COUNT=$(echo "$RESPONSE" | jq '.purchaseOrderList | length')
  echo "Page $PAGE: $COUNT records"

  if [ "$COUNT" -eq 0 ]; then
    break
  fi
  PAGE=$((PAGE + 1))
done
```

---

## Query Parameters Reference

### Products

| Parameter | Type | Description |
|---|---|---|
| `pageNumber` | int | Page number (starts at 1) |
| `warehouse` | string | Filter by warehouse code |

### CustomerOrders

| Parameter | Type | Description |
|---|---|---|
| `pageNumber` | int | Page number (starts at 1) |
| `createDateFrom` | datetime | **Required** -- filter orders created after this date (endpoint times out without it) |
| `createDateTo` | datetime | Filter orders created before this date |

### PurchaseOrders

| Parameter | Type | Description |
|---|---|---|
| `pageNumber` | int | Page number (starts at 1) |
| `IsOpen` | bool | Filter by open status |
| `IsReceived` | bool | Filter by received status |
| `warehouse` | string | Filter by warehouse code |
| `createDateFrom` | datetime | Filter POs created after this date |
| `createDateTo` | datetime | Filter POs created before this date |
| `supplierNumber` | string | Filter by supplier number |
| `purchaseNumber` | string | Filter by specific PO number |
