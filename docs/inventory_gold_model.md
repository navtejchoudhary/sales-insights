# Inventory gold data dictionary

Generated from `src/sales_insights/pipeline/inventory_model.py`: do not edit by hand.
Refresh: `uv run python -m sales_insights.pipeline.inventory_model > docs/inventory_gold_model.md`

## gold.fact_stock_daily

Closing stock: one row per location, product and day (snapshot_date + location_id + product_id). Stock is a level, not a flow: never add stock_quantity across days; for a period use the last day of the period (e.g. stock at month end). Quantities are in the product's base unit (cases, bottles, bags).

| Column | Description |
|---|---|
| `snapshot_date` | Day the stock was counted, at the end of the day (Sri Lanka time). Join to dim_date.date. |
| `location_id` | Where the stock is held, plant-storage location, e.g. 2010-8001 (Colombo depot). Join to dim_location. |
| `product_id` | Material (product) code, e.g. MC6001LT. Join to dim_product in the sales catalog. |
| `stock_quantity` | Unrestricted stock at the end of the day, in the product's base unit. Never negative. |
| `in_transit_quantity` | Stock on its way to this location from the hub (shipped, not yet arrived), base unit. |
| `stock_value_amount` | Stock at standard cost: stock_quantity x product standard cost, LKR. |
| `issued_quantity` | Quantity issued to customers from this location on that day (goods issues, movement 601), base unit. |
| `avg_daily_issued_quantity_28d` | Average quantity issued per day over the 28 days ending on snapshot_date, base unit. |
| `days_of_cover` | How many days the stock lasts at the 28-day average issue rate: stock_quantity / avg_daily_issued_quantity_28d. Empty when nothing was issued in 28 days. |
| `is_stock_out` | True when the location holds no stock of a product it normally issues (stock_quantity = 0 and issues in the last 28 days). |
| `is_low_cover` | True when stock lasts less than 7 days at the 28-day average issue rate (and is not zero). |

## gold.fact_stock_movements

Stock movements: one row per material document line (material_document + material_document_item): receipts, transfers between locations, goods issues to customers, returns, cancellations and stock count differences. quantity is signed: positive into the location, negative out of it.

| Column | Description |
|---|---|
| `material_document` | SAP material document number, 10 digits, starting with 4. |
| `material_document_item` | Line number within the material document (0001, 0002 ...). |
| `posting_date` | Date the movement was posted (Sri Lanka time). Join to dim_date.date. |
| `movement_type` | SAP movement type code: 561 opening balance, 101 goods receipt, 313 transfer out (in transit), 315 transfer in, 601 goods issue for a sale, 602 reversal of a goods issue (cancelled invoice), 653 customer return, 701 / 702 stock count gain / loss. |
| `movement_type_name` | Plain-English movement type, e.g. Goods issue, Goods receipt, Transfer in. |
| `location_id` | Location whose stock changes, e.g. 2010-0090 (Galle hub). Join to dim_location. |
| `product_id` | Material (product) code, e.g. MC6001LT. Join to dim_product in the sales catalog. |
| `quantity` | Signed quantity in the product's base unit: positive into the location, negative out of it. |
| `issued_quantity` | Quantity issued to customers (movement 601) as a positive number; 0 on other rows. |
| `received_quantity` | Quantity received from outside (movement 101) as a positive number; 0 on other rows. |
| `sales_office_code` | For sales-related movements (601, 602, 653): the customer's sales office, e.g. C001. |
| `reference_invoice_number` | For sales-related movements: the billing document (invoice, return or cancellation) in fact_sales. |
| `reference_invoice_item` | For sales-related movements: the line of that billing document. |
| `counterpart_location_id` | For transfers (313 / 315): the other location (the hub for a depot, the depot for the hub). |
| `delivered_date` | Day the source delivered the movement (usually the posting date; up to 2 days later for late lines). Empty for the history backfill. |

## gold.dim_location

Stock locations: one row per plant storage location or depot, with its sales office and province.

| Column | Description |
|---|---|
| `location_id` | Location code: plant-storage location, e.g. 2010-8001. |
| `location_name` | Location name, e.g. Colombo Depot, Galle Industrial - Distribution Hub. |
| `location_type` | Depot (serves one sales office) or Plant store (hub, raw materials, returns, building solutions). |
| `company_code` | Company code that owns the stock: 2010 crop protection, 4010 building solutions. |
| `plant_code` | SAP plant, e.g. 2010 Galle, 4020 Kandy. |
| `storage_location_code` | SAP storage location within the plant, e.g. 0090 (hub), 8001 (Colombo depot). |
| `sales_office_code` | For depots: the sales office whose customers it supplies, e.g. C001. Empty for plant stores. |
| `district_code` | District (SAP region) of the location, 2 digits, e.g. 05. Join to dim_region. |
| `province` | Province of the location, e.g. Western. |
| `supplying_location_id` | For depots: the location that replenishes it (the Galle hub 2010-0090). |
