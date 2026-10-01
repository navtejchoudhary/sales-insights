# Gold data dictionary

Generated from `src/sales_insights/pipeline/gold_model.py`: do not edit by hand.
Refresh: `uv run python -m sales_insights.pipeline.gold_model > docs/gold_model.md`

## gold.fact_sales

Sales transactions: one row per invoice line (invoice_number + invoice_item). Includes normal invoices, free-of-charge deliveries, returns, cancellations and price corrections, so summing net_revenue_amount over any period gives true net revenue. Money is in Sri Lankan rupees (LKR), excluding VAT.

| Column | Description |
|---|---|
| `invoice_number` | SAP billing document number, 10 digits. Returns start with 7, cancellations 8, price corrections 6, invoices 9. |
| `invoice_item` | Line number within the invoice (10, 20, 30 ...). |
| `invoice_type` | SAP billing type code: ZAOR invoice, ZFOC free of charge, ZARE return, S1 cancellation, ZACR price correction. |
| `document_type` | Plain-English billing type: Invoice, Free of charge, Return, Cancellation or Price correction. |
| `invoice_date` | Date of the billing document (Sri Lanka time). Use this date for all sales reporting. |
| `fiscal_year` | Fiscal year April-March, labelled by its start year (April 2026 to March 2027 = 2026). |
| `fiscal_period` | Fiscal month 1-12; 1 = April, 12 = March. |
| `customer_id` | Sold-to customer, 10 digits with leading zeros. Join to dim_customer. |
| `product_id` | Material (product) code, e.g. MC6001LT. Join to dim_product. |
| `district_code` | District (SAP region) of the customer, 2 digits, e.g. 05 = Colombo. Join to dim_region. |
| `sales_office_code` | Sales office that handles the customer, e.g. C001. |
| `distribution_channel_code` | Distribution channel code: 10 General Trade, 20 Export Sales, 30 Modern Trade. Join to dim_channel. |
| `plant_code` | Plant (warehouse) that shipped the goods, e.g. 2010. |
| `company_code` | Company code: 2010 Enterprise Trading Company (crop protection), 4010 Global Consumer Products Ltd (building solutions). |
| `sales_rep_id` | Sales representative on the line. Only filled from 22 Oct 2026, when the source started sending it. |
| `quantity` | Net quantity in sales units (cases). Positive for invoices, negative for returns and cancellations, 0 for price corrections. |
| `sold_quantity` | Quantity invoiced (invoices and free-of-charge lines only), in cases; 0 on other rows. |
| `returned_quantity` | Quantity returned (return credit memos only), negative, in cases; 0 on other rows. |
| `net_revenue_amount` | Net revenue in LKR excluding VAT. The headline revenue measure: sum it to get revenue. Returns, cancellations and price corrections are negative. |
| `invoiced_revenue_amount` | Revenue from invoices and free-of-charge lines only (before returns, cancellations and corrections), LKR. |
| `returns_amount` | Revenue reversed by return credit memos, LKR, negative; 0 on other rows. |
| `cancellations_amount` | Revenue reversed by invoice cancellations, LKR, negative; 0 on other rows. |
| `price_corrections_amount` | Credits given for pricing errors, LKR, negative; 0 on other rows. |
| `gross_revenue_amount` | Revenue including VAT, LKR (net revenue + tax). |
| `tax_amount` | VAT on the line, LKR (18% on domestic sales; exports are zero-rated). |
| `cost_amount` | Standard cost of the quantity on the line (quantity x product standard cost), LKR. Same sign as quantity. |
| `margin_amount` | Net revenue minus standard cost, LKR. Gross margin % = sum(margin_amount) / sum(net_revenue_amount). |
| `is_free_of_charge` | True for free-of-charge deliveries (ZFOC): goods given away; revenue is minus the VAT the company bears. |
| `is_cancelled` | True if this invoice line was later cancelled. Its revenue is already reversed by a Cancellation row, so do not filter it out of revenue totals. |
| `cancelled_by_invoice_number` | The cancellation document that reversed this line, if any. |
| `reference_invoice_number` | For returns, cancellations and price corrections: the original invoice they relate to. |
| `source_kind` | Which feed the row came from: history (initial 18-month backfill), orders or changes (daily files). |
| `loaded_business_date` | Business date of the daily delivery that brought the row; empty for history. |
| `arrival_delay_days` | Days between invoice_date and the delivery that brought it (0 = same day; >0 = arrived late). Empty for history. |

## gold.dim_customer

Customers: one row per customer (latest version), with cleaned names and canonical city, district and province.

| Column | Description |
|---|---|
| `customer_id` | Customer number, 10 digits with leading zeros. |
| `customer_name` | Short customer name. |
| `customer_full_name` | Full legal name. |
| `customer_group_code` | Customer group code, e.g. 03. |
| `customer_group_name` | Customer group, e.g. Domestic, Distributors, Government, Plantations. 'Unassigned' when the source left it blank. |
| `city` | Canonical city name in capitals, e.g. COLOMBO 10. |
| `district_code` | District (SAP region) code, 2 digits. |
| `district_name` | District name, e.g. Colombo. |
| `province` | Province, e.g. Western, Southern. |
| `sales_office_code` | Sales office that serves the customer. |
| `sales_office_name` | Sales office name. |
| `distribution_channel_code` | Usual distribution channel: 10 General Trade, 20 Export Sales, 30 Modern Trade. |
| `payer_customer_id` | Customer who pays the invoices (supermarket branches share one payer). |
| `created_date` | Date the customer was created in the source system. |
| `city_is_canonical` | True if the city spelling matched the official city list. |

## gold.dim_product

Products (materials): one row per product with group, type, division, list price and standard cost.

| Column | Description |
|---|---|
| `product_id` | Material code, e.g. MC6001LT. |
| `product_description` | Product name in capitals, e.g. MC600 WEEDICIDE 1LTR (CASE). |
| `product_group_code` | Product group code, e.g. 2CHE06. |
| `product_group_name` | Product group, e.g. WEEDICIDE, INSECTICIDE, FUNGICIDE. |
| `product_type_code` | Material type code, e.g. ZFRT. |
| `product_type_name` | Material type, e.g. Finished Material, Trading Material. |
| `division_code` | Division code, e.g. 04. |
| `division_name` | Division, e.g. Agriculture Business, Building Solutions. |
| `company_code` | Company code that sells the product. |
| `plant_code` | Default plant (warehouse). |
| `base_unit` | Unit of measure, e.g. CA (case). |
| `list_price_amount` | Current list price per sales unit, LKR, excluding VAT. |
| `standard_cost_amount` | Standard cost per sales unit, LKR. |
| `launch_date` | Date the product was launched. |

## gold.dim_region

Geography: one row per district (SAP region), with its province.

| Column | Description |
|---|---|
| `district_code` | District (SAP region) code, 2 digits, e.g. 05. |
| `district_name` | District name, e.g. Colombo. |
| `province` | Province, e.g. Western. Sri Lanka has 9 provinces and 25 districts. |
| `country` | Country code (LK). |

## gold.dim_channel

Distribution channels: how goods reach the customer.

| Column | Description |
|---|---|
| `distribution_channel_code` | Channel code: 10, 20 or 30. |
| `distribution_channel_name` | Channel name: General Trade, Export Sales or Modern Trade. |

## gold.dim_date

Calendar: one row per day, with fiscal year (April-March) and Sri Lankan cultivation season.

| Column | Description |
|---|---|
| `date` | Calendar date. Join to fact_sales.invoice_date. |
| `year` | Calendar year. |
| `quarter` | Calendar quarter 1-4. |
| `month` | Calendar month 1-12. |
| `month_name` | Month name, e.g. October. |
| `month_start_date` | First day of the month (use to group by month). |
| `week_start_date` | Monday of the week (use to group by week). |
| `day_of_week` | Day of week, 1 = Monday ... 7 = Sunday. |
| `day_name` | Day name, e.g. Monday. |
| `is_weekend` | True on Saturday and Sunday. |
| `fiscal_year` | Fiscal year April-March, labelled by its start year (April 2026 to March 2027 = 2026). |
| `fiscal_year_label` | Fiscal year as text, e.g. FY2026. |
| `fiscal_period` | Fiscal month 1-12; 1 = April. |
| `fiscal_quarter` | Fiscal quarter 1-4; Q1 = April-June. |
| `cultivation_season` | Sri Lankan cultivation season: Maha (Sep-Mar), Yala (May-Aug) or Inter-season (April). |
