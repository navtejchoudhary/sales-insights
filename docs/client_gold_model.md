# Client (SAP data) gold data dictionary

Generated from `src/sales_insights/pipeline/client_model.py`: do not edit by hand.
Refresh: `uv run python -m sales_insights.pipeline.client_model > docs/client_gold_model.md`

## gold.fact_sales

Sales transactions from the client's SAP billing data: one row per invoice line (invoice_number + invoice_item), whole months only. Includes invoices, free-of-charge deliveries, returns, cancellations, price corrections and debit memos, so summing net_revenue_amount over any period gives true net revenue. Money is in Sri Lankan rupees (LKR), excluding tax.

| Column | Description |
|---|---|
| `invoice_number` | SAP billing document number, 10 digits. The number does not show the document type: use document_type. |
| `invoice_item` | Line number within the invoice (10, 20, 30 ...). |
| `invoice_type` | SAP billing type code: ZAOR invoice, ZFOC free of charge, ZARE return, S1 cancellation, ZACR price correction, ZADR debit memo, S2 credit memo cancellation; the client's own invoice types (e.g. ZCF2, ZAEP, ZFV1, ZFV2) are invoices too (SAP document category M). |
| `document_type` | Plain-English billing type: Invoice, Free of charge, Return, Cancellation, Price correction, Debit memo or Credit memo cancellation. The client's own invoice types show as Other (<type>), e.g. Other (ZCF2), and count as invoiced revenue. |
| `invoice_date` | Date of the billing document (Sri Lanka time). Use this date for all sales reporting. |
| `fiscal_year` | Fiscal year April-March, labelled by its start year (April 2026 to March 2027 = 2026). |
| `fiscal_period` | Fiscal month 1-12; 1 = April, 12 = March. |
| `customer_id` | Sold-to customer, 10 digits with leading zeros. Join to dim_customer. |
| `product_id` | SAP material (product) code. Join to dim_product. |
| `district_code` | SAP sales district of the line. Join to dim_region (names and provinces are 'Unassigned' until the business provides them). |
| `sales_office_code` | SAP sales office that handled the line. |
| `distribution_channel_code` | SAP distribution channel code. Join to dim_channel for the name. |
| `plant_code` | SAP plant (warehouse) that shipped the goods. |
| `company_code` | SAP company code. This data holds one company: 2010. |
| `sales_rep_id` | Sales representative: always empty (not in the SAP billing data). |
| `quantity` | Net quantity in the line's sales unit (units differ by product). Positive for invoices, negative for returns and cancellations, 0 for price corrections. |
| `sold_quantity` | Quantity invoiced (invoice-type documents only), in the line's sales unit; 0 on other rows. |
| `returned_quantity` | Quantity returned (returns only), negative, in the line's sales unit; 0 on other rows. |
| `net_revenue_amount` | Net revenue in LKR excluding VAT. The headline revenue measure: sum it to get revenue. Returns, cancellations and price corrections are negative. |
| `invoiced_revenue_amount` | Revenue from every invoice-type document (SAP category M: invoices, free of charge and the client's own invoice types) before returns, cancellations and corrections, LKR. |
| `returns_amount` | Revenue reversed by return credit memos, LKR, negative; 0 on other rows. |
| `cancellations_amount` | Revenue reversed by invoice cancellations, LKR, negative; 0 on other rows. |
| `price_corrections_amount` | Credits given for pricing errors, LKR, negative; 0 on other rows. |
| `gross_revenue_amount` | Revenue including tax, LKR: net revenue + tax_amount (SAP's own gross field is empty in this data). |
| `tax_amount` | Tax on the line as recorded in SAP, LKR. Mostly VAT (18% on domestic sales; exports are zero-rated), but it may include other taxes or levies. |
| `cost_amount` | SAP's cost on the billing line, LKR. Same sign as quantity. |
| `margin_amount` | Net revenue minus cost_amount, LKR. Gross margin % = sum(margin_amount) / sum(net_revenue_amount). |
| `is_free_of_charge` | True for free-of-charge deliveries (ZFOC): goods given away at no charge. |
| `is_cancelled` | True if this invoice line was later cancelled. Its revenue is already reversed by a Cancellation row, so do not filter it out of revenue totals. |
| `cancelled_by_invoice_number` | The cancellation document that reversed this line, if any. |
| `reference_invoice_number` | For returns, cancellations and price corrections: the original invoice they relate to. |
| `source_kind` | Which feed the row came from: always 'history' (one-time copy of the SAP billing data). |
| `loaded_business_date` | Empty: the data comes from a one-time copy, not from daily deliveries. |
| `arrival_delay_days` | Empty: the data comes from a one-time copy, not from daily deliveries. |

## gold.dim_customer

Customers (sold-to): one row per customer, latest version. Names are personal data and are not copied: each customer is called 'Customer <customer_id>'.

| Column | Description |
|---|---|
| `customer_id` | Customer number, 10 digits with leading zeros. |
| `customer_name` | Placeholder name 'Customer <customer_id>' (names are not copied); 'Unknown customer <customer_id>' when billed but missing from the master data. |
| `customer_full_name` | Same as customer_name (full names are not copied). |
| `customer_group_code` | SAP customer group code. |
| `customer_group_name` | Customer group name from SAP; 'Unassigned' when the source left it blank. |
| `city` | City in capitals from the customer master data; 'Unassigned' when blank. |
| `district_code` | SAP sales district or region of the customer. |
| `district_name` | District name: 'Unassigned' until the business provides the district names. |
| `province` | Province: 'Unassigned' until the business provides the district to province list. |
| `sales_office_code` | Sales office that serves the customer. Empty: the copied SAP master data has no such field. |
| `sales_office_name` | Sales office name. Empty: the copied SAP master data has no such field. |
| `distribution_channel_code` | Usual distribution channel of the customer. Empty: the copied SAP master data has no such field. |
| `payer_customer_id` | Customer who pays the invoices. Empty: the copied SAP master data has no such field. |
| `created_date` | Date the customer was created in the source system. |
| `city_is_canonical` | True if the city spelling matched the official city list. |

## gold.dim_product

Products (SAP materials): one row per material, with material group, type and division from SAP's texts.

| Column | Description |
|---|---|
| `product_id` | SAP material code. |
| `product_description` | Product name in capitals; the material code when the master data has no name; 'UNKNOWN PRODUCT <code>' when billed but missing from the master data. |
| `product_group_code` | SAP material group code. |
| `product_group_name` | Material group name from SAP; 'Unassigned' when unknown. |
| `product_type_code` | SAP material type code. |
| `product_type_name` | Material type name from SAP; 'Unassigned' when unknown. |
| `division_code` | SAP division code (may be empty). |
| `division_name` | Division name from SAP; 'Unassigned' when unknown. |
| `company_code` | Company code that bills the product (from its billing lines). |
| `plant_code` | Default plant. Empty: the copied SAP master data has no such field. |
| `base_unit` | SAP base unit of measure. |
| `list_price_amount` | List price: always empty (prices are not in the copied data). |
| `standard_cost_amount` | Standard cost: always empty (the cost comes from each billing line: fact_sales.cost_amount). |
| `launch_date` | Date the material was created. Empty: the copied SAP master data has no such field. |

## gold.dim_region

Geography: one row per SAP sales district. Names and provinces are 'Unassigned' until the business provides them.

| Column | Description |
|---|---|
| `district_code` | SAP sales district code. |
| `district_name` | District name: 'Unassigned' until the business provides it. |
| `province` | Province: 'Unassigned' until the business provides the district to province list. |
| `country` | Country code (LK). |

## gold.dim_channel

Distribution channels from SAP: how goods reach the customer.

| Column | Description |
|---|---|
| `distribution_channel_code` | SAP distribution channel code. |
| `distribution_channel_name` | Channel name from SAP's texts. |

## gold.dim_date

Calendar: one row per day, with fiscal year (April-March), Sri Lankan cultivation season and True/False flags for the periods people ask about (last month, month to date, year to date ...), all relative to the latest data date.

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
| `is_latest_day` | True on the latest invoice date in the data (the day the figures run up to). |
| `is_month_to_date` | True from the 1st of the latest data month up to the latest data date: 'this month', 'month to date', 'MTD'. Usually an incomplete month. |
| `is_same_days_last_month` | True on the same days of the previous month as month to date (like-for-like comparison for this month so far). |
| `is_same_days_last_year` | True on the same days one year earlier as month to date (like-for-like comparison with last year). |
| `is_last_7_days` | True on the 7 days ending on the latest data date: 'last 7 days', 'past week'. |
| `is_previous_7_days` | True on the 7 days before the last 7 days (comparison week). |
| `is_last_complete_month` | True in the latest complete calendar month: what 'last month' means (never the current, incomplete month). |
| `is_month_before_last_complete_month` | True in the month before the last complete month (month-on-month comparison). |
| `is_last_complete_month_last_year` | True in the same calendar month one year before the last complete month (year-on-year comparison). |
| `is_fiscal_year_to_date` | True from 1 April of the current fiscal year up to the latest data date: 'year to date', 'YTD', 'this year so far'. |
| `is_same_period_last_fiscal_year` | True on the same stretch of the previous fiscal year as fiscal year to date (like-for-like YTD comparison). |
| `is_current_fiscal_year` | True in the current fiscal year (April to March) that contains the latest data date: 'this year', 'this fiscal year'. |
| `is_previous_fiscal_year` | True in the fiscal year before the current one: 'last year', 'last fiscal year'. |
