# Story sheet (v2 — for sponsor review)

Stories are planted in the generated data so we know the right answers in advance. The answer key (`genie/answer_key.csv`) is computed from these.

**Single source of truth: `src/sales_insights/generator/stories.py`.** Change a story there, re-run the generators, and the data and answer key follow.

## The business

Modelled on the company's reference SAP order-to-cash extract: a Sri Lankan company selling crop-protection chemicals (company code 2010: weedicides, insecticides, fungicides, sprayers, seedling trays) plus building-solution systems and services (company code 4010). Currency LKR. Fiscal year April–March, labelled by its start year. Customer, rep and product names are fictional; code lists follow the reference.

## Master data (`generator/masters.py`)

| Master | Size | Notes |
|---|---|---|
| Regions | 25 districts → 9 provinces | SAP region code = district, as in the reference |
| Cities | 84 canonical names | Used by silver to fix spelling variants |
| Customers | 600 (+12 duplicate rows as dirt) | 6 customer groups; General Trade, Export Sales, Modern Trade (supermarket chains share a payer) |
| Products | 40 | Reference material IDs and groups, plus 8 new products launched inside the history window |
| Sales offices / reps | 11 offices, 36 reps | One office per province, two in Western and Southern |
| Lookups | companies, plants, storage locations, channels, divisions, customer groups, product groups, product types | |

History: 18 months of invoice lines ending **yesterday**. Daily drops continue from today.

## Planted stories

| # | Story | Setting | Where it shows |
|---|---|---|---|
| 1 | Demand peaks before the **Maha** (Sep–Nov) and **Yala** (Mar–May) cultivation seasons | Seasonal curve in history generator | History + live days |
| 2 | A few products bring most revenue (80/20) | `POPULARITY_SKEW = 1.1` | History |
| 3 | **Southern** province steadily underperforms | Demand × 0.85 | History |
| 4 | **Weedicide** (`2CHE06`) price rise: revenue up, units down | +8% from 1 Apr 2026 (start of FY2026) | History |
| 5 | New product **HEX FUNGICIDE 1LTR** (`HEX1LTR`) ramps up slowly | Launch 15 Jun 2026 | History |
| 6 | Returns spike on **HGL INSECTICIDE 400ML** (`HGL400ML`): leaking bottles | From 1 Jul 2026 | History (return credit memos) |
| 7 | **Live:** top seller **MC600 WEEDICIDE 1LTR** (`MC6001LT`) out of stock in **Western** province | From 23 Oct 2026 | Live days: alert, insights job and Genie catch it in demo week |

## Dirt injected on purpose (every row logged in the manifest)

### In master files (built now)

| Dirt | Rate | Silver must |
|---|---|---|
| City spelling variants (`COLOMBO - 10`, `Colombo 10`, `ANURADHAPURA.`, `colombo 09`) — same styles as the reference | 10% of customers | Map to the canonical name in `cities.csv` |
| Blank region code | 3% | Derive from the city |
| Blank customer group | 15% | Set to "Unassigned" |
| Trailing spaces in customer name | 2% | Trim |
| Same customer twice (older version, older timestamp) | 2% | Keep the latest `last_updated_timestamp` |
| Customer ID without leading zeros (`1001591` vs `0001001591`) | 1% | Left-pad to 10 digits |
| Blank product group | 5% of products | Fill from product family or "Unassigned" |
| Mixed-case product description | 8% of products | Upper-case |

### In daily invoice files (next steps)

| Dirt | Starting rate |
|---|---|
| Duplicate invoice lines | 1–2% |
| Late invoices dated 1–7 days back | ~3% |
| Cancellations (`S1`) and return credit memos (`ZARE`) against older invoices | every day |
| Blank product / plant / sales office codes (as in the reference) | ~1% |
| Zero-priced lines (free of charge, `ZFOC`) | <1% |
| Invalid values (bad date, negative quantity on a normal invoice) | <0.5% → quarantine |
| Same file delivered twice | once a week |
| Missing day, delivered next day | once |
| New column appears | once, ~22 Oct |

## Open decisions

- [ ] Sponsor approves the business setting and stories
- [ ] Confirm attach-rate definition (see `kpi_definitions.md`)
