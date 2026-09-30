# Story sheet (DRAFT — agree with Anil on Fri 2 Oct)

Stories are planted in the generated data so we know the right answers in advance. The answer key (`genie/answer_key.csv`) is computed from these.

## Master data sizes (proposals)

| Master | Size |
|---|---|
| Products | ~120 across 6 categories |
| Customers | ~2,000 |
| Regions | 5 (North, South, East, West, Central), ~25 cities |
| Channels | 3 (Direct, Dealer, Online) |
| Sales reps | ~40 |

History: 18 months of order lines ending **yesterday**. Daily drops continue from today.

## Planted stories

| # | Story | Where it shows | Why it's there |
|---|---|---|---|
| 1 | Festive-season build-up in Oct–Nov; weekends busier than weekdays | History + live days | Seasonality questions |
| 2 | A few products bring most revenue (80/20) | History | "Top products" questions |
| 3 | One region steadily underperforms | History | Region comparison, "why" questions |
| 4 | Price increase on one category: revenue up, units down | History | Price vs volume drivers |
| 5 | A new product ramps up slowly after launch | History | New-product questions |
| 6 | Returns spike on one product | History | Returns questions; Phase 2 insight |
| 7 | **Live:** a top product goes out of stock in one region from 23 Oct | Live days | Revenue-drop alert, insights job and Genie all catch it in demo week |

## Dirt injected on purpose (starting rates; logged in every manifest)

| Dirt | Starting rate | Tests |
|---|---|---|
| Duplicate rows | 1–2% of rows | Dedupe |
| Late orders dated 1–7 days back | ~3% | Rebuilding past dates in gold |
| Status changes to old orders | every day | Silver MERGE |
| Inconsistent text (Bengaluru / Bangalore / BLR, mixed case) | ~5% | Standardisation map |
| Nulls in non-key fields | ~2% | Quality rules |
| Invalid values (bad date, negative quantity without return flag) | <0.5% | Quarantine |
| Same file delivered twice | once a week | Exactly-once ingestion |
| Missing day, delivered next day | once | Catch-up |
| New column appears | once, ~22 Oct | Schema evolution |

## Open decisions

- [ ] Anil approves the story list
- [ ] Which region underperforms, which category gets the price increase, which product stocks out
- [ ] Final master data sizes
