"""The planted stories, in one place.

Every generator reads its story settings from here, so the story sheet
(docs/story_sheet.md), the data and the Genie answer key never disagree.
Change a story here, re-run the generators, and everything follows.

Business (modelled on the reference SAP O2C extract): a Sri Lankan company selling
crop-protection chemicals (company code 2010) plus building-solution systems and
services (company code 4010). Currency LKR. Fiscal year April-March.
"""

from __future__ import annotations

from datetime import date

# Story 1: agricultural seasonality. Demand peaks before the two cultivation seasons:
# Maha (Sep-Nov) and Yala (Mar-May). Built into the history generator.
MAHA_PEAK_MONTHS = (9, 10, 11)
YALA_PEAK_MONTHS = (3, 4, 5)

# Story 2: a few products bring most revenue (Zipf-style popularity)
POPULARITY_SKEW = 1.1

# Story 3: one province steadily underperforms (demand multiplier vs others)
WEAK_PROVINCE = "Southern"
WEAK_PROVINCE_FACTOR = 0.85

# Story 4: price increase on one product group -> price per case up, units lag the other crop chemicals
PRICE_RISE_PRODUCT_GROUP = "2CHE06"  # WEEDICIDE
PRICE_RISE_PCT = 8.0
PRICE_RISE_FROM = date(2026, 4, 1)

# Story 5: a new product launches mid-year and ramps up slowly
NEW_PRODUCT_ID = "HEX1LTR"
NEW_PRODUCT_LAUNCH = date(2026, 6, 15)

# Story 6: returns spike on one product (leaking bottles)
RETURNS_SPIKE_PRODUCT_ID = "HGL400ML"
RETURNS_SPIKE_FROM = date(2026, 7, 1)

# Story 7 (live, demo week): the top-selling product goes out of stock in one province
HERO_PRODUCT_ID = "MC6001LT"
STOCKOUT_PROVINCE = "Western"
STOCKOUT_FROM = date(2026, 10, 23)


# ---------------------------------------------------------------------------
# Inventory area stories (generator/inventory.py; plan: docs/multiple_gold_layers.md section 4.4)
# Stock movements follow the sales above, so these stories explain the sales stories.
# ---------------------------------------------------------------------------

# I1: the depots of STOCKOUT_PROVINCE run out of the hero product exactly when story 7 starts,
# while the central hub (plant 2010, Galle) still holds stock. Their last transfer is sized to last
# until the day before STOCKOUT_FROM; the next transfer is held back.
STOCKOUT_LAST_TRANSFER_DAYS = 30  # the last transfer is shipped this many days before STOCKOUT_FROM

# I2: the substitute pack (engine.STOCKOUT_SUBSTITUTE_ID) leaves the same depots faster from STOCKOUT_FROM.
# No extra setting: it follows the sales story.

# I3: one slow building-solutions spare kit is over-bought: a single large receipt at the Kandy plant
OVERSTOCK_PRODUCT_ID = "S-10000576"
OVERSTOCK_RECEIPT_DATE = date(2026, 8, 3)
OVERSTOCK_DAYS_OF_COVER = 200

# I4: returned goods pile up in the returns store; the returns-spike product (story 6) grows fastest.
# No extra setting: it follows the sales story.


# ---------------------------------------------------------------------------
# Delivery and schema dirt in the daily feed (tests the pipeline, not the business)
# ---------------------------------------------------------------------------

# A new column appears in the daily files from this date (schema evolution)
SCHEMA_CHANGE_FROM = date(2026, 10, 22)
NEW_COLUMN = "sales_rep_id"

# The feed misses one day; that day's folder arrives together with the next day's
MISSING_DELIVERY_DAY = date(2026, 10, 18)

# Every Monday, Sunday's orders file is delivered a second time (same file name)
REDELIVERY_WEEKDAY = 0  # Monday
