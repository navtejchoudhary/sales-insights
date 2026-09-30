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

# Story 4: price increase on one product group -> revenue up, units down
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
# Delivery and schema dirt in the daily feed (tests the pipeline, not the business)
# ---------------------------------------------------------------------------

# A new column appears in the daily files from this date (schema evolution)
SCHEMA_CHANGE_FROM = date(2026, 10, 22)
NEW_COLUMN = "sales_rep_id"

# The feed misses one day; that day's folder arrives together with the next day's
MISSING_DELIVERY_DAY = date(2026, 10, 18)

# Every Monday, Sunday's orders file is delivered a second time (same file name)
REDELIVERY_WEEKDAY = 0  # Monday
