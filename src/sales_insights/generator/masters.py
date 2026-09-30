"""Master data generator, modelled on the reference SAP order-to-cash extract.

Column names and code values follow the reference file (SAP-style: 10-digit customer
numbers, district region codes, sales offices, divisions, material types ZFRT/ZTRD/...),
so real data can replace dummy data later with minimal mapping.

Output (like an outside source system delivers it):

    staging/masters/<table>.csv          one file per master table (see TABLES)
    staging/masters/masters_manifest.json  written LAST: row counts, SHA-256, dirt log

Dirt is injected on purpose (city spellings, blank codes, IDs without leading zeros,
duplicate records with a newer timestamp...) and every injected row is listed in the
manifest, so silver-layer tests can prove each one gets fixed.

Same seed + same as-of date = byte-identical files.

Run:
    uv run python -m sales_insights.generator.masters            # as-of = simulation_start in config
    uv run python -m sales_insights.generator.masters --no-dirt
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from sales_insights.generator import stories

COUNTRY = "LK"
CURRENCY = "LKR"
HISTORY_DAYS = 548  # ~18 months

# ---------------------------------------------------------------------------
# Reference code lists (taken from the reference extract; additions are marked)
# ---------------------------------------------------------------------------

# SAP region = Sri Lankan district (alphabetical code, as in the reference file)
DISTRICTS: list[tuple[str, str, str, float]] = [
    # (region code, district, province, share of customers)
    ("01", "Ampara", "Eastern", 0.03),
    ("02", "Anuradhapura", "North Central", 0.04),
    ("03", "Badulla", "Uva", 0.03),
    ("04", "Batticaloa", "Eastern", 0.02),
    ("05", "Colombo", "Western", 0.15),
    ("06", "Galle", "Southern", 0.05),
    ("07", "Gampaha", "Western", 0.10),
    ("08", "Hambantota", "Southern", 0.03),
    ("09", "Jaffna", "Northern", 0.04),
    ("10", "Kalutara", "Western", 0.06),
    ("11", "Kandy", "Central", 0.07),
    ("12", "Kegalle", "Sabaragamuwa", 0.03),
    ("13", "Kilinochchi", "Northern", 0.01),
    ("14", "Kurunegala", "North Western", 0.06),
    ("15", "Mannar", "Northern", 0.01),
    ("16", "Matale", "Central", 0.03),
    ("17", "Matara", "Southern", 0.04),
    ("18", "Monaragala", "Uva", 0.02),
    ("19", "Mullaitivu", "Northern", 0.005),
    ("20", "Nuwara Eliya", "Central", 0.03),
    ("21", "Polonnaruwa", "North Central", 0.03),
    ("22", "Puttalam", "North Western", 0.04),
    ("23", "Ratnapura", "Sabaragamuwa", 0.04),
    ("24", "Trincomalee", "Eastern", 0.02),
    ("25", "Vavuniya", "Northern", 0.015),
]

# Canonical (clean) city names per district, upper case as in the reference
CITIES: dict[str, list[str]] = {
    "01": ["AMPARA", "DEHIATTAKANDIYA", "KALMUNAI"],
    "02": ["ANURADHAPURA", "KEKIRAWA", "MEDAWACHCHIYA"],
    "03": ["BADULLA", "WELIMADA", "BANDARAWELA"],
    "04": ["BATTICALOA", "ERAVUR"],
    "05": [
        "COLOMBO 01",
        "COLOMBO 02",
        "COLOMBO 03",
        "COLOMBO 05",
        "COLOMBO 07",
        "COLOMBO 10",
        "COLOMBO 15",
        "BATTARAMULLA",
        "DEHIWALA",
        "MAHARAGAMA",
        "NUGEGODA",
        "KOTTE",
        "RAJAGIRIYA",
        "RATMALANA",
        "AVISSAWELLA",
        "HANWELLA",
        "PADUKKA",
    ],
    "06": ["GALLE", "AMBALANGODA", "BATAPOLA"],
    "07": ["WATTALA", "KELANIYA", "KADAWATHA", "MIRIGAMA", "NITTAMBUWA", "KIRINDIWELA", "NEGOMBO"],
    "08": ["AMBALANTOTA", "TISSAMAHARAMA", "HAMBANTOTA"],
    "09": ["JAFFNA", "KARAVEDDI", "CHAVAKACHCHERI"],
    "10": ["KALUTARA", "HORANA", "MATUGAMA", "ALUTHGAMA", "AGALAWATTA", "BULATHSINGHALA"],
    "11": ["KANDY", "PERADENIYA", "PILIMATHALAWA", "GAMPOLA", "PUPURESSA"],
    "12": ["KEGALLE", "WARAKAPOLA", "DEHIOWITA"],
    "13": ["KILINOCHCHI"],
    "14": ["KURUNEGALA", "KULIYAPITIYA"],
    "15": ["MANNAR"],
    "16": ["MATALE", "DAMBULLA"],
    "17": ["MATARA", "URUBOKKA"],
    "18": ["MONERAGALA"],
    "19": ["MULLAITIVU"],
    "20": ["NUWARA ELIYA", "PUSSELLAWA", "UPCOT"],
    "21": ["POLONNARUWA"],
    "22": ["PUTTALAM", "PALLAMA", "KALPITIYA", "CHILAW"],
    "23": ["RATNAPURA", "BALANGODA", "EHELIYAGODA", "EMBILIPITIYA", "GODAKAWELA", "NIVITIGALA"],
    "24": ["TRINCOMALEE"],
    "25": ["VAVUNIYA"],
}

COMPANIES = [
    # company_code / sales_org, name, main plant
    ("2010", "Enterprise Trading Company", "2010"),
    ("4010", "Global Consumer Products Ltd", "4020"),
]

PLANTS = [
    # plant, name, region, company_code   (4020 is new: the reference leaves this code blank)
    ("2010", "Galle Industrial", "06", "2010"),
    ("4020", "Kandy Production Facility", "11", "4010"),
]

STORAGE_LOCATIONS = [
    ("2010", "0010", "Main Store"),
    ("2010", "0030", "Finished Goods"),
    ("2010", "0032", "Returns Store"),
    ("2010", "0090", "Distribution Hub"),
    ("4020", "0010", "Main Store"),
    ("4020", "0030", "Finished Goods"),
]

DISTRIBUTION_CHANNELS = [
    ("10", "General Trade"),
    ("20", "Export Sales"),
    ("30", "Modern Trade"),  # added: supermarket chains
]

DIVISIONS = [
    ("02", "Agriculture Business"),
    ("03", "Consumer Products"),
    ("04", "Healthcare Products"),
    ("30", "Building Solutions"),
    ("31", "Plastic Products"),
]

CUSTOMER_GROUPS = [
    # code, name, share of customers
    ("03", "Domestic", 0.38),
    ("20", "Distributors", 0.18),
    ("21", "Direct Dealers", 0.27),
    ("22", "Institution", 0.04),
    ("24", "Plantations", 0.10),
    ("37", "Government", 0.03),
]

# Sales offices: one per province (codes from the reference; K001 and others are added)
SALES_OFFICES = [
    # office, name, region (district), province
    ("C001", "Colombo", "05", "Western"),
    ("FN04", "Negombo", "07", "Western"),
    ("K001", "Kandy", "11", "Central"),
    ("FN01", "Galle", "06", "Southern"),
    ("A005", "Matara", "17", "Southern"),
    ("A101", "Jaffna", "09", "Northern"),
    ("A201", "Batticaloa", "04", "Eastern"),
    ("FN03", "Kurunegala", "14", "North Western"),
    ("FN06", "Anuradhapura", "02", "North Central"),
    ("FN07", "Badulla", "03", "Uva"),
    ("FN05", "Ratnapura", "23", "Sabaragamuwa"),
]
# Districts served by a second office in their province; all others use the province office
OFFICE_BY_DISTRICT = {"07": "FN04", "17": "A005", "08": "A005"}

MATERIAL_TYPES = [
    ("ZFRT", "Finished Material"),
    ("ZTRD", "Trading Material"),
    ("ZROH", "Raw material"),
    ("ZSER", "Serviceable Material"),
    ("ZSRV", "Service Materials"),
]
MARGIN_BY_TYPE = {
    "ZFRT": (0.30, 0.40),
    "ZTRD": (0.12, 0.20),
    "ZROH": (0.08, 0.12),
    "ZSER": (0.45, 0.55),
    "ZSRV": (0.60, 0.70),
}

PRODUCT_GROUPS = [
    ("2CHE03", "FUNGICIDE"),
    ("2CHE04", "INSECTICIDE"),
    ("2CHE06", "WEEDICIDE"),
    ("2CHE99", "UNSPECIFY- PTC"),
    ("2HAF09", "SEEDLING TRAYS"),
    ("40091", "Net work Switch"),
    ("40146", "SYSTEM-SL1000"),
    ("40147", "SYSTEM-SV"),
    ("40174", "Services"),
]


@dataclass(frozen=True)
class ProductSpec:
    product_id: str
    description: str
    group: str
    mtype: str
    division: str
    unit: str
    net_kg: float
    list_price: float
    launch: str
    company: str = "2010"


# IDs, groups, types and units follow the reference; descriptions are fictional
# (the reference descriptions are scrambled). New products are marked "new".
PRODUCTS: list[ProductSpec] = [
    # WEEDICIDE (story 4: +8% price from 1 Apr 2026; MC6001LT is the hero product)
    ProductSpec("MC6001LT", "MC600 WEEDICIDE 1LTR (CASE)", "2CHE06", "ZFRT", "04", "CA", 12.0, 2200, "2019-06-01"),
    ProductSpec(
        "MC6004LT", "MC600 WEEDICIDE 4LTR (CASE)", "2CHE06", "ZFRT", "04", "CA", 16.0, 7900, "2025-08-01"
    ),  # new
    ProductSpec("KAR1L", "KAR WEEDICIDE 1LTR", "2CHE06", "ZFRT", "04", "BO", 1.1, 2950, "2020-03-01"),
    ProductSpec("KAR200ML", "KAR WEEDICIDE 200ML", "2CHE06", "ZFRT", "04", "BO", 0.22, 700, "2020-03-01"),
    ProductSpec("KAR400ML", "KAR WEEDICIDE 400ML", "2CHE06", "ZFRT", "04", "BO", 0.44, 1300, "2020-03-01"),
    ProductSpec("KAR4L", "KAR WEEDICIDE 4LTR", "2CHE06", "ZFRT", "04", "BO", 4.4, 10900, "2025-11-10"),  # new
    ProductSpec("000000000020006092", "GLYPHO TRADE 1LTR", "2CHE06", "ZTRD", "04", "BO", 1.1, 1400, "2021-01-15"),
    ProductSpec(
        "000000000020006095", "GLYPHO TRADE PREMIUM 1LTR", "2CHE06", "ZTRD", "04", "BO", 1.1, 1650, "2021-01-15"
    ),
    ProductSpec("000000000020006133", "PARAQ TRADE 5LTR", "2CHE06", "ZTRD", "04", "BO", 5.5, 7200, "2022-04-01"),
    ProductSpec(
        "000000000020006143", "PARAQ TRADE 20LTR (CASE)", "2CHE06", "ZTRD", "04", "CA", 22.0, 27200, "2022-04-01"
    ),
    ProductSpec(
        "000000000010003490", "WEEDICIDE TECHNICAL 1KG", "2CHE06", "ZROH", "04", "KGM", 1.0, 8900, "2018-01-01"
    ),
    # INSECTICIDE (story 6: HGL400ML returns spike from 1 Jul 2026)
    ProductSpec("HGL1LTR", "HGL INSECTICIDE 1LTR (CASE)", "2CHE04", "ZFRT", "04", "CA", 12.0, 2490, "2019-02-01"),
    ProductSpec("HGL2LTR", "HGL INSECTICIDE 2LTR (CASE)", "2CHE04", "ZFRT", "04", "CA", 12.0, 6050, "2019-02-01"),
    ProductSpec("HGL400ML", "HGL INSECTICIDE 400ML", "2CHE04", "ZFRT", "04", "BO", 0.44, 1210, "2019-02-01"),
    ProductSpec("HGL4LTR", "HGL INSECTICIDE 4LTR (CASE)", "2CHE04", "ZFRT", "04", "CA", 16.0, 13600, "2019-02-01"),
    ProductSpec("HGL250ML", "HGL INSECTICIDE 250ML", "2CHE04", "ZFRT", "04", "BO", 0.28, 790, "2025-06-01"),  # new
    ProductSpec("HPL1LTR", "HPL INSECTICIDE 1LTR (CASE)", "2CHE04", "ZFRT", "04", "CA", 12.0, 5900, "2021-07-01"),
    ProductSpec("PRI2KG", "PRI INSECTICIDE 2KG", "2CHE04", "ZFRT", "04", "BG", 2.0, 3850, "2020-09-01"),
    ProductSpec("PRI500GR", "PRI INSECTICIDE 500G", "2CHE04", "ZFRT", "04", "BG", 0.5, 1050, "2026-01-15"),  # new
    ProductSpec("PRO02.5GR", "PRO 2.5 GR INSECTICIDE", "2CHE04", "ZFRT", "04", "BG", 1.0, 190, "2018-05-01"),
    ProductSpec("PRO1KG", "PRO GR INSECTICIDE 1KG", "2CHE04", "ZFRT", "04", "BG", 1.0, 690, "2025-09-01"),  # new
    ProductSpec(
        "000000000010018472", "INSECTICIDE TECHNICAL 1LTR", "2CHE04", "ZROH", "04", "LTR", 1.0, 1360, "2018-01-01"
    ),
    # FUNGICIDE (story 5: HEX1LTR new launch 15 Jun 2026)
    ProductSpec("HEX004LT", "HEX FUNGICIDE 4LTR (CASE)", "2CHE03", "ZFRT", "04", "CA", 16.0, 14500, "2019-10-01"),
    ProductSpec("HEX050ML", "HEX FUNGICIDE 50ML", "2CHE03", "ZFRT", "04", "BO", 0.06, 350, "2019-10-01"),
    ProductSpec("HEX100ML", "HEX FUNGICIDE 100ML", "2CHE03", "ZFRT", "04", "BO", 0.11, 400, "2019-10-01"),
    ProductSpec("HEX200ML", "HEX FUNGICIDE 200ML", "2CHE03", "ZFRT", "04", "BO", 0.22, 590, "2019-10-01"),
    ProductSpec("HEX400ML", "HEX FUNGICIDE 400ML", "2CHE03", "ZFRT", "04", "BO", 0.44, 2000, "2019-10-01"),
    ProductSpec("HEX1LTR", "HEX FUNGICIDE 1LTR", "2CHE03", "ZFRT", "04", "BO", 1.1, 4900, "2026-06-15"),  # new, story 5
    ProductSpec("MAN500GR", "MAN FUNGICIDE 500G", "2CHE03", "ZFRT", "04", "BG", 0.5, 650, "2020-06-01"),
    ProductSpec("MAN1KG", "MAN FUNGICIDE 1KG", "2CHE03", "ZFRT", "04", "BG", 1.0, 1190, "2026-02-01"),  # new
    # Other crop-care items
    ProductSpec("000000000020001972", "SPRAYER NOZZLE SET", "2CHE99", "ZTRD", "04", "EA", 0.3, 1350, "2020-01-01"),
    ProductSpec("000000000010003480", "ADJUVANT TECHNICAL 1LTR", "2CHE99", "ZROH", "04", "LTR", 1.0, 720, "2018-01-01"),
    ProductSpec("000000000020002191", "SEEDLING TRAY 104 CELL", "2HAF09", "ZTRD", "04", "EA", 0.15, 39, "2021-03-01"),
    ProductSpec(
        "000000000020002192", "SEEDLING TRAY 50 CELL", "2HAF09", "ZTRD", "04", "EA", 0.10, 29, "2025-07-01"
    ),  # new
    # Building solutions (company 4010)
    ProductSpec("S-10000571", "SYSTEM SV SPARE KIT A", "40147", "ZSER", "30", "EA", 2.0, 3500, "2022-01-01", "4010"),
    ProductSpec("S-10000572", "SYSTEM SV SPARE KIT B", "40147", "ZSER", "30", "EA", 2.5, 4800, "2022-01-01", "4010"),
    ProductSpec("S-10000576", "SYSTEM SV SPARE KIT C", "40147", "ZSER", "30", "EA", 3.0, 6200, "2023-05-01", "4010"),
    ProductSpec(
        "S-10000573", "SYSTEM SL1000 CONTROL UNIT", "40146", "ZSER", "30", "EA", 5.0, 18500, "2022-01-01", "4010"
    ),
    ProductSpec(
        "ZSRV1000453", "SYSTEM INSTALLATION SERVICE", "40174", "ZSRV", "30", "EA", 0.0, 150000, "2022-01-01", "4010"
    ),
    ProductSpec(
        "000000000020004383", "NETWORK SWITCH 24 PORT", "40091", "ZTRD", "31", "EA", 3.5, 23000, "2023-02-01", "4010"
    ),
]

# Customer name pools (the reference names are anonymised placeholders; we reuse them)
TRADE_NAMES = [
    "Blue Ocean Traders",
    "Galaxy Trading",
    "Global Traders",
    "Golden Retailers",
    "Metro Retail",
    "Ocean Enterprises",
    "Pinnacle Trading",
    "Prime Distributors",
    "Smart Retail",
    "Star Retailers",
    "Sunrise Wholesalers",
    "Topline Traders",
    "Unity Distributors",
    "Value Stores",
    "Elite Stores",
    "Evergreen Stores",
    "Lanka Agro Centre",
    "Ceylon Farm Supplies",
    "Perera Agro Traders",
    "Silva Hardware",
    "Fernando & Sons",
    "Jayasinghe Distributors",
    "Wijesinghe Agro Services",
    "Nadarajah Traders",
    "Sivakumar Agro Stores",
    "Mohamed Brothers",
    "Rathnayake Stores",
    "Kumara Agro Mart",
    "Harvest Agro Centre",
    "Govi Sahana Centre",
    "Island Agro Distributors",
    "Paddy Field Supplies",
]
CHAIN_NAMES = ["Super Market Group", "City Mart", "Central Mart", "Nova Markets"]
PLANTATION_NAMES = [
    "Hill Country Plantations",
    "Green Valley Estates",
    "Uva Highlands Estates",
    "Kelani Valley Estates",
    "Mountain Mist Tea Estates",
    "Ruhuna Plantations",
]
GOVERNMENT_NAMES = ["Divisional Agrarian Office", "Provincial Agriculture Department"]
INSTITUTION_NAMES = ["Agricultural Training Centre", "Farm Research Station"]

FIRST_NAMES = [
    "Kasun",
    "Nimali",
    "Tharindu",
    "Dilani",
    "Ruwan",
    "Shalini",
    "Chaminda",
    "Sanduni",
    "Pradeep",
    "Ishara",
    "Arun",
    "Priya",
    "Fazal",
    "Ayesha",
    "Nuwan",
    "Kavindi",
]
LAST_NAMES = [
    "Perera",
    "Fernando",
    "Silva",
    "Jayasinghe",
    "Bandara",
    "Wijesinghe",
    "Rathnayake",
    "Kumara",
    "Sivakumar",
    "Nadarajah",
    "Mohamed",
    "Dissanayake",
    "Herath",
    "Gunawardena",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts(rng: np.random.Generator, start: date, end: date) -> str:
    """Random 'YYYY-MM-DD HH:MM:SS.mmm' timestamp between two dates (reference format)."""
    span = max((end - start).days, 1) * 86_400_000
    moment = datetime.combine(start, datetime.min.time()) + timedelta(milliseconds=int(rng.integers(0, span)))
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def _history_start(as_of: date) -> date:
    return as_of - timedelta(days=HISTORY_DAYS)


# ---------------------------------------------------------------------------
# Builders (pure functions: inputs in, DataFrame out)
# ---------------------------------------------------------------------------


def build_regions() -> pd.DataFrame:
    return pd.DataFrame(
        [{"region": c, "district_name": d, "province": p, "country": COUNTRY} for c, d, p, _ in DISTRICTS]
    )


def build_cities() -> pd.DataFrame:
    rows = [{"city": city, "region": code} for code, cities in CITIES.items() for city in cities]
    return pd.DataFrame(rows)


def build_lookups() -> dict[str, pd.DataFrame]:
    return {
        "companies": pd.DataFrame(
            [
                {
                    "company_code": c,
                    "company_name": n,
                    "sales_org": c,
                    "sales_org_name": n,
                    "company_currency": CURRENCY,
                    "country": COUNTRY,
                    "default_plant": p,
                }
                for c, n, p in COMPANIES
            ]
        ),
        "plants": pd.DataFrame(PLANTS, columns=["plant", "plant_name", "region", "company_code"]),
        "storage_locations": pd.DataFrame(
            STORAGE_LOCATIONS, columns=["plant", "storage_location", "storage_location_name"]
        ),
        "distribution_channels": pd.DataFrame(
            DISTRIBUTION_CHANNELS, columns=["distribution_channel", "distribution_channel_name"]
        ),
        "divisions": pd.DataFrame(DIVISIONS, columns=["division", "division_name"]),
        "customer_groups": pd.DataFrame(
            [(c, n) for c, n, _ in CUSTOMER_GROUPS], columns=["customer_group", "customer_group_name"]
        ),
        "sales_offices": pd.DataFrame(
            SALES_OFFICES, columns=["sales_office", "sales_office_name", "region", "province"]
        ),
        "product_groups": pd.DataFrame(PRODUCT_GROUPS, columns=["product_group", "product_group_name"]),
        "product_types": pd.DataFrame(MATERIAL_TYPES, columns=["product_type", "product_type_name"]),
    }


def build_products(rng: np.random.Generator, as_of: date) -> pd.DataFrame:
    groups = dict(PRODUCT_GROUPS)
    mtypes = dict(MATERIAL_TYPES)
    divisions = dict(DIVISIONS)
    plants = {c: p for c, _, p in COMPANIES}
    rows = []
    for s in PRODUCTS:
        margin = float(rng.uniform(*MARGIN_BY_TYPE[s.mtype]))
        launch = date.fromisoformat(s.launch)
        if launch >= as_of:
            raise AssertionError(f"{s.product_id} launches after the as-of date")
        rows.append(
            {
                "product_id": s.product_id,
                "product_description": s.description,
                "product_group": s.group,
                "product_group_name": groups[s.group],
                "product_type": s.mtype,
                "product_type_name": mtypes[s.mtype],
                "division": s.division,
                "division_name": divisions[s.division],
                "company_code": s.company,
                "plant": plants[s.company],
                "base_unit": s.unit,
                "sales_unit": s.unit,
                "gross_weight": f"{s.net_kg * 1.08:.3f}",
                "net_weight": f"{s.net_kg:.3f}",
                "weight_unit": "KGM",
                "list_price": f"{s.list_price:.2f}",
                "standard_cost": f"{s.list_price * (1 - margin):.2f}",
                "currency": CURRENCY,
                "launch_date": launch.isoformat(),
                "last_updated_timestamp": _ts(rng, max(launch, _history_start(as_of)), as_of),
            }
        )
    return pd.DataFrame(rows)


def build_customers(rng: np.random.Generator, as_of: date, count: int = 600) -> pd.DataFrame:
    history_start = _history_start(as_of)
    codes = [c for c, *_ in DISTRICTS]
    weights = np.array([w for *_, w in DISTRICTS])
    province = {c: p for c, _, p, _ in DISTRICTS}
    office_by_province = {p: o for o, _, _, p in SALES_OFFICES if o not in OFFICE_BY_DISTRICT.values()}
    group_codes = [c for c, _, _ in CUSTOMER_GROUPS]
    group_names = {c: n for c, n, _ in CUSTOMER_GROUPS}
    group_share = np.array([s for *_, s in CUSTOMER_GROUPS])

    districts = rng.choice(codes, size=count, p=weights / weights.sum())
    groups = rng.choice(group_codes, size=count, p=group_share / group_share.sum())

    used: set[str] = set()
    chain_head: dict[str, str] = {}
    rows = []
    for i in range(count):
        region, group = str(districts[i]), str(groups[i])
        city = CITIES[region][int(rng.integers(len(CITIES[region])))]
        town = city.title()

        # ~5% of accounts use the older "00000xxxxx" number range, like the reference
        customer_id = f"{50100 + i:010d}" if rng.random() < 0.05 else f"{1001000 + i:010d}"

        channel = "10"
        if group == "24":
            base = PLANTATION_NAMES[int(rng.integers(len(PLANTATION_NAMES)))]
        elif group == "37":
            base = GOVERNMENT_NAMES[int(rng.integers(len(GOVERNMENT_NAMES)))]
        elif group == "22":
            base = INSTITUTION_NAMES[int(rng.integers(len(INSTITUTION_NAMES)))]
        elif group == "03" and rng.random() < 0.20:
            base = CHAIN_NAMES[int(rng.integers(len(CHAIN_NAMES)))]
            channel = "30"
        else:
            base = TRADE_NAMES[int(rng.integers(len(TRADE_NAMES)))]
            if group == "20" and rng.random() < 0.25:
                channel = "20"

        name = f"{base} {town}"
        n = 2
        while name in used:
            name, n = f"{base} {town} Branch {n}", n + 1
        used.add(name)

        # Supermarket branches are paid for by the chain's head office (first branch seen)
        payer = customer_id
        if channel == "30":
            payer = chain_head.setdefault(base, customer_id)

        created = (
            history_start + timedelta(days=int(rng.integers(0, HISTORY_DAYS - 30)))
            if rng.random() < 0.12
            else history_start - timedelta(days=int(rng.integers(30, 2500)))
        )
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": name,
                "customer_full_name": f"{name} Pvt Ltd" if group in ("03", "20", "21", "24") else name,
                "customer_group": group,
                "customer_group_name": group_names[group],
                "country": COUNTRY,
                "city": city,
                "region": region,
                "sales_office": OFFICE_BY_DISTRICT.get(region, office_by_province[province[region]]),
                "distribution_channel": channel,
                "sales_org": "2010",
                "payer_customer_id": payer,
                "created_date": created.isoformat(),
                "last_updated_timestamp": _ts(rng, max(created, history_start), as_of),
            }
        )
    return pd.DataFrame(rows)


def build_sales_reps(rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    n = 0
    for office, office_name, _, prov in SALES_OFFICES:
        manager = f"{FIRST_NAMES[int(rng.integers(len(FIRST_NAMES)))]} {LAST_NAMES[int(rng.integers(len(LAST_NAMES)))]}"
        for _ in range(4 if office in ("C001", "FN04", "K001") else 3):
            n += 1
            rows.append(
                {
                    "sales_rep_id": f"SR{n:03d}",
                    "rep_name": f"{FIRST_NAMES[int(rng.integers(len(FIRST_NAMES)))]} {LAST_NAMES[int(rng.integers(len(LAST_NAMES)))]}",
                    "sales_office": office,
                    "sales_office_name": office_name,
                    "province": prov,
                    "manager_name": manager,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Dirt (applied AFTER clean data is built; every change is logged)
# ---------------------------------------------------------------------------


def _city_variant(rng: np.random.Generator, city: str) -> str:
    """Spelling variants seen in the reference: case, trailing dot, dashes, double spaces."""
    if city.startswith("COLOMBO "):
        num = city.split(" ")[1]
        options = [
            f"COLOMBO - {num}",
            f"Colombo {num}",
            f"COLOMBO  {num.lstrip('0')}",
            f"colombo {num}",
            f"COLOMBO - {num}.",
        ]
    else:
        options = [city.title(), f"{city}.", city.lower(), f"{city.title()}."]
    return options[int(rng.integers(len(options)))]


def _pick(rng: np.random.Generator, index: pd.Index, rate: float) -> list[int]:
    k = max(1, int(round(len(index) * rate)))
    return sorted(int(i) for i in rng.choice(index, size=k, replace=False))


def dirty_customers(rng: np.random.Generator, df: pd.DataFrame, as_of: date) -> tuple[pd.DataFrame, list[dict]]:
    df = df.copy()
    log: list[dict] = []

    def record(kind: str, rows: list[int], note: str) -> None:
        log.append(
            {
                "table": "customers",
                "dirt": kind,
                "count": len(rows),
                "customer_ids": sorted(df.loc[rows, "customer_id"].tolist()),
                "note": note,
            }
        )

    rows = _pick(rng, df.index, 0.10)
    df.loc[rows, "city"] = [_city_variant(rng, c) for c in df.loc[rows, "city"]]
    record("city_text_variant", rows, "Same city spelled differently; silver maps to the canonical name in cities.csv")

    rows = _pick(rng, df.index, 0.03)
    df.loc[rows, "region"] = ""
    record("blank_region", rows, "Region code missing; silver derives it from the city")

    rows = _pick(rng, df.index, 0.15)
    df.loc[rows, ["customer_group", "customer_group_name"]] = ""
    record("blank_customer_group", rows, "Optional field left empty in the source; silver sets 'Unassigned'")

    rows = _pick(rng, df.index, 0.02)
    df.loc[rows, "customer_name"] = df.loc[rows, "customer_name"] + "  "
    record("trailing_spaces", rows, "Trailing spaces in name; silver trims")

    # Duplicates: an older version of the record, with an OLDER timestamp. Latest must win.
    rows = _pick(rng, df.index, 0.02)
    older = df.loc[rows].copy()
    older["customer_group"], older["customer_group_name"] = "03", "Domestic"
    older["last_updated_timestamp"] = [
        (datetime.strptime(t, "%Y-%m-%d %H:%M:%S.%f") - timedelta(days=int(rng.integers(1, 90)))).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]
        for t in older["last_updated_timestamp"]
    ]
    record("duplicate_older_version", rows, "Same customer twice; keep the row with the latest last_updated_timestamp")

    # IDs without leading zeros (SAP ALPHA conversion skipped by the extract), applied last
    id_rows = _pick(rng, df.index.difference(rows), 0.01)
    record("id_without_leading_zeros", id_rows, "Silver left-pads customer_id to 10 digits")
    df.loc[id_rows, "customer_id"] = df.loc[id_rows, "customer_id"].str.lstrip("0")

    df = pd.concat([df, older], ignore_index=True)
    df = df.iloc[rng.permutation(len(df))].reset_index(drop=True)
    return df, log


def dirty_products(rng: np.random.Generator, df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    df = df.copy()
    log: list[dict] = []
    protected = {stories.HERO_PRODUCT_ID, stories.NEW_PRODUCT_ID, stories.RETURNS_SPIKE_PRODUCT_ID}
    candidates = df.index[~df["product_id"].isin(protected)]

    rows = _pick(rng, candidates, 0.05)
    df.loc[rows, ["product_group", "product_group_name"]] = ""
    log.append(
        {
            "table": "products",
            "dirt": "blank_product_group",
            "count": len(rows),
            "product_ids": df.loc[rows, "product_id"].tolist(),
            "note": "Group missing; silver fills it from the product's family (same ID prefix) or 'Unassigned'",
        }
    )

    rows = _pick(rng, candidates.difference(rows), 0.08)
    df.loc[rows, "product_description"] = df.loc[rows, "product_description"].str.title()
    log.append(
        {
            "table": "products",
            "dirt": "description_case_variant",
            "count": len(rows),
            "product_ids": df.loc[rows, "product_id"].tolist(),
            "note": "Mixed case description; silver upper-cases",
        }
    )
    return df, log


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def build_masters(seed: int, as_of: date, dirt: bool = True) -> tuple[dict[str, pd.DataFrame], list[dict]]:
    """Build every master table. Call order is fixed so output is reproducible."""
    rng = np.random.default_rng(seed)
    tables = {"regions": build_regions(), "cities": build_cities(), **build_lookups()}
    tables["products"] = build_products(rng, as_of)
    tables["customers"] = build_customers(rng, as_of)
    tables["sales_reps"] = build_sales_reps(rng)

    dirt_log: list[dict] = []
    if dirt:
        dirt_rng = np.random.default_rng(seed + 1)  # separate stream: clean data is identical with or without dirt
        tables["customers"], log_c = dirty_customers(dirt_rng, tables["customers"], as_of)
        tables["products"], log_p = dirty_products(dirt_rng, tables["products"])
        dirt_log = log_c + log_p
    return tables, dirt_log


def write_masters(tables: dict[str, pd.DataFrame], dirt_log: list[dict], out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / "masters"
    target.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, df in tables.items():
        path = target / f"{name}.csv"
        df.to_csv(path, index=False, lineterminator="\n")
        files[path.name] = {"rows": int(len(df)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    manifest = {
        "kind": "masters",
        "source_system": "SAP S/4HANA (simulated)",
        "as_of": as_of.isoformat(),
        "seed": seed,
        "currency": CURRENCY,
        "files": files,
        "dirt": dirt_log,
    }
    # Manifest LAST: its presence means the drop is complete
    manifest_path = target / "masters_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    cfg = load_config()
    parser = argparse.ArgumentParser(description="Generate master data CSVs.")
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.fromisoformat(cfg.business["simulation_start"]),
        help="Simulation start (YYYY-MM-DD) from config. History ends the day before.",
    )
    parser.add_argument("--seed", type=int, default=cfg.business["random_seed"])
    parser.add_argument("--out", type=Path, default=Path(cfg.path(cfg.staging_path)))
    parser.add_argument("--no-dirt", action="store_true", help="Write clean data only")
    args = parser.parse_args(argv)

    tables, dirt_log = build_masters(args.seed, args.as_of, dirt=not args.no_dirt)
    target = write_masters(tables, dirt_log, args.out, args.seed, args.as_of)
    for name, df in tables.items():
        print(f"  {name:<22} {len(df):>5} rows")
    for d in dirt_log:
        print(f"  dirt: {d['table']}.{d['dirt']:<26} {d['count']:>4} rows")
    print(f"Masters written to {target}")


if __name__ == "__main__":
    main()
