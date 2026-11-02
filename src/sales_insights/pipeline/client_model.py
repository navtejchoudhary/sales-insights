"""The client area's gold model: the sales model with descriptions that are true for the SAP billing data.

The client area (`run_pipeline --area client`) builds the same gold tables as the sales demo, so the columns are
the same. Only the descriptions differ: the sales model describes the dummy data (example codes, number ranges,
daily feeds), and Genie reads these comments to answer questions, so on the real data they must say what is
really there. The sales demo keeps its own descriptions unchanged (`gold_model.py`).

Rules on top of the sales model's rules: never a real value from the client's data in here (the repo is
public): no customer numbers, product codes, names or figures; SAP's own standard codes are fine.

    uv run python -m sales_insights.pipeline.client_model > docs/client_gold_model.md
"""

from __future__ import annotations

import copy
import sys

from sales_insights.pipeline import gold_model as gm

INVOICE_TYPES = (
    "ZAOR invoice, ZFOC free of charge, ZARE return, S1 cancellation, ZACR price correction, ZADR debit memo, "
    "S2 credit memo cancellation; the client's own invoice types (e.g. ZCF2, ZAEP, ZFV1, ZFV2) are invoices too "
    "(SAP document category M)"
)
NOT_IN_MASTER = "Empty: the copied SAP master data has no such field."

CHANGES: dict[str, dict] = {
    "fact_sales": {
        "description": (
            "Sales transactions from the client's SAP billing data: one row per invoice line (invoice_number + "
            "invoice_item), whole months only. Includes invoices, free-of-charge deliveries, returns, "
            "cancellations, price corrections and debit memos, so summing net_revenue_amount over any period gives "
            "true net revenue. Money is in Sri Lankan rupees (LKR), excluding tax."
        ),
        "columns": {
            "invoice_number": "SAP billing document number, 10 digits. The number does not show the document type: use document_type.",
            "invoice_type": f"SAP billing type code: {INVOICE_TYPES}.",
            "document_type": (
                "Plain-English billing type: Invoice, Free of charge, Return, Cancellation, Price correction, Debit "
                "memo or Credit memo cancellation. The client's own invoice types show as Other (<type>), e.g. "
                "Other (ZCF2), and count as invoiced revenue."
            ),
            "product_id": "SAP material (product) code. Join to dim_product.",
            "district_code": "SAP sales district of the line. Join to dim_region (names and provinces are 'Unassigned' until the business provides them).",
            "sales_office_code": "SAP sales office that handled the line.",
            "distribution_channel_code": "SAP distribution channel code. Join to dim_channel for the name.",
            "plant_code": "SAP plant (warehouse) that shipped the goods.",
            "company_code": "SAP company code. This data holds one company: 2010.",
            "sales_rep_id": "Sales representative: always empty (not in the SAP billing data).",
            "quantity": "Net quantity in the line's sales unit (units differ by product). Positive for invoices, negative for returns and cancellations, 0 for price corrections.",
            "sold_quantity": "Quantity invoiced (invoice-type documents only), in the line's sales unit; 0 on other rows.",
            "returned_quantity": "Quantity returned (returns only), negative, in the line's sales unit; 0 on other rows.",
            "invoiced_revenue_amount": "Revenue from every invoice-type document (SAP category M: invoices, free of charge and the client's own invoice types) before returns, cancellations and corrections, LKR.",
            "gross_revenue_amount": "Revenue including tax, LKR: net revenue + tax_amount (SAP's own gross field is empty in this data).",
            "tax_amount": "Tax on the line as recorded in SAP, LKR. Mostly VAT (18% on domestic sales; exports are zero-rated), but it may include other taxes or levies.",
            "cost_amount": "SAP's cost on the billing line, LKR. Same sign as quantity.",
            "is_free_of_charge": "True for free-of-charge deliveries (ZFOC): goods given away at no charge.",
            "is_invoice": "True for paid invoice-type documents: ZAOR and the client's own invoice types (SAP category M), not free of charge. The Invoices and Active customers measures count these lines when not is_cancelled.",
            "source_kind": "Which feed the row came from: always 'history' (one-time copy of the SAP billing data).",
            "loaded_business_date": "Empty: the data comes from a one-time copy, not from daily deliveries.",
            "arrival_delay_days": "Empty: the data comes from a one-time copy, not from daily deliveries.",
        },
    },
    "dim_customer": {
        "description": (
            "Customers (sold-to): one row per customer, latest version. Names are personal data and are not copied: "
            "each customer is called 'Customer <customer_id>'."
        ),
        "columns": {
            "customer_name": "Placeholder name 'Customer <customer_id>' (names are not copied); 'Unknown customer <customer_id>' when billed but missing from the master data.",
            "customer_full_name": "Same as customer_name (full names are not copied).",
            "customer_group_code": "SAP customer group code.",
            "customer_group_name": "Customer group name from SAP; 'Unassigned' when the source left it blank.",
            "city": "City in capitals from the customer master data; 'Unassigned' when blank.",
            "district_code": "SAP sales district or region of the customer.",
            "district_name": "District name: 'Unassigned' until the business provides the district names.",
            "province": "Province: 'Unassigned' until the business provides the district to province list.",
            "sales_office_code": f"Sales office that serves the customer. {NOT_IN_MASTER}",
            "sales_office_name": f"Sales office name. {NOT_IN_MASTER}",
            "distribution_channel_code": f"Usual distribution channel of the customer. {NOT_IN_MASTER}",
            "payer_customer_id": f"Customer who pays the invoices. {NOT_IN_MASTER}",
        },
    },
    "dim_product": {
        "description": "Products (SAP materials): one row per material, with material group, type and division from SAP's texts.",
        "columns": {
            "product_id": "SAP material code.",
            "product_description": "Product name in capitals; the material code when the master data has no name; 'UNKNOWN PRODUCT <code>' when billed but missing from the master data.",
            "product_group_code": "SAP material group code.",
            "product_group_name": "Material group name from SAP; 'Unassigned' when unknown.",
            "product_type_code": "SAP material type code.",
            "product_type_name": "Material type name from SAP; 'Unassigned' when unknown.",
            "division_code": "SAP division code (may be empty).",
            "division_name": "Division name from SAP; 'Unassigned' when unknown.",
            "company_code": "Company code that bills the product (from its billing lines).",
            "plant_code": f"Default plant. {NOT_IN_MASTER}",
            "base_unit": "SAP base unit of measure.",
            "list_price_amount": "List price: always empty (prices are not in the copied data).",
            "standard_cost_amount": "Standard cost: always empty (the cost comes from each billing line: fact_sales.cost_amount).",
            "launch_date": f"Date the material was created. {NOT_IN_MASTER}",
        },
    },
    "dim_region": {
        "description": "Geography: one row per SAP sales district. Names and provinces are 'Unassigned' until the business provides them.",
        "columns": {
            "district_code": "SAP sales district code.",
            "district_name": "District name: 'Unassigned' until the business provides it.",
            "province": "Province: 'Unassigned' until the business provides the district to province list.",
        },
    },
    "dim_channel": {
        "description": "Distribution channels from SAP: how goods reach the customer.",
        "columns": {
            "distribution_channel_code": "SAP distribution channel code.",
            "distribution_channel_name": "Channel name from SAP's texts.",
        },
    },
}


def _apply(base: dict[str, dict], changes: dict[str, dict]) -> dict[str, dict]:
    out = copy.deepcopy(base)
    for table, change in changes.items():
        unknown = set(change.get("columns", {})) - set(out[table]["columns"])
        if unknown:
            raise ValueError(f"client_model changes unknown columns of {table}: {sorted(unknown)}")
        if "description" in change:
            out[table]["description"] = change["description"]
        out[table]["columns"].update(change.get("columns", {}))
    return out


TABLES: dict[str, dict] = _apply(gm.TABLES, CHANGES)


def data_dictionary() -> str:
    return gm.data_dictionary(TABLES, "client_model", "client_gold_model.md", "Client (SAP data) gold")


if __name__ == "__main__":
    sys.stdout.write(data_dictionary())
