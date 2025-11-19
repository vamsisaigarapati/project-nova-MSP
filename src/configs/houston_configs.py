"""Houston-specific configuration helpers."""

from __future__ import annotations

from typing import List

import pandas as pd

houston_raw_column_types: List[dict[str, object]] = [
    {"Parent Acct": "str"},
    {"Parent Acct #": "float"},
    {"Child Acct": "str"},
    {"Child Acct #": "float"},
    {"Business Unit": "str"},
    {"Job #": "str"},
    {"Order #": "str"},
    {"External Order #": "str"},
    {"Entry Date": "date"},
    {"Issue Date": "date"},
    {"Invoice Date": "date"},
    {"Invoice #": "str"},
    {"Fiscal Period #": "float"},
    {"Fiscal Week #": "float"},
    {"Revenue": "float"},
    {"Channel Group": "str"},
    {"Edition Pub": "str"},
    {"Section": "str"},
    {"Ad Vertical": "str"},
    {"Classification": "str"},
    {"Sales Team Name": "str"},
    {"Sales Rep": "str"},
    {"Order Taker": "str"},
]

houston_sisense_columns: List[str] = [
    "Parent Account",
    "Parent Acc. #",
    "Child Account",
    "Child Acc. #",
    "Child Account.1",
    "Child Acct #",
    "Business Unit",
    "Job #",
    "Order #",
    "Issue Date",
    "Invoice Date",
    "Invoice #",
    "Revenue",
    "Channel Group",
    "Edition Pub",
    "Fiscal Week #",
    "Section",
    "Ad Vertical",
    "Classification Description",
    "Team Name",
    "Fiscal Period #",
    "Entry Date",
    "Employee Name",
    "NCS ordertaker",
    "iPub Legal ordertaker",
    "iPub Obit ordertaker",
    "RevenueDate",
    "Strategic Flag",
]


def calculate_revenue(raw_df: pd.DataFrame) -> pd.DataFrame:
    """For now return the Houston raw data unchanged."""
    return raw_df.copy()
