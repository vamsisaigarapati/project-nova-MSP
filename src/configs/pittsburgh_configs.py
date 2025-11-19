"""Pittsburgh-specific configuration and helpers."""

from __future__ import annotations

from pathlib import Path
from typing import List, Union

import pandas as pd

from src.configs.common_configs import (
    aggregate_first_sum_by_group,
    assign_revenue_date_generic,
    enforce_strategic_orders,
    tag_msp_from_rep,
    tag_verified_strategic_generic,
    tag_welcome_back_generic,
)
from src.utils.excel_file_operations import load_excel_file

pittsburgh_raw_column_types: List[dict[str, object]] = []
pittsburgh_sisense_columns: List[str] = []


sisense_columns = ['Order #',
 "Sum of 'Net'",
 'Booking person',
 'Commission Rep',
 'Pat',
 'Publication date',
 'Media',
 'Section',
 'MSP',
 'Customer',
 'Ad #',
 'Order # +',
 'Gross',
 'Extra',
 'Discount',
 'Agency comm.',
 'Net',
 '% disc.',
 'Count of matches',
 'Revenue Date',
 'WB 3-6',
 'WB 7-12',
 'WB 12+',
 'Verified Strategic']

raw_column_types = [{'Booking person': 'str'},
 {'Pat': 'str'},
 {'Publication date': 'date'},
 {'Media': 'str'},
 {'Section': 'str'},
 {'Customer': 'str'},
 {'Ad #': 'int'},
 {'Order #': 'int'},
 {'Gross': 'float'},
 {'Extra': 'float'},
 {'Discount': 'float'},
 {'Agency comm.': 'float'},
 {'Net': 'float'},
 {'% disc.': 'float'}]

def calculate_revenue(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate Pittsburgh revenue by Order # using the shared helper."""
    working_df = raw_df.copy()
    if "Net" not in working_df.columns:
        raise KeyError("Pittsburgh raw data missing 'Net' column")
    working_df["Sum of 'Net'"] = pd.to_numeric(working_df["Net"], errors="coerce").fillna(0)
    working_df.drop(columns=["Net"], inplace=True)

    aggregated = aggregate_first_sum_by_group(
        working_df,
        group_column="Order #",
        value_column="Sum of 'Net'",
        count_column_name="Count of matches",
    )

    aggregated["Order # +"] = aggregated["Order #"]
    aggregated = aggregated[aggregated["Sum of 'Net'"] != 0]

    return aggregated




def tag_verified_strategic(
    processed_df: pd.DataFrame,
    *,
    lookup_path,
    strategic_file_name: str,
    sheet_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """Apply the generic strategic tagging using Pittsburgh-specific column mappings."""
    result = tag_verified_strategic_generic(
        processed_df,
        lookup_path=lookup_path,
        strategic_file_name=strategic_file_name,
        sheet_name=sheet_name,
        partner_name=partner_name,
        processed_lookup_columns=[
            ("Customer", "Complete Name"),
        ],
        processed_date_column="Publication date",
        lookup_date_column="Strategic End Date",
        company_column="Company",
        output_column="Verified Strategic",
        strategic_date_output_column="_strategic_date",
        diagnostics_prefix=f"[{partner_name} Strategic]",
    )
    result.drop(columns=["_strategic_date"], inplace=True)
    return result


def enforce_strategic_orders_lookup(
    processed_df: pd.DataFrame,
    *,
    lookup_path,
    lookup_file_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """Ensure Pittsburgh orders listed in the strategic lookup are flagged as strategic."""
    return enforce_strategic_orders(
        processed_df,
        lookup_path=lookup_path,
        lookup_file_name=lookup_file_name,
        partner_name=partner_name,
        processed_order_column="Order #",
        processed_verified_column="Verified Strategic",
        lookup_order_column="Order Number",
    )


def tag_welcome_back(
    processed_df: pd.DataFrame,
    *,
    lookup_path,
    welcome_back_file: str,
    sheet_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """Apply the generic Welcome Back tagging using Pittsburgh-specific columns."""
    return tag_welcome_back_generic(
        processed_df,
        lookup_path=lookup_path,
        welcome_back_file=welcome_back_file,
        sheet_name=sheet_name,
        partner_name=partner_name,
        processed_order_column="Order #",
        processed_date_column="Publication date",
        lookup_order_column="Order Number",
        lookup_company_column="Company",
        lookup_date_column="Welcome Back End Date",
        output_column="WB 3-6",
        diagnostics_prefix=f"[{partner_name} WelcomeBack]",
    )


def assign_revenue_date(
    processed_df: pd.DataFrame,
    *,
    partner_name: str,
) -> pd.DataFrame:
    """
    Assign Revenue Date for Pittsburgh.

    This client always uses the first day of the current month,
    so we rely on the generic helper with calendar_year_or_not=True.
    """
    return assign_revenue_date_generic(
        processed_df,
        calendar_year_or_not=True,
        partner_name=partner_name,
        period_column="Publication date",
        output_column="Revenue Date",
    )


def tag_msp_from_class_lookup(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    lookup_file_name: str,
    lookup_sheet_name: str = "Class List",
) -> pd.DataFrame:
    """
    Fill the MSP column using the Pittsburgh class list lookup.
    """
    if "Section" not in processed_df.columns:
        raise KeyError("Processed DataFrame missing required column: Section")

    result_df = processed_df.copy()

    lookup_df = load_excel_file(
        path=lookup_path,
        file_name=lookup_file_name,
        sheet_name=lookup_sheet_name,
    )

    required_cols = {"Class Code in Client Data", "Ad Category"}
    missing = required_cols - set(lookup_df.columns)
    if missing:
        raise KeyError(f"Pittsburgh class lookup missing columns: {', '.join(sorted(missing))}")

    lookup_df = lookup_df.copy()
    lookup_df["_class_key"] = lookup_df["Class Code in Client Data"].astype(str).str.strip().str.casefold()
    class_map = (
        lookup_df[["_class_key", "Ad Category"]]
        .dropna(subset=["_class_key"])
        .drop_duplicates(subset=["_class_key"], keep="first")
        .set_index("_class_key")["Ad Category"]
    )

    section_keys = result_df["Section"].astype(str).str.strip().str.casefold()
    mapped_categories = section_keys.map(class_map)

    result_df["MSP"] = mapped_categories.where(mapped_categories.notna(), result_df.get("MSP", "Other"))

    return result_df
