"""Boston-specific configuration helpers."""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from src.configs.common_configs import (
    enforce_strategic_orders,
    tag_verified_strategic_generic,
)
from src.utils.excel_file_operations import load_excel_file

boston_raw_column_types: List[dict[str, object]] = [
    {"OrderURN": "str"},
    {"CustomerURN": "str"},
    {"Customer_Number": "float"},
    {"Customer_Name": "str"},
    {"Agency_URN": "str"},
    {"Agency_Number": "float"},
    {"Agency_Name": "str"},
    {"TitleType1": "str"},
    {"Title": "str"},
    {"PageGroup": "str"},
    {"Class": "str"},
    {"Position": "str"},
    {"Style": "str"},
    {"Border": "str"},
    {"FT_Campaign_ID": "float"},
    {"lineitem_id": "float"},
    {"Insert_Date": "str"},
    {"Stop_Date": "str"},
    {"Number_Dates": "float"},
    {"Size": "str"},
    {"HJ_Columns": "int"},
    {"HJ_Depth": "float"},
    {"HJ_Width": "float"},
    {"HJ_Lines": "float"},
    {"Insert_Net_Price": "float"},
    {"Insert_Gross_Price": "float"},
    {"Insert_Tax": "float"},
    {"Insert_Tax_Rate": "float"},
    {"Row_Net_Price": "float"},
    {"Row_Gross_Price": "float"},
    {"Reason_Code": "str"},
    {"Reason_Description": "str"},
    {"Ad_Color": "str"},
    {"PONumber": "str"},
    {"StyleType": "str"},
    {"First_Date": "str"},
    {"Last_Date": "str"},
    {"Edzone": "str"},
    {"Invoice_Text": "str"},
    {"Physical_Inserts": "float"},
    {"Number_Of_Pages": "int"},
    {"Advertiser_Type": "str"},
    {"Create_Time": "str"},
    {"UpdateTime": "str"},
    {"Booking_Notes": "str"},
    {"PackageName": "str"},
    {"Payment": "str"},
    {"AdSource": "float"},
    {"Scrutiny": "float"},
    {"ImmigrationAD": "str"},
    {"SummaryClass": "str"},
    {"External_AD_ID": "str"},
    {"OrderKeyer": "str"},
    {"Team_Keyer": "str"},
    {"OperatorName": "str"},
    {"Scrutiny_Release_Operator": "str"},
    {"Team_Name": "str"},
    {"OrderTaker": "str"},
    {"Sales_Rep": "str"},
    {"SRWork_Responsibility": "str"},
    {"HouseAD": "int"},
    {"Insert_Text_Version": "int"},
    {"Contract_ID": "float"},
    {"Original_Sold_Amount": "float"},
]

boston_sisense_columns: List[str] = [
    "OrderURN",
    "CustomerURN",
    "Customer_Number",
    "Customer_Name",
    "Agency_URN",
    "Agency_Number",
    "Agency_Name",
    "TitleType1",
    "Title",
    "PageGroup",
    "Class",
    "Position",
    "Style",
    "Border",
    "FT_Campaign_ID",
    "Insert_Date",
    "Stop_Date",
    "Number_Dates",
    "Size",
    "HJ_Columns",
    "HJ_Depth",
    "HJ_Width",
    "HJ_Lines",
    "Insert_Net_Price",
    "Insert_Gross_Price",
    "Insert_Tax",
    "Insert_Tax_Rate",
    "Row_Net_Price",
    "Row_Gross_Price",
    "Reason_Code",
    "Reason_Description",
    "Ad_Color",
    "PONumber",
    "StyleType",
    "First_Date",
    "Last_Date",
    "Edzone",
    "Invoice_Text",
    "Physical_Inserts",
    "Number_Of_Pages",
    "Advertiser_Type",
    "Create_Time",
    "UpdateTime",
    "Booking_Notes",
    "PackageName",
    "Payment",
    "AdSource",
    "Scrutiny",
    "ImmigrationAD",
    "SummaryClass",
    "External_AD_ID",
    "OrderKeyer",
    "Team_Keyer",
    "OperatorName",
    "Scrutiny_Release_Operator",
    "Team_Name",
    "OrderTaker",
    "Sales_Rep",
    "SRWork_Responsibility",
    "HouseAD",
    "Insert_Text_Version",
    "Contract_ID",
    "Revenue_Date",
    "Strategic_Flag",
]


def calculate_revenue(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Boston feed currently passes through raw data."""
    return raw_df.copy()


def update_immigration_flags(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Path | str,
    lookup_file_name: str,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    """Use the immigration lookup to standardize ImmigrationAD for duplicated orders."""
    if "OrderURN" not in processed_df.columns or "ImmigrationAD" not in processed_df.columns:
        raise KeyError("Processed DataFrame must include 'OrderURN' and 'ImmigrationAD'.")

    def normalize_order_value(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if not text:
            return ""
        cleaned = text.replace(",", "")
        try:
            number = float(cleaned)
        except ValueError:
            return cleaned.casefold()
        if number.is_integer():
            return str(int(number))
        return cleaned.casefold()

    result_df = processed_df.copy()
    result_df["_order_key"] = result_df["OrderURN"].apply(normalize_order_value)
    result_df["_immigration_norm"] = (
        result_df["ImmigrationAD"].astype(str).str.strip().str.upper().replace({"": pd.NA, "NAN": pd.NA})
    )

    conflict_counts = result_df.groupby("_order_key")["_immigration_norm"].apply(
        lambda col: col.replace({"": pd.NA}).dropna().nunique()
    )
    conflicting_keys = conflict_counts[conflict_counts > 1].index.tolist()
    if not conflicting_keys:
        print("[Boston Immigration] No conflicting ImmigrationAD values found; skipping lookup.")
        result_df.drop(columns=["_order_key", "_immigration_norm"], inplace=True)
        return result_df

    conflict_mask = result_df["_order_key"].isin(conflicting_keys)
    print(f"[Boston Immigration] Rows with conflicting ImmigrationAD values: {int(conflict_mask.sum())}")
    sample_conflicts = (
        result_df.loc[conflict_mask, "OrderURN"].drop_duplicates().head(5).tolist()
    )
    print(f"[Boston Immigration] Example conflicting OrderURNs: {sample_conflicts}")

    lookup_df = load_excel_file(
        path=lookup_path,
        file_name=lookup_file_name,
        sheet_name=sheet_name,
    )

    required_cols = {"Order Number", "Immigration Order"}
    missing = required_cols - set(lookup_df.columns)
    if missing:
        raise KeyError(f"Boston immigration lookup missing columns: {', '.join(sorted(missing))}")

    lookup_df = lookup_df.copy()
    lookup_df["Order Number Normalized"] = lookup_df["Order Number"].apply(normalize_order_value)

    def to_flag(value: object) -> str:
        if pd.isna(value):
            return "N"
        if isinstance(value, (int, float)) and not pd.isna(value):
            if value == 1:
                return "Y"
            if value == 0:
                return "N"
        text = str(value).strip()
        if not text:
            return "N"
        return "Y" if text.lower() in {"true", "1"} else "N"

    lookup_df["Immigration Order"] = lookup_df["Immigration Order"].apply(to_flag)
    order_map = (
        lookup_df[["Order Number Normalized", "Immigration Order"]]
        .dropna(subset=["Order Number Normalized"])
        .drop_duplicates(subset=["Order Number Normalized"], keep="first")
        .set_index("Order Number Normalized")["Immigration Order"]
    )

    result_df["_lookup_flag"] = result_df["_order_key"].map(order_map)
    update_mask = conflict_mask & result_df["_lookup_flag"].notna()
    if update_mask.any():
        result_df.loc[update_mask, "ImmigrationAD"] = result_df.loc[update_mask, "_lookup_flag"]

    missing_keys = set(conflicting_keys) - set(order_map.index)
    if missing_keys:
        missing_examples = (
            result_df.loc[result_df["_order_key"].isin(list(missing_keys)), "OrderURN"].drop_duplicates().head(5).tolist()
        )
        print(
            f"[Boston Immigration] WARNING: {len(missing_keys)} conflicting OrderURNs not found in lookup. Examples: {missing_examples}"
        )

    result_df.drop(columns=["_lookup_flag"], inplace=True)
    result_df["_immigration_norm"] = (
        result_df["ImmigrationAD"].astype(str).str.strip().str.upper().replace({"": pd.NA, "NAN": pd.NA})
    )
    still_conflicts = result_df.groupby("_order_key")["_immigration_norm"].apply(
        lambda col: col.replace({"": pd.NA}).dropna().nunique()
    ) > 1
    if still_conflicts.any():
        remaining_mask = result_df["_order_key"].isin(still_conflicts[still_conflicts].index)
        print(
            f"[Boston Immigration] WARNING: {int(remaining_mask.sum())} rows still have conflicting ImmigrationAD values after lookup."
        )

    result_df.drop(columns=["_order_key", "_immigration_norm"], inplace=True)
    return result_df


def tag_verified_strategic(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Path | str,
    strategic_file_name: str,
    sheet_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """Apply strategic tagging with sales person replacement for Boston."""
    required_cols = {"CustomerURN", "Customer_Name", "Insert_Date", "OperatorName"}
    missing = required_cols - set(processed_df.columns)
    if missing:
        raise KeyError(f"Processed DataFrame missing columns: {', '.join(sorted(missing))}")

    result_df = tag_verified_strategic_generic(
        processed_df,
        lookup_path=lookup_path,
        strategic_file_name=strategic_file_name,
        sheet_name=sheet_name,
        partner_name=partner_name,
        processed_lookup_columns=[
            ("CustomerURN", "Account Number"),
            ("Customer_Name", "Complete Name"),
        ],
        processed_date_column="Insert_Date",
        lookup_date_column="Strategic End Date",
        company_column="Company",
        output_column="Strategic_Flag",
        strategic_date_output_column="_strategic_date",
        diagnostics_prefix=f"[{partner_name} Strategic]",
        sales_person_replacement=True,
        processed_sales_column="OperatorName",
    )

    result_df.drop(columns=["_strategic_date"], inplace=True)
    return result_df


def enforce_strategic_orders_lookup(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Path | str,
    lookup_file_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """Ensure Boston orders listed in strategic orders lookup are flagged and operator updated."""
    return enforce_strategic_orders(
        processed_df,
        lookup_path=lookup_path,
        lookup_file_name=lookup_file_name,
        partner_name=partner_name,
        processed_order_column="OrderURN",
        processed_verified_column="Strategic_Flag",
        lookup_order_column="Order Number",
        sales_person_replacement=True,
        processed_sales_column="OperatorName",
    )
