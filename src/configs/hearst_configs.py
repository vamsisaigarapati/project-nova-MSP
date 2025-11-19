from pathlib import Path
from typing import Optional, Union

import pandas as pd

from src.config import HEARST_RAW_DIR, HEASRT_FILE
from src.configs.common_configs import (
    aggregate_first_sum_by_group,
    assign_revenue_date_generic,
    enforce_strategic_orders,
    tag_msp_from_rep,
    tag_verified_strategic_generic,
    tag_welcome_back_generic,
)
from src.utils.excel_file_operations import load_excel_file


sisense_columns = [
    "Job Number",
    "Sum of 'Revenue'",
    "Year",
    "Period #",
    "Job Number +",
    "Child Acct #",
    "Inches",
    "Ad Type",
    "Section",
    "Class Code",
    "WoRev Bill Cycle",
    "Child Acct Name",
    "First Issue Date",
    "Full Name LF",
    "Commission Rep",
    "MSP/non-MSP",
    "Business Unit GL",
    "GL_LOB_L1",
    "Pub",
    "Revenue",
    "Count of matches",
    "Verified Strategic",
    "Welcome Back",
    "Renewal",
    "Revenue Date",
    "Wave2 Prior Bill",
]

raw_column_types = [
    {"Year": int},
    {"Period #": int},
    {"Job Number": int},
    {"Child Acct #": str},
    {"Inches": float},
    {"Ad Type": str},
    {"Section": str},
    {"Class Code": str},
    {"WoRev Bill Cycle": str},
    {"Child Acct Name": str},
    {"First Issue Date": "date"},
    {"Full Name LF": str},
    {"Business Unit GL": str},
    {"GL_LOB_L1": str},
    {"Pub": str},
    {"Revenue": float},
]


# def calculate_revenue(raw_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Enrich raw Hearst data with market information and aggregate revenue by Job Number +.
#     """
#     market_list = load_excel_file(
#         path=HEARST_RAW_DIR,
#         file_name=HEASRT_FILE,
#         sheet_name="Hearst Pub Market List",
#     )
#
#     merged_df = raw_df.copy()
#     market_list = market_list.copy()
#     merged_df["Pub_key"] = merged_df["Pub"].astype(str).str.strip().str.lower()
#     market_list["Pub_key"] = market_list["Pub"].astype(str).str.strip().str.lower()
#
#     merged_df = merged_df.merge(
#         market_list[["Pub_key", "Market"]],
#         on="Pub_key",
#         how="inner",
#     )
#
#     merged_df["Job Number"] = merged_df["Job Number"].astype(str).str.strip()
#     merged_df["Market"] = merged_df["Market"].astype(str).str.strip()
#
#     merged_df["Job Number +"] = merged_df.apply(
#         lambda r: f"{r['Market']}{r['Job Number']}"
#         if r["Market"] not in ["", "nan", "None"]
#         else r["Job Number"],
#         axis=1,
#     )
#
#     merged_df["Sum of 'Revenue'"] = pd.to_numeric(merged_df["Revenue"], errors="coerce").fillna(0)
#
#     agg_dict = {col: "first" for col in merged_df.columns if col not in ["Sum of 'Revenue'"]}
#     agg_dict["Sum of 'Revenue'"] = "sum"
#
#     result_df = merged_df.groupby("Job Number +", as_index=False).agg(agg_dict)
#
#     counts = merged_df.groupby("Job Number +").size().reset_index(name="Count of matches")
#     result_df = result_df.merge(counts, on="Job Number +", how="left")
#     print(result_df.columns)
#
#     cols = [c for c in result_df.columns if c not in ["Job Number +", "Sum of 'Revenue'", "Count of matches"]]
#     result_df = result_df[cols + ["Job Number +", "Sum of 'Revenue'", "Count of matches"]]
#     result_df["Job Number +"], result_df["Job Number"] = result_df["Job Number"].copy(), result_df["Job Number +"].copy()
#
#     result_df = result_df[result_df["Sum of 'Revenue'"] != 0.0]
#     return result_df


def calculate_revenue(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich raw Hearst data with market information and aggregate revenue by Job Number +.
    """
    market_list = load_excel_file(
        path=HEARST_RAW_DIR,
        file_name=HEASRT_FILE,
        sheet_name="Hearst Pub Market List",
    )

    merged_df = raw_df.copy()
    market_list = market_list.copy()
    merged_df["Pub_key"] = merged_df["Pub"].astype(str).str.strip().str.lower()
    market_list["Pub_key"] = market_list["Pub"].astype(str).str.strip().str.lower()

    merged_df = merged_df.merge(
        market_list[["Pub_key", "Market"]],
        on="Pub_key",
        how="inner",
    )

    merged_df["Job Number"] = merged_df["Job Number"].astype(str).str.strip()
    merged_df["Market"] = merged_df["Market"].astype(str).str.strip()

    merged_df["Job Number +"] = merged_df.apply(
        lambda r: f"{r['Market']}{r['Job Number']}"
        if r["Market"] not in ["", "nan", "None"]
        else r["Job Number"],
        axis=1,
    )

    merged_df["Sum of 'Revenue'"] = pd.to_numeric(merged_df["Revenue"], errors="coerce").fillna(0)

    aggregated = aggregate_first_sum_by_group(
        merged_df,
        group_column="Job Number +",
        value_column="Sum of 'Revenue'",
        count_column_name="Count of matches",
    )

    aggregated["Job Number +"], aggregated["Job Number"] = (
        aggregated["Job Number"].copy(),
        aggregated["Job Number +"].copy(),
    )

    aggregated = aggregated[aggregated["Sum of 'Revenue'"] != 0.0]
    return aggregated


# def tag_msp_from_rep(processed_df: pd.DataFrame) -> pd.DataFrame:




def enrich_with_msp_reference(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    lookup_file_name: str,
    lookup_sheet_name: str,
) -> pd.DataFrame:
    """
    Resolve \"Assigned, Not\" or \"Wave2, Wave2\" rows using the Not Assigned lookup and apply Wave2 clean-up.
    """
    result_df = processed_df.copy()

    job_number_candidates = ["Job Number +", "Job Number", "Job Number "]
    job_number_col = next((col for col in job_number_candidates if col in result_df.columns), None)
    if not job_number_col:
        raise KeyError("Processed DataFrame missing a 'Job Number' column for MSP enrichment.")

    if "Full Name LF" not in result_df.columns:
        raise KeyError("Processed DataFrame missing required column: Full Name LF")

    lookup_df = load_excel_file(
        path=lookup_path,
        file_name=lookup_file_name,
        sheet_name=lookup_sheet_name,
    )

    required_lookup_cols = {"Job #", "MSP Agent"}
    missing_lookup = required_lookup_cols - set(lookup_df.columns)
    if missing_lookup:
        missing = ", ".join(sorted(missing_lookup))
        raise KeyError(f"Lookup DataFrame missing expected columns: {missing}")

    lookup_df = lookup_df.copy()
    lookup_df["Job #"] = lookup_df["Job #"].astype(str).str.strip()
    lookup_df["MSP Agent"] = lookup_df["MSP Agent"].astype(str).str.strip()

    enrich_targets = {"assigned, not", "wave2, wave2"}
    assigned_mask = (
        result_df["Full Name LF"]
        .astype(str)
        .str.strip()
        .str.casefold()
        .isin(enrich_targets)
    )

    if not assigned_mask.any():
        print("[MSP Enrich] No 'Assigned, Not' or 'Wave2, Wave2' records found; skipping updates.")
        return result_df

    job_series = (
        result_df.loc[assigned_mask, job_number_col]
        .astype(str)
        .str.strip()
    )

    job_map = (
        lookup_df[["Job #", "MSP Agent"]]
        .dropna(subset=["Job #", "MSP Agent"])
        .drop_duplicates(subset=["Job #"], keep="last")
        .set_index("Job #")["MSP Agent"]
    )

    mapped_agents = job_series.map(job_map)
    match_mask = mapped_agents.notna()

    print(f"[MSP Enrich] Target rows: {int(assigned_mask.sum())}")
    print(f"[MSP Enrich] Matches found: {int(match_mask.sum())}")

    matched_indices = job_series.index[match_mask]
    result_df.loc[matched_indices, "Full Name LF"] = mapped_agents.loc[match_mask]
    if "MSP/non-MSP" in result_df.columns:
        result_df.loc[matched_indices, "MSP/non-MSP"] = "MSP"

    unmatched_indices = job_series.index[~match_mask]
    if len(unmatched_indices):
        result_df.loc[unmatched_indices, "Full Name LF"] = "Wave2, Wave2"
        if "MSP/non-MSP" in result_df.columns:
            result_df.loc[unmatched_indices, "MSP/non-MSP"] = "Non-MSP"

    special_wave2_names = {
        "Palmiero, Kristi",
        "zzzColello, Barbara",
        "zzzCollazo, Maria",
        "zzzHenderson, Pam",
        "zzzTrapasso, Rose",
    }
    if "Section" in result_df.columns:
        section_mask = result_df["Section"].astype(str).str.strip().eq("Wave2 Death Notices")
        name_mask = result_df["Full Name LF"].astype(str).str.strip().isin(special_wave2_names)
        wave2_override = section_mask & name_mask
        if wave2_override.any():
            result_df.loc[wave2_override, "Full Name LF"] = "Wave2, Wave2"
            if "MSP/non-MSP" in result_df.columns:
                result_df.loc[wave2_override, "MSP/non-MSP"] = "Non-MSP"

    return result_df

def tag_verified_strategic(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    strategic_file_name: str,
    sheet_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """
    Derive the Verified Strategic flag using the shared helper and apply Hearst-specific legal handling.
    """
    required_processed_cols = {
        "Child Acct #",
        "Child Acct Name",
        "First Issue Date",
        "Ad Type",
    }
    missing = required_processed_cols - set(processed_df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise KeyError(f"Processed DataFrame missing expected columns: {missing_cols}")

    result_df = tag_verified_strategic_generic(
        processed_df,
        lookup_path=lookup_path,
        strategic_file_name=strategic_file_name,
        sheet_name=sheet_name,
        partner_name=partner_name,
        processed_lookup_columns=[
            ("Child Acct #", "Account Number"),
            ("Child Acct Name", "Complete Name"),
        ],
        processed_date_column="First Issue Date",
        lookup_date_column="Strategic End Date",
        company_column="Company",
        output_column="Verified Strategic",
        strategic_date_output_column="_strategic_date",
        diagnostics_prefix=f"[{partner_name} Strategic]",
    )

    legal_mask = result_df["Ad Type"].astype(str).str.contains("legal", case=False, na=False)
    if legal_mask.any():
        result_df.loc[legal_mask, "Verified Strategic"] = 0
        result_df.loc[legal_mask, "_strategic_date"] = pd.NaT

    result_df.drop(columns=["_strategic_date"], inplace=True)

    return result_df


def enforce_strategic_orders_lookup(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    lookup_file_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """Ensure orders listed in the strategic order lookup are flagged as strategic."""
    return enforce_strategic_orders(
        processed_df,
        lookup_path=lookup_path,
        lookup_file_name=lookup_file_name,
        partner_name=partner_name,
        processed_order_column="Job Number +",
        processed_verified_column="Verified Strategic",
        lookup_order_column="Order Number",
    )


def tag_welcome_back(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    welcome_back_file: str,
    sheet_name: str,
    partner_name: str,
) -> pd.DataFrame:
    """
    Apply the generic welcome back tagging with Hearst-specific columns.
    """
    return tag_welcome_back_generic(
        processed_df,
        lookup_path=lookup_path,
        welcome_back_file=welcome_back_file,
        sheet_name=sheet_name,
        partner_name=partner_name,
        processed_order_column="Job Number +",
        processed_date_column="First Issue Date",
        lookup_order_column="Order Number",
        lookup_company_column="Company",
        lookup_date_column="Welcome Back End Date",
        output_column="Welcome Back",
        diagnostics_prefix=f"[{partner_name} WelcomeBack]",
    )


def assign_revenue_date(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    calendar_file: str,
    partner_name: str,
    calendar_year_or_not: bool,
    sheet_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Populate Revenue Date using the shared helper.
    """
    return assign_revenue_date_generic(
        processed_df,
        calendar_year_or_not=calendar_year_or_not,
        partner_name=partner_name,
        period_column="Period #",
        lookup_path=lookup_path,
        calendar_file=calendar_file,
        sheet_name=sheet_name,
        calendar_period_candidates=("Period #", "Period", "Period#", "Period Num"),
        output_column="Revenue Date",
    )
