from pathlib import Path
from typing import Union

import pandas as pd

from src.config import (
    HEARST_RAW_DIR,
    HEASRT_FILE,
    HEARST_LOOKUP_DIR,
    MSP_AGENNT_LOOKUP_FILE,
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

    agg_dict = {col: "first" for col in merged_df.columns if col not in ["Sum of 'Revenue'"]}
    agg_dict["Sum of 'Revenue'"] = "sum"

    result_df = merged_df.groupby("Job Number +", as_index=False).agg(agg_dict)

    counts = merged_df.groupby("Job Number +").size().reset_index(name="Count of matches")
    result_df = result_df.merge(counts, on="Job Number +", how="left")
    print(result_df.columns)

    cols = [c for c in result_df.columns if c not in ["Job Number +", "Sum of 'Revenue'", "Count of matches"]]
    result_df = result_df[cols + ["Job Number +", "Sum of 'Revenue'", "Count of matches"]]
    result_df["Job Number +"], result_df["Job Number"] = result_df["Job Number"].copy(), result_df["Job Number +"].copy()

    result_df = result_df[result_df["Sum of 'Revenue'"] != 0.0]
    return result_df


def tag_msp_from_rep(processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds an 'MSP/non-MSP' flag to processed_df by joining with the MSP lookup.
    """
    rep_list = load_excel_file(
        path=HEARST_LOOKUP_DIR,
        file_name=MSP_AGENNT_LOOKUP_FILE,
        sheet_name="All Rep Names",
    )

    rep_list["_system_lower"] = rep_list["System(s)"].astype(str).str.lower().str.strip()
    rep_list["_agent_lower"] = rep_list["Agent Names"].astype(str).str.lower().str.strip()
    rep_list["_fullname_lower"] = rep_list["Full Name"].astype(str).str.lower().str.strip()

    processed_df["_fullname_lf_lower"] = processed_df["Full Name LF"].astype(str).str.lower().str.strip()

    rep_filtered = rep_list[
        rep_list["_system_lower"].str.contains("hearst", na=False)
        & (rep_list["_agent_lower"] != "wave2, wave2")
    ].copy()

    rep_filtered = (
        rep_filtered[["Agent Names", "System(s)", "Full Name", "_agent_lower"]]
        .drop_duplicates(subset=["_agent_lower"], keep="first")
        .reset_index(drop=True)
    )

    print(f"Filtered rep_list to {len(rep_filtered)} records for 'hearst' system (excluding 'wave2, wave2').")

    merged = processed_df.merge(
        rep_filtered[["_agent_lower"]].rename(columns={"_agent_lower": "_join_key"}),
        how="left",
        left_on="_fullname_lf_lower",
        right_on="_join_key",
        indicator=True,
    )

    merged["MSP/non-MSP"] = merged["_merge"].map(
        {
            "both": "MSP",
            "left_only": "Non-MSP",
        }
    )

    merged = merged.drop(columns=["_fullname_lf_lower", "_join_key", "_merge"], errors="ignore")

    return merged


# def tag_verified_strategic(
#     processed_df: pd.DataFrame,
#     *,
#     lookup_path: Union[str, Path],
#     strategic_file_name: str,
#     sheet_name: str,
# ) -> pd.DataFrame:
#     """
#     Derive the Verified Strategic flag using the strategic account lookup.
#
#     Steps
#     -----
#     1. Load strategic data, coerce the end date column to datetime, and filter to Company == 'Hearst'.
#     2. Resolve strategic end dates with two passes:
#        a. Map on Child Acct # -> Account Number.
#        b. For unmatched rows, map on Child Acct Name -> Complete Name.
#     3. Mark Verified Strategic = 1 when First Issue Date < Strategic end Date, else 0.
#     """
#     result_df = processed_df.copy()
#
#     expected_cols = {"Child Acct #", "Child Acct Name", "First Issue Date"}
#     missing_cols = expected_cols - set(result_df.columns)
#     if missing_cols:
#         missing = ", ".join(sorted(missing_cols))
#         raise KeyError(f"Processed DataFrame missing expected columns: {missing}")
#
#     strategic_df = load_excel_file(
#         path=lookup_path,
#         file_name=strategic_file_name,
#         sheet_name=sheet_name,
#     )
#
#     required_lookup_cols = {"Account Number", "Complete Name", "Strategic End Date", "Company"}
#     missing_lookup_cols = required_lookup_cols - set(strategic_df.columns)
#     if missing_lookup_cols:
#         missing = ", ".join(sorted(missing_lookup_cols))
#         raise KeyError(f"Strategic DataFrame missing expected columns: {missing}")
#
#     strategic_df = strategic_df.copy()
#     strategic_df["Strategic End Date"] = pd.to_datetime(
#         strategic_df["Strategic End Date"],
#         errors="coerce",
#     )
#
#     strategic_df = strategic_df[strategic_df["Company"].astype(str).str.strip().eq("Hearst")]
#     strategic_df = strategic_df.dropna(subset=["Strategic End Date"])
#
#     # Normalize key columns for matching
#     strategic_df["Account Number"] = strategic_df["Account Number"].astype(str).str.strip()
#     strategic_df["Complete Name"] = strategic_df["Complete Name"].astype(str).str.strip()
#
#     strategic_subset = strategic_df[["Account Number", "Complete Name", "Strategic End Date"]].drop_duplicates()
#
#     result_df["Verified Strategic"] = 0
#
#     result_df["_acct_number"] = result_df["Child Acct #"].astype(str).str.strip()
#     result_df["_acct_name"] = result_df["Child Acct Name"].astype(str).str.strip()
#
#     account_map = strategic_subset.set_index("Account Number")["Strategic End Date"]
#     result_df["_strategic_date"] = result_df["_acct_number"].map(account_map)
#
#     missing_strategy_mask = result_df["_strategic_date"].isna()
#     if missing_strategy_mask.any():
#         name_map = strategic_subset.set_index("Complete Name")["Strategic End Date"]
#         matched_names = result_df.loc[missing_strategy_mask, "_acct_name"].map(name_map)
#         result_df.loc[missing_strategy_mask, "_strategic_date"] = matched_names
#
#     first_issue = pd.to_datetime(result_df["First Issue Date"], errors="coerce")
#
#     strategy_mask = first_issue.notna() & result_df["_strategic_date"].notna()
#     verified_mask = strategy_mask & (first_issue < result_df["_strategic_date"])
#     result_df.loc[strategy_mask, "Verified Strategic"] = 0
#     result_df.loc[verified_mask, "Verified Strategic"] = 1
#
#     result_df.drop(columns=["_acct_number", "_acct_name", "_strategic_date"], inplace=True)
#
#     return result_df


# def enrich_with_msp_reference(
#     processed_df: pd.DataFrame,
#     *,
#     lookup_path: Union[str, Path],
#     lookup_file_name: str,
#     lookup_sheet_name: str,
# ) -> pd.DataFrame:
#     """
#     Update MSP-related fields using a reference Excel file.
#     """
#     result_df = processed_df.copy()
#
#     expected_processed_cols = {"Section", "Full Name LF", "MSP/non-MSP", "Job Number +"}
#     missing_processed = expected_processed_cols - set(result_df.columns)
#     if missing_processed:
#         missing = ", ".join(sorted(missing_processed))
#         raise KeyError(f"Processed DataFrame missing expected columns: {missing}")
#
#     lookup_df = load_excel_file(
#         path=lookup_path,
#         file_name=lookup_file_name,
#         sheet_name=lookup_sheet_name,
#     )
#
#     expected_lookup_cols = {"Job #", "MSP Agent"}
#     missing_lookup = expected_lookup_cols - set(lookup_df.columns)
#     if missing_lookup:
#         missing = ", ".join(sorted(missing_lookup))
#         raise KeyError(f"Lookup DataFrame missing expected columns: {missing}")
#
#     wave2_mask = result_df["Section"].fillna("").astype(str).str.contains("Wave2 Death Notices", na=False)
#     result_df.loc[wave2_mask, "Full Name LF"] = "Wave2, Wave2"
#     result_df.loc[wave2_mask, "MSP/non-MSP"] = "Non-MSP"
#
#     result_df["_join_index"] = result_df.index
#     merged = result_df.merge(
#         lookup_df[["Job #", "MSP Agent"]],
#         how="left",
#         left_on="Job Number +",
#         right_on="Job #",
#     )
#
#     joined_mask = merged["MSP Agent"].notna()
#     if joined_mask.any():
#         join_indices = merged.loc[joined_mask, "_join_index"]
#         new_names = merged.loc[joined_mask, "MSP Agent"].values
#         result_df.loc[join_indices, "Full Name LF"] = new_names
#         result_df.loc[join_indices, "MSP/non-MSP"] = "Non-MSP"
#
#     result_df.drop(columns="_join_index", inplace=True)
#
#     return result_df


def enrich_with_msp_reference(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    lookup_file_name: str,
    lookup_sheet_name: str,
) -> pd.DataFrame:
    """
    Update MSP fields by joining Assigned, Not rows to the lookup, then applying Wave2 overrides.
    """
    result_df = processed_df.copy()

    expected_processed_cols = {"Section", "Full Name LF", "MSP/non-MSP",  "Job Number +"}
    missing_processed = expected_processed_cols - set(result_df.columns)
    if missing_processed:
        missing = ", ".join(sorted(missing_processed))
        raise KeyError(f"Processed DataFrame missing expected columns: {missing}")

    lookup_df = load_excel_file(
        path=lookup_path,
        file_name=lookup_file_name,
        sheet_name=lookup_sheet_name,
    )

    expected_lookup_cols = {"Job #", "MSP Agent"}
    missing_lookup = expected_lookup_cols - set(lookup_df.columns)
    if missing_lookup:
        missing = ", ".join(sorted(missing_lookup))
        raise KeyError(f"Lookup DataFrame missing expected columns: {missing}")

    result_df["_join_index"] = result_df.index
    assigned_mask = result_df["Full Name LF"].astype(str).str.strip().eq("Assigned, Not")

    updated_job_numbers_plus: set[str] = set()
    if assigned_mask.any():
        assigned_subset = result_df.loc[assigned_mask].copy()
        merged_assigned = assigned_subset.merge(
            lookup_df[["Job #", "MSP Agent"]],
            how="left",
            left_on="Job Number +",
            right_on="Job #",
        )

        matched_mask = merged_assigned["MSP Agent"].notna()
        if matched_mask.any():
            match_indices = merged_assigned.loc[matched_mask, "_join_index"]
            new_names = merged_assigned.loc[matched_mask, "MSP Agent"].values
            result_df.loc[match_indices, "Full Name LF"] = new_names
            result_df.loc[match_indices, "MSP/non-MSP"] = "Non-MSP"

            job_numbers_plus = (
                result_df.loc[match_indices, "Job Number +"]
                .dropna()
                .astype(str)
                .str.strip()
            )
            updated_job_numbers_plus = {
                value
                for value in job_numbers_plus
                if value and value.lower() != "nan"
            }

    wave2_mask = result_df["Section"].fillna("").astype(str).str.contains("Wave2 Death Notices", na=False)
    if updated_job_numbers_plus:
        wave2_exclusion = result_df["Job Number +"].fillna("").astype(str).str.strip().isin(updated_job_numbers_plus)
        wave2_mask &= ~wave2_exclusion

    if wave2_mask.any():
        result_df.loc[wave2_mask, "Full Name LF"] = "Wave2, Wave2"
        result_df.loc[wave2_mask, "MSP/non-MSP"] = "Non-MSP"

    result_df.drop(columns="_join_index", inplace=True)

    return result_df
def tag_verified_strategic(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    strategic_file_name: str,
    sheet_name: str,
) -> pd.DataFrame:
    """
    Derive the Verified Strategic flag using lookup data with full joins and diagnostics.
    """
    result_df = processed_df.copy()

    required_processed_cols = {
        "Child Acct #",
        "Child Acct Name",
        "First Issue Date",
    }
    missing = required_processed_cols - set(result_df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise KeyError(f"Processed DataFrame missing expected columns: {missing_cols}")

    strategic_df = load_excel_file(
        path=lookup_path,
        file_name=strategic_file_name,
        sheet_name=sheet_name,
    )

    required_lookup_cols = {
        "Account Number",
        "Complete Name",
        "Strategic End Date",
        "Company",
    }
    missing_lookup = required_lookup_cols - set(strategic_df.columns)
    if missing_lookup:
        missing_cols = ", ".join(sorted(missing_lookup))
        raise KeyError(f"Strategic DataFrame missing expected columns: {missing_cols}")

    strategic_df = strategic_df.copy()
    strategic_df["Strategic End Date"] = pd.to_datetime(
        strategic_df["Strategic End Date"],
        errors="coerce",
    )
    strategic_df = strategic_df[
        strategic_df["Company"].astype(str).str.contains("Hearst", case=False, na=False)
    ]
    strategic_df = strategic_df.dropna(subset=["Strategic End Date"])

    strategic_df["Account Number"] = strategic_df["Account Number"].astype(str).str.strip()
    strategic_df["Complete Name"] = strategic_df["Complete Name"].astype(str).str.strip()

    print(f"[Strategic] Lookup rows after filtering Hearst: {len(strategic_df)}")

    result_df["Verified Strategic"] = 0

    result_df["_acct_number"] = result_df["Child Acct #"].astype(str).str.strip()
    result_df["_acct_name"] = result_df["Child Acct Name"].astype(str).str.strip()

    account_join = result_df.merge(
        strategic_df[["Account Number", "Strategic End Date"]],
        how="left",
        left_on="_acct_number",
        right_on="Account Number",
        indicator=True,
    )

    matched_account = account_join["_merge"] == "both"
    print(f"[Strategic] Matched via Account Number: {matched_account.sum()} rows")

    result_df["_strategic_date"] = account_join["Strategic End Date"]

    missing_after_account = result_df["_strategic_date"].isna()
    if missing_after_account.any():
        name_join_input = result_df.loc[missing_after_account].copy()
        name_join = name_join_input.merge(
            strategic_df[["Complete Name", "Strategic End Date"]],
            how="left",
            left_on="_acct_name",
            right_on="Complete Name",
            indicator=True,
        )
        matched_name = name_join["_merge"] == "both"
        print(f"[Strategic] Additional matches via Complete Name: {matched_name.sum()} rows")

        result_df.loc[missing_after_account, "_strategic_date"] = name_join["Strategic End Date"].values

    first_issue = pd.to_datetime(result_df["First Issue Date"], errors="coerce")
    strategy_mask = first_issue.notna() & result_df["_strategic_date"].notna()
    verified_mask = strategy_mask & (first_issue < result_df["_strategic_date"])

    print(f"[Strategic] Rows with usable dates: {strategy_mask.sum()}")
    print(f"[Strategic] Rows flagged as Verified: {verified_mask.sum()}")

    result_df.loc[strategy_mask, "Verified Strategic"] = 0
    result_df.loc[verified_mask, "Verified Strategic"] = 1

    result_df.drop(columns=["_acct_number", "_acct_name", "_strategic_date"], inplace=True)

    return result_df


def tag_welcome_back(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    welcome_back_file: str,
    sheet_name: str,
) -> pd.DataFrame:
    """
    Derive the Welcome Back flag using the welcome back lookup.
    """
    result_df = processed_df.copy()

    required_processed_cols = {"Job Number +", "First Issue Date"}
    missing_processed = required_processed_cols - set(result_df.columns)
    if missing_processed:
        missing = ", ".join(sorted(missing_processed))
        raise KeyError(f"Processed DataFrame missing expected columns: {missing}")

    welcome_df = load_excel_file(
        path=lookup_path,
        file_name=welcome_back_file,
        sheet_name=sheet_name,
    )

    required_lookup_cols = {"Order Number", "Welcome Back End Date", "Company"}
    missing_lookup = required_lookup_cols - set(welcome_df.columns)
    if missing_lookup:
        missing = ", ".join(sorted(missing_lookup))
        raise KeyError(f"Welcome Back DataFrame missing expected columns: {missing}")

    welcome_df = welcome_df.copy()
    welcome_df["Welcome Back End Date"] = pd.to_datetime(
        welcome_df["Welcome Back End Date"],
        errors="coerce",
    )
    welcome_df = welcome_df[
        welcome_df["Company"].astype(str).str.contains("Hearst", case=False, na=False)
    ]
    welcome_df = welcome_df.dropna(subset=["Welcome Back End Date"])
    welcome_df["Order Number"] = welcome_df["Order Number"].astype(str).str.strip()

    print(f"[WelcomeBack] Lookup rows after filtering Hearst: {len(welcome_df)}")

    result_df["Welcome Back"] = 0
    result_df["_job_plus"] = result_df["Job Number +"].astype(str).str.strip()

    joined = result_df.merge(
        welcome_df[["Order Number", "Welcome Back End Date"]],
        how="left",
        left_on="_job_plus",
        right_on="Order Number",
        indicator=True,
    )

    matched = joined["_merge"] == "both"
    print(f"[WelcomeBack] Matched via Job Number +: {matched.sum()} rows")

    first_issue = pd.to_datetime(result_df["First Issue Date"], errors="coerce")
    welcome_dates = joined["Welcome Back End Date"]

    valid_mask = first_issue.notna() & welcome_dates.notna()
    welcome_mask = valid_mask & (first_issue < welcome_dates)

    print(f"[WelcomeBack] Rows with usable dates: {valid_mask.sum()}")
    print(f"[WelcomeBack] Rows flagged as Welcome Back: {welcome_mask.sum()}")

    result_df.loc[welcome_mask.index[welcome_mask], "Welcome Back"] = 1

    result_df.drop(columns=["_job_plus"], inplace=True)

    return result_df


def assign_revenue_date(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Union[str, Path],
    calendar_file: str,
    sheet_name: str,
) -> pd.DataFrame:
    """
    Populate Revenue Date using a calendar lookup filtered to Hearst.
    """
    result_df = processed_df.copy()

    calendar_df = load_excel_file(
        path=lookup_path,
        file_name=calendar_file,
        sheet_name=sheet_name,
    )

    required_cols = {"Company", "Calendar_Date"}
    missing = required_cols - set(calendar_df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise KeyError(f"Calendar DataFrame missing expected columns: {missing_cols}")

    calendar_df = calendar_df[
        calendar_df["Company"].astype(str).str.contains("Hearst", case=False, na=False)
    ]

    if calendar_df.empty:
        raise ValueError("Calendar lookup does not contain Hearst records.")

    calendar_df = calendar_df.sort_values("Calendar_Date", ascending=False).reset_index(drop=True)
    revenue_date = pd.to_datetime(calendar_df.loc[0, "Calendar_Date"])

    print(f"[RevenueDate] Using calendar date: {revenue_date}")

    result_df["Revenue Date"] = revenue_date

    return result_df
