# Standard library imports
import os
import sys
from pathlib import Path

# Third-party imports
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
# print(sys.path)
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))
# print(sys.path)

# Local application imports
from src.config import HEARST_DIR, HEARST_RAW_DIR, HEARST_PROCESSED, HEASRT_FILE, HEASRT_FILE_SISENSE,MSP_AGENNT_LOOKUP_FILE,HEARST_LOOKUP_DIR
from src.configs.hearst_configs import raw_column_types,sisense_columns
from src.utils.excel_file_operations import load_excel_file, write_df_to_excel
from src.utils.dataframe_utils import rearrange_columns



def calculate_revenue(raw_df):
    """
    Reads the 'Hearst Pub Market List' sheet from 'Heast Files.xlsx',
    joins with raw_df on 'Pub', creates 'Job Number +',
    and returns a DataFrame grouped by 'Job Number +' with summed Revenue.
    Output columns: ['Old Job Number', 'Job Number', 'Job Number +', 'Revenue'].
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

    # Merge Market info (inner join for exact matches)
    merged_df = merged_df.merge(
        market_list[["Pub_key", "Market"]],
        on="Pub_key",
        how="inner"
    )

    # Normalize columns for concatenation
    merged_df["Job Number"] = merged_df["Job Number"].astype(str).str.strip()
    merged_df["Market"] = merged_df["Market"].astype(str).str.strip()

    # Create "Job Number +" = Market + Job Number
    merged_df["Job Number +"] = merged_df.apply(
        lambda r: f"{r['Market']}{r['Job Number']}"
        if r["Market"] not in ["", "nan", "None"]
        else r["Job Number"],
        axis=1
    )

    # Convert Revenue safely to numeric
    merged_df["Sum of 'Revenue'"] = pd.to_numeric(merged_df["Revenue"], errors="coerce").fillna(0)

    # ---- Aggregate ----
    # Keep first record for every column except the revenue column
    agg_dict = {col: "first" for col in merged_df.columns if col not in ["Sum of 'Revenue'"]}
    agg_dict["Sum of 'Revenue'"] = "sum"

    result_df = merged_df.groupby("Job Number +", as_index=False).agg(agg_dict)

    # Add the count of records per group
    counts = merged_df.groupby("Job Number +").size().reset_index(name="Count of matches")
    result_df = result_df.merge(counts, on="Job Number +", how="left")
    print(result_df.columns)

    # Reorder columns — keep Job Number +, Sum of Revenue, Count of matches at the end
    cols = [c for c in result_df.columns if c not in ["Job Number +", "Sum of 'Revenue'", "Count of matches"]]
    result_df = result_df[cols + ["Job Number +", "Sum of 'Revenue'", "Count of matches"]]
    result_df['Job Number +'], result_df['Job Number'] = result_df['Job Number'].copy(), result_df['Job Number +'].copy()

    result_df = result_df[result_df["Sum of 'Revenue'"] != 0.0]
    return result_df


def tag_msp_from_rep(processed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds an 'MSP/non-MSP' flag to processed_df by joining with rep_list.

    Logic:
    1. Load rep_list (MSP agent lookup sheet).
    2. Create lowercase helper columns for matching.
    3. Filter rep_list where 'System(s)' contains 'hearst'
       and exclude agent 'wave2, wave2'.
    4. Deduplicate by Agent Names (first occurrence only).
    5. Left join processed_df.Full Name LF with rep_list.Agent Names (lowercase helper columns).
    6. Add 'MSP/non-MSP' = 'MSP' if match found, else 'Non-MSP'.
    7. Drop helper columns to return original unmodified columns.

    Returns:
        pd.DataFrame: processed_df with a new column 'MSP/non-MSP'
    """

    # --- Step 1: Load the rep_list from Excel lookup ---
    rep_list = load_excel_file(
        path=HEARST_LOOKUP_DIR,
        file_name=MSP_AGENNT_LOOKUP_FILE,
        sheet_name="All Rep Names",
    )

    # --- Step 2: Create lowercase helper columns (preserve originals) ---
    rep_list["_system_lower"] = rep_list["System(s)"].astype(str).str.lower().str.strip()
    rep_list["_agent_lower"] = rep_list["Agent Names"].astype(str).str.lower().str.strip()
    rep_list["_fullname_lower"] = rep_list["Full Name"].astype(str).str.lower().str.strip()

    processed_df["_fullname_lf_lower"] = processed_df["Full Name LF"].astype(str).str.lower().str.strip()

    # --- Step 3: Filter rep_list for 'hearst' but exclude agent 'wave2, wave2' ---
    rep_filtered = rep_list[
        rep_list["_system_lower"].str.contains("hearst", na=False)
        & (rep_list["_agent_lower"] != "wave2, wave2")
    ].copy()

    # --- Step 4: Keep only relevant columns & deduplicate by Agent Name ---
    rep_filtered = (
        rep_filtered[["Agent Names", "System(s)", "Full Name", "_agent_lower"]]
        .drop_duplicates(subset=["_agent_lower"], keep="first")
        .reset_index(drop=True)
    )

    print(f"Filtered rep_list to {len(rep_filtered)} records for 'hearst' system (excluding 'wave2, wave2').")

    # --- Step 5: Perform left join on lowercase helper columns ---
    merged = processed_df.merge(
        rep_filtered[["_agent_lower"]].rename(columns={"_agent_lower": "_join_key"}),
        how="left",
        left_on="_fullname_lf_lower",
        right_on="_join_key",
        indicator=True
    )

    print(f"Merged DataFrame has {len(merged)} records after join.")

    # --- Step 6: Add MSP flag ---
    merged["MSP/non-MSP"] = merged["_merge"].map({
        "both": "MSP",
        "left_only": "Non-MSP"
    })

    # --- Step 7: Cleanup helper columns ---
    merged = merged.drop(columns=["_fullname_lf_lower", "_join_key", "_merge"], errors="ignore")

    return merged



def main():
    """
    Main entry point: loads the 'Raw' sheet from Hearst Files.xlsx
    with proper type handling and prints a quick preview.
    """
    partner_name = "Hearst"
    print(f"Processing data for partner: {partner_name}")
    raw_df = load_excel_file(
        path=HEARST_RAW_DIR,                 # or "/full/path/to/dir"
        file_name=HEASRT_FILE,
        column_types=raw_column_types,
        sheet_name="Raw",                    # or omit to read the first sheet
    )
    # write_df_to_excel(raw_df, HEARST_PROCESSED, "checking.xlsx", sheet_name="Sisense")
    processed_df=calculate_revenue(raw_df)
    processed_df= tag_msp_from_rep(processed_df)
    processed_df = rearrange_columns(processed_df, sisense_columns)
    write_df_to_excel(processed_df, HEARST_PROCESSED, HEASRT_FILE_SISENSE, sheet_name="Sisense")


if __name__ == "__main__":
    main()