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
from src.config import HEARST_DIR, HEARST_RAW_DIR, HEARST_PROCESSED, HEASRT_FILE
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
    counts = merged_df.groupby("Job Number +").size().reset_index(name="Count of Matches")
    result_df = result_df.merge(counts, on="Job Number +", how="left")

    # Reorder columns — keep Job Number +, Sum of Revenue, Count of Matches at the end
    cols = [c for c in result_df.columns if c not in ["Job Number +", "Sum of 'Revenue'", "Count of Matches"]]
    result_df = result_df[cols + ["Job Number +", "Sum of 'Revenue'", "Count of Matches"]]

    return result_df

def main():
    """
    Main entry point: loads the 'Raw' sheet from Hearst Files.xlsx
    with proper type handling and prints a quick preview.
    """

    raw_df = load_excel_file(
        path=HEARST_RAW_DIR,                 # or "/full/path/to/dir"
        file_name=HEASRT_FILE,
        column_types=raw_column_types,
        sheet_name="Raw",                    # or omit to read the first sheet
    )
    processed_df=calculate_revenue(raw_df)
    processed_df = rearrange_columns(processed_df, sisense_columns)
    write_df_to_excel(processed_df, HEARST_PROCESSED, HEASRT_FILE, sheet_name="Sisense")


if __name__ == "__main__":
    main()