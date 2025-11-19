# Standard library imports
import os
import sys
from pathlib import Path

# Third-party imports
from dotenv import load_dotenv

load_dotenv()
# print(sys.path)
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))
# print(sys.path)

# Local application imports
from src.config import (
    HEARST_RAW_DIR,
    HEARST_PROCESSED,
    HEASRT_FILE,
    HEASRT_FILE_SISENSE,
    COMMON_LOOKUP_DIR,
    MSP_AGENNT_LOOKUP_FILE,
    MSP_NOT_ASSIGNED_FILE_NAME,
    MSP_STRATEGIC_FILE,
    MSP_WELCOME_BACK_FILE,
    MSP_REVENUE_DATE_FILE,
    STRATEGIC_ORDERS_FILE,
)
from src.configs.hearst_configs import (
    raw_column_types,
    sisense_columns,
    calculate_revenue,
    tag_msp_from_rep,
    enrich_with_msp_reference,
    tag_verified_strategic,
    tag_welcome_back,
    assign_revenue_date,
    enforce_strategic_orders_lookup,
)
from src.utils.excel_file_operations import load_excel_file, write_df_to_excel
from src.utils.dataframe_utils import rearrange_columns



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
    processed_df = calculate_revenue(raw_df)
    processed_df = tag_msp_from_rep(
        processed_df,
        lookup_path=COMMON_LOOKUP_DIR,
        lookup_file_name=MSP_AGENNT_LOOKUP_FILE,
        lookup_sheet_name="All Rep Names",
        processed_name_column="Full Name LF",
        partner_name=partner_name,
    )
    processed_df = enrich_with_msp_reference(
        processed_df,
        lookup_path=COMMON_LOOKUP_DIR,
        lookup_file_name=MSP_NOT_ASSIGNED_FILE_NAME,
        lookup_sheet_name="Not Assigned Reference List",
    )
    processed_df = tag_verified_strategic(
        processed_df,
        lookup_path=COMMON_LOOKUP_DIR,
        strategic_file_name=MSP_STRATEGIC_FILE,
        sheet_name="Strategic Account List",
        partner_name=partner_name,
    )
    processed_df = enforce_strategic_orders_lookup(
        processed_df,
        lookup_path=COMMON_LOOKUP_DIR,
        lookup_file_name=STRATEGIC_ORDERS_FILE,
        partner_name=partner_name,
    )
    processed_df = tag_welcome_back(
        processed_df,
        lookup_path=COMMON_LOOKUP_DIR,
        welcome_back_file=MSP_WELCOME_BACK_FILE,
        sheet_name="Welcome Back List",
        partner_name=partner_name,
    )
    processed_df = assign_revenue_date(
        processed_df,
        lookup_path=COMMON_LOOKUP_DIR,
        calendar_file=MSP_REVENUE_DATE_FILE,
        partner_name=partner_name,
        calendar_year_or_not=False,
    )
    processed_df = rearrange_columns(processed_df, sisense_columns)
    write_df_to_excel(processed_df, HEARST_PROCESSED, HEASRT_FILE_SISENSE, sheet_name="Sisense")


if __name__ == "__main__":
    main()
