"""Pittsburgh processing pipeline."""

from __future__ import annotations

from dotenv import load_dotenv

from src.config import (
    COMMON_LOOKUP_DIR,
    MSP_WELCOME_BACK_FILE,
    MSP_STRATEGIC_FILE,
    PITTSBURGH_CLASS_LOOKUP_FILE,
    PITTSBURGH_FILE,
    PITTSBURGH_PROCESSED,
    PITTSBURGH_PROCESSED_FILE,
    PITTSBURGH_RAW_DIR,
    PITTSBURGH_LOOKUP_DIR,
    STRATEGIC_ORDERS_FILE,
)
from src.configs.pittsburgh_configs import (
    raw_column_types,
    sisense_columns,
    calculate_revenue,
    tag_welcome_back,
    tag_verified_strategic,
    assign_revenue_date,
    tag_msp_from_class_lookup,
    enforce_strategic_orders_lookup,
)
from src.utils.excel_file_operations import load_excel_file, write_df_to_excel
from src.utils.dataframe_utils import rearrange_columns

load_dotenv()


def main() -> None:
    """Entry point for the Pittsburgh pipeline."""
    partner_name = "Pittsburgh"
    print(f"Processing data for partner: {partner_name}")
    raw_df = load_excel_file(
        path=PITTSBURGH_RAW_DIR,
        file_name=PITTSBURGH_FILE,
        column_types=raw_column_types,
        sheet_name="Raw",
    )

    processed_df = calculate_revenue(raw_df)

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
    processed_df = tag_msp_from_class_lookup(
        processed_df,
        lookup_path=PITTSBURGH_LOOKUP_DIR,
        lookup_file_name=PITTSBURGH_CLASS_LOOKUP_FILE,
    )
    processed_df = assign_revenue_date(
        processed_df,
        partner_name=partner_name,
    )
    processed_df = rearrange_columns(processed_df, sisense_columns)
    output_path = write_df_to_excel(
        processed_df,
        path=PITTSBURGH_PROCESSED,
        file_name=PITTSBURGH_PROCESSED_FILE,
        sheet_name="Sisense",
    )

    print(f"✅ Wrote Pittsburgh processed file to {output_path}")


if __name__ == "__main__":
    main()
