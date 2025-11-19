"""Boston processing pipeline."""

from __future__ import annotations

import pandas as pd
import pandas as pd
from dotenv import load_dotenv

from src.config import (
    BOSTON_FILE,
    BOSTON_IMMIGRATION_LOOKUP_FILE,
    BOSTON_LOOKUP_DIR,
    BOSTON_PROCESSED,
    BOSTON_PROCESSED_FILE,
    BOSTON_RAW_DIR,
    COMMON_LOOKUP_DIR,
    MSP_STRATEGIC_FILE,
    STRATEGIC_ORDERS_FILE,
)
from src.configs.boston_configs import (
    boston_sisense_columns,
    calculate_revenue,
    enforce_strategic_orders_lookup,
    tag_verified_strategic,
    update_immigration_flags,
)
from src.utils.excel_file_operations import write_df_to_excel
from src.utils.dataframe_utils import rearrange_columns

load_dotenv()


def main() -> None:
    """Entry point for the Boston pipeline."""
    partner_name = "Boston"
    print(f"Processing data for partner: {partner_name}")
    raw_path = BOSTON_RAW_DIR / BOSTON_FILE
    raw_df = pd.read_csv(raw_path, low_memory=False)

    processed_df = calculate_revenue(raw_df)
    processed_df = update_immigration_flags(
        processed_df,
        lookup_path=BOSTON_LOOKUP_DIR,
        lookup_file_name=BOSTON_IMMIGRATION_LOOKUP_FILE,
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
    if boston_sisense_columns:
        processed_df = rearrange_columns(processed_df, boston_sisense_columns)

    output_path = write_df_to_excel(
        processed_df,
        path=BOSTON_PROCESSED,
        file_name=BOSTON_PROCESSED_FILE,
        sheet_name="Processed",
    )
    print(f"✅ Wrote Boston processed file to {output_path}")


if __name__ == "__main__":
    main()
