"""Houston processing pipeline."""

from __future__ import annotations

from dotenv import load_dotenv

from src.config import (
    HOUSTON_FILE,
    HOUSTON_PROCESSED,
    HOUSTON_PROCESSED_FILE,
    HOUSTON_RAW_DIR,
)
from src.configs.houston_configs import (
    calculate_revenue,
    houston_raw_column_types,
    houston_sisense_columns,
)
from src.utils.excel_file_operations import load_excel_file, write_df_to_excel
from src.utils.dataframe_utils import rearrange_columns

load_dotenv()


def main() -> None:
    """Entry point for the Houston pipeline."""
    partner_name = "Houston"
    print(f"Processing data for partner: {partner_name}")
    raw_df = load_excel_file(
        path=HOUSTON_RAW_DIR,
        file_name=HOUSTON_FILE,
        column_types=houston_raw_column_types,
    )

    processed_df = calculate_revenue(raw_df)
    if houston_sisense_columns:
        processed_df = rearrange_columns(processed_df, houston_sisense_columns)

    output_path = write_df_to_excel(
        processed_df,
        path=HOUSTON_PROCESSED,
        file_name=HOUSTON_PROCESSED_FILE,
        sheet_name="Processed",
    )
    print(f"✅ Wrote Houston processed file to {output_path}")


if __name__ == "__main__":
    main()
