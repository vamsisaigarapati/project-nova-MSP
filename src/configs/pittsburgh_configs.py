"""Pittsburgh pipeline configuration stubs."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from src.config import HEARST_RAW_DIR, HEASRT_FILE
from src.configs.common_configs import (
    aggregate_first_sum_by_group,
    assign_revenue_date_generic,
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