import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Define directories
PARENT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PARENT_DIR / "data"
HEARST_DIR = DATA_DIR / "hearst"
HEARST_RAW_DIR = HEARST_DIR / "raw"
HEARST_PROCESSED= HEARST_DIR / "processed"
HEASRT_FILE="Hearst Files.xlsx"

# Create directories if they don't exist
for directory in [
    DATA_DIR,HEARST_DIR,HEARST_RAW_DIR,HEARST_PROCESSED
]:
    directory.mkdir(parents=True, exist_ok=True)