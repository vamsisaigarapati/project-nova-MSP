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
HEASRT_FILE_SISENSE="Hearst Files Sisense.xlsx"
HEARST_LOOKUP_DIR=HEARST_DIR / "lookups"
MSP_AGENNT_LOOKUP_FILE="MappingFile-Agent Name.xlsx"
MSP_STRATEGIC_FILE="Strategic Account List.csv"
MSP_NOT_ASSIGNED_FILE_NAME="Not Assigned Reference List.csv"
MSP_WELCOME_BACK_FILE="Welcome Back List.csv"
MSP_CALENDAR_FILE="calendar.csv"

# Create directories if they don't exist
for directory in [
    DATA_DIR,HEARST_DIR,HEARST_RAW_DIR,HEARST_PROCESSED
]:
    directory.mkdir(parents=True, exist_ok=True)
