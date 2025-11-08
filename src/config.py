import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Define directories
PARENT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PARENT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Client directory scaffolding
CLIENT_NAMES = ["hearst", "pittsburgh"]
CLIENT_DIRS = {}
for client in CLIENT_NAMES:
    base_dir = DATA_DIR / client
    raw_dir = base_dir / "raw"
    processed_dir = base_dir / "processed"
    lookup_dir = base_dir / "lookups"
    for path in (base_dir, raw_dir, processed_dir, lookup_dir):
        path.mkdir(parents=True, exist_ok=True)
    CLIENT_DIRS[client] = {
        "BASE": base_dir,
        "RAW": raw_dir,
        "PROCESSED": processed_dir,
        "LOOKUPS": lookup_dir,
    }

COMMON_LOOKUP_DIR = DATA_DIR / "common_lookups"
COMMON_LOOKUP_DIR.mkdir(parents=True, exist_ok=True)

# Hearst constants (backwards compatibility)
HEARST_DIR = CLIENT_DIRS["hearst"]["BASE"]
HEARST_RAW_DIR = CLIENT_DIRS["hearst"]["RAW"]
HEARST_PROCESSED = CLIENT_DIRS["hearst"]["PROCESSED"]
HEARST_LOOKUP_DIR = CLIENT_DIRS["hearst"]["LOOKUPS"]
HEASRT_FILE = "Hearst Files.xlsx"
HEASRT_FILE_SISENSE = "Hearst Files Sisense.xlsx"

# Pittsburgh placeholders (ready for future use)
PITTSBURGH_DIR = CLIENT_DIRS["pittsburgh"]["BASE"]
PITTSBURGH_RAW_DIR = CLIENT_DIRS["pittsburgh"]["RAW"]
PITTSBURGH_PROCESSED = CLIENT_DIRS["pittsburgh"]["PROCESSED"]
PITTSBURGH_LOOKUP_DIR = CLIENT_DIRS["pittsburgh"]["LOOKUPS"]

# Shared lookup filenames
MSP_AGENNT_LOOKUP_FILE = "MappingFile-Agent Name.xlsx"
MSP_STRATEGIC_FILE = "Strategic Account List.csv"
MSP_NOT_ASSIGNED_FILE_NAME = "Not Assigned Reference List.csv"
MSP_WELCOME_BACK_FILE = "Welcome Back List.csv"
MSP_REVENUE_DATE_FILE = "Revenue Date Calendar Reference.xlsx"
