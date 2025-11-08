"""Placeholder pipeline for Pittsburgh client."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from src.config import (
    PITTSBURGH_RAW_DIR,
    PITTSBURGH_PROCESSED,
)

load_dotenv()


def main() -> None:
    """Entry point for Pittsburgh pipeline placeholder."""
    client = "Pittsburgh"
    print(f"Pipeline for {client} is not yet implemented.")
    print(f"Raw dir: {PITTSBURGH_RAW_DIR}")
    print(f"Processed dir: {PITTSBURGH_PROCESSED}")
    print("Please implement processing logic in src/configs/pittsburgh_configs.py")


if __name__ == "__main__":
    main()
