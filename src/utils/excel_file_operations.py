# src/utils/load_file.py
"""
Generic Excel loader with safe dtype handling.

Usage:
    from src.utils.load_file import load_excel_file
    df = load_excel_file("/some/path", "My File.xlsx",
                         column_types=[{"Year": int}, {"First Issue Date": "datetime64[ns]"}],
                         sheet_name="Raw")
"""

from pathlib import Path
from typing import List, Dict, Optional, Union
import pandas as pd


def load_excel_file(
    path: Union[str, Path],
    file_name: str,
    *,
    column_types: Optional[List[Dict[str, object]]] = None,
    sheet_name: Optional[Union[str, int]] = None,
) -> pd.DataFrame:
    """
    Load an Excel file with optional dtype handling.

    Parameters
    ----------
    path : str | Path
        Directory containing the Excel file. (Required)
    file_name : str
        Excel file name (e.g., "Hearst Files.xlsx"). (Required)
    column_types : list[dict], optional
        List of one-key dicts mapping column -> desired dtype.
        Example: [{"Year": int}, {"First Issue Date": "datetime64[ns]"}, {"Revenue": float}]
        - int/float/str handled via dtype/converters
        - "datetime64[ns]" handled via parse_dates
    sheet_name : str | int | None, optional
        Sheet to read. If None, reads the first sheet (index 0).

    Returns
    -------
    pd.DataFrame
        Loaded DataFrame with safe conversions applied.

    Notes
    -----
    - Non-numeric junk in numeric columns is coerced to NaN.
    - Integer columns are finalized to nullable Int64 (preserves NaN).
    - If `column_types` is None, pandas infers types normally.
    """
    file_path = Path(path) / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")

    # Build dtype / parse_dates / converters from column_types (if provided)
    dtype_dict, parse_dates, converters = {}, [], {}
    if column_types:
        for d in column_types:
            (col, dtype), = d.items()
            if dtype == "datetime64[ns]":
                parse_dates.append(col)
            elif dtype == "date":
                converters[col] = lambda v: pd.to_datetime(v, errors="coerce").date() if pd.notna(v) else pd.NaT
            elif dtype is int:
                # per-cell converter -> numeric (NaN on failure); finalize to Int64 after read
                converters[col] = lambda v: pd.to_numeric(v, errors="coerce")
            elif dtype is float:
                converters[col] = lambda v: pd.to_numeric(v, errors="coerce")
            else:
                # str/object types
                dtype_dict[col] = dtype

    # Default to first sheet if not specified
    _sheet = 0 if sheet_name is None else sheet_name

    df = pd.read_excel(
        file_path,
        sheet_name=_sheet,
        dtype=(dtype_dict or None),
        parse_dates=(parse_dates or None),
        converters=(converters or None),
    )

    # Finalize integer columns to pandas nullable Int64
    if column_types:
        for d in column_types:
            (col, dtype), = d.items()
            if dtype is int and col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df




def write_df_to_excel(
    df: pd.DataFrame,
    path: str | Path,
    file_name: str,
    sheet_name: str = "Sisense",
    index: bool = False,
    mode: str = "w"
) -> Path:
    """
    Writes a pandas DataFrame to an Excel file.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to write.
    path : str | Path
        Directory where the file will be saved.
    file_name : str
        Name of the Excel file (e.g., 'output.xlsx').
    sheet_name : str, default 'Sheet1'
        Name of the sheet in the Excel workbook.
    index : bool, default False
        Whether to write the DataFrame index.
    mode : {'w', 'a'}, default 'w'
        File mode: 'w' = write (overwrite existing file),
                   'a' = append as a new sheet if file exists.

    Returns
    -------
    Path
        The full path to the written Excel file.

    Notes
    -----
    - Creates the directory if it doesn’t exist.
    - When mode='a', appends new sheet using openpyxl engine.
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / file_name
    engine = "openpyxl"

    # Handle writing or appending
    if mode == "a" and file_path.exists():
        with pd.ExcelWriter(file_path, mode="a", engine=engine, if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)
    else:
        with pd.ExcelWriter(file_path, mode="w", engine=engine) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)

    print(f"✅ DataFrame written successfully to: {file_path} (Sheet: '{sheet_name}')")
    return file_path
