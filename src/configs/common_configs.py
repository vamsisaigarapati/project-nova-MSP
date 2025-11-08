from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd

from src.utils.excel_file_operations import load_excel_file


def aggregate_first_sum_by_group(
    df: pd.DataFrame,
    *,
    group_column: str,
    value_column: str,
    count_column_name: str = "Count of matches",
) -> pd.DataFrame:
    """
    Group a DataFrame, summing ``value_column`` and carrying the first record for other fields.
    """
    if group_column not in df.columns:
        raise KeyError(f"Group column '{group_column}' missing from DataFrame")
    if value_column not in df.columns:
        raise KeyError(f"Value column '{value_column}' missing from DataFrame")

    agg_source = df.copy()

    agg_dict = {
        col: "first"
        for col in agg_source.columns
        if col not in (value_column, group_column)
    }
    agg_dict[value_column] = "sum"

    grouped = agg_source.groupby(group_column, as_index=False).agg(agg_dict)
    counts = (
        agg_source.groupby(group_column)
        .size()
        .reset_index(name=count_column_name)
    )

    merged = grouped.merge(counts, on=group_column, how="left")

    ordered_cols = [
        c
        for c in merged.columns
        if c not in (group_column, value_column, count_column_name)
    ]
    ordered_cols += [group_column, value_column, count_column_name]

    return merged[ordered_cols]


def tag_msp_from_rep(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Path | str,
    lookup_file_name: str,
    lookup_sheet_name: str,
    processed_name_column: str,
    partner_name: str,
) -> pd.DataFrame:
    """
    Adds MSP tagging by matching processed names to the lookup.
    """
    if processed_name_column not in processed_df.columns:
        raise KeyError(f"Processed DataFrame missing column: {processed_name_column}")

    rep_list = load_excel_file(
        path=lookup_path,
        file_name=lookup_file_name,
        sheet_name=lookup_sheet_name,
    )

    partner_lower = partner_name.casefold()
    rep_list["_system_lower"] = rep_list["System(s)"].astype(str).str.casefold().str.strip()
    rep_list["_agent_lower"] = rep_list["Agent Names"].astype(str).str.casefold().str.strip()

    processed_df = processed_df.copy()
    processed_df["_processed_name_lower"] = (
        processed_df[processed_name_column].astype(str).str.casefold().str.strip()
    )

    rep_filtered = rep_list[
        rep_list["_system_lower"].str.contains(partner_lower, na=False)
        & (rep_list["_agent_lower"] != "wave2, wave2")
    ].copy()
    rep_filtered = rep_filtered.drop_duplicates(subset=["_agent_lower"], keep="first").reset_index(drop=True)

    merged = processed_df.merge(
        rep_filtered[["_agent_lower"]].rename(columns={"_agent_lower": "_join_key"}),
        how="left",
        left_on="_processed_name_lower",
        right_on="_join_key",
        indicator=True,
    )

    merged["MSP/non-MSP"] = merged["_merge"].map({"both": "MSP", "left_only": "Non-MSP"})
    merged = merged.drop(columns=["_processed_name_lower", "_join_key", "_merge"], errors="ignore")
    return merged


def tag_verified_strategic_generic(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Path | str,
    strategic_file_name: str,
    sheet_name: str,
    partner_name: str,
    processed_lookup_columns: Sequence[tuple[str, str]],
    processed_date_column: str = "First Issue Date",
    lookup_date_column: str = "Strategic End Date",
    company_column: str = "Company",
    output_column: str = "Verified Strategic",
    strategic_date_output_column: str = "_strategic_date",
    diagnostics_prefix: str = "[Strategic]",
) -> pd.DataFrame:
    """
    Generic strategic tagging helper that sequentially matches processed columns to lookup columns.
    """
    if not processed_lookup_columns:
        raise ValueError("processed_lookup_columns must contain at least one mapping")

    required_processed = {processed_date_column, *[c for c, _ in processed_lookup_columns]}
    missing_processed = required_processed - set(processed_df.columns)
    if missing_processed:
        raise KeyError(f"Processed DataFrame missing columns: {', '.join(sorted(missing_processed))}")

    lookup_df = load_excel_file(
        path=lookup_path,
        file_name=strategic_file_name,
        sheet_name=sheet_name,
    )

    required_lookup = {lookup_date_column, company_column, *[c for _, c in processed_lookup_columns]}
    missing_lookup = required_lookup - set(lookup_df.columns)
    if missing_lookup:
        raise KeyError(f"Lookup DataFrame missing columns: {', '.join(sorted(missing_lookup))}")

    lookup_df = lookup_df.copy()
    partner_lower = partner_name.casefold()
    lookup_df[company_column] = lookup_df[company_column].astype(str)
    lookup_df = lookup_df[
        lookup_df[company_column].str.casefold().str.contains(partner_lower, na=False)
    ]
    lookup_df[lookup_date_column] = pd.to_datetime(lookup_df[lookup_date_column], errors="coerce")
    lookup_df = lookup_df.dropna(subset=[lookup_date_column])

    if lookup_df.empty:
        raise ValueError(f"{diagnostics_prefix} Lookup does not contain usable rows for partner '{partner_name}'.")

    result_df = processed_df.copy()

    def _normalize(series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.strip()
            .str.casefold()
            .str.replace(r"\.0+$", "", regex=True)
        )

    normalized_processed = {
        col: _normalize(result_df[col])
        for col, _ in processed_lookup_columns
    }
    for _, lookup_col in processed_lookup_columns:
        norm_col = f"_norm_{lookup_col}"
        lookup_df[norm_col] = _normalize(lookup_df[lookup_col])

    strategic_dates = pd.Series(pd.NaT, index=result_df.index, dtype="datetime64[ns]")

    print(f"{diagnostics_prefix} Lookup rows after filtering {partner_name}: {len(lookup_df)}")
    for processed_col, lookup_col in processed_lookup_columns:
        remaining_mask = strategic_dates.isna()
        if not remaining_mask.any():
            break
        norm_lookup_col = f"_norm_{lookup_col}"
        lookup_map = (
            lookup_df[[norm_lookup_col, lookup_date_column]]
            .dropna(subset=[norm_lookup_col])
            .drop_duplicates(subset=[norm_lookup_col], keep="first")
            .set_index(norm_lookup_col)[lookup_date_column]
        )
        mapped = normalized_processed[processed_col].loc[remaining_mask].map(lookup_map)
        strategic_dates.loc[remaining_mask] = mapped.combine_first(strategic_dates.loc[remaining_mask])
        print(
            f"{diagnostics_prefix} Matches via {processed_col} → {lookup_col}: {int(mapped.notna().sum())}"
        )

    result_df[strategic_date_output_column] = strategic_dates

    first_issue = pd.to_datetime(result_df[processed_date_column], errors="coerce")
    valid_mask = first_issue.notna() & strategic_dates.notna()
    verified_mask = valid_mask & (first_issue < strategic_dates)

    result_df[output_column] = 0
    result_df.loc[verified_mask, output_column] = 1

    print(f"{diagnostics_prefix} Rows with usable dates: {int(valid_mask.sum())}")
    print(f"{diagnostics_prefix} Rows flagged as {output_column}: {int(verified_mask.sum())}")

    return result_df


def tag_welcome_back_generic(
    processed_df: pd.DataFrame,
    *,
    lookup_path: Path | str,
    welcome_back_file: str,
    sheet_name: str,
    partner_name: str,
    processed_order_column: str,
    processed_date_column: str,
    lookup_order_column: str = "Order Number",
    lookup_company_column: str = "Company",
    lookup_date_column: str = "Welcome Back End Date",
    output_column: str = "Welcome Back",
    diagnostics_prefix: str = "[WelcomeBack]",
) -> pd.DataFrame:
    """
    Generic helper to mark Welcome Back rows by matching order numbers and comparing dates.
    """
    required_processed = {processed_order_column, processed_date_column}
    missing_processed = required_processed - set(processed_df.columns)
    if missing_processed:
        raise KeyError(f"Processed DataFrame missing columns: {', '.join(sorted(missing_processed))}")

    lookup_df = load_excel_file(
        path=lookup_path,
        file_name=welcome_back_file,
        sheet_name=sheet_name,
    )

    required_lookup = {lookup_order_column, lookup_company_column, lookup_date_column}
    missing_lookup = required_lookup - set(lookup_df.columns)
    if missing_lookup:
        raise KeyError(f"Welcome Back lookup missing columns: {', '.join(sorted(missing_lookup))}")

    partner_lower = partner_name.casefold()
    lookup_df = lookup_df.copy()
    lookup_df[lookup_company_column] = lookup_df[lookup_company_column].astype(str)
    lookup_df = lookup_df[
        lookup_df[lookup_company_column].str.casefold().str.contains(partner_lower, na=False)
    ]
    lookup_df[lookup_order_column] = lookup_df[lookup_order_column].astype(str).str.strip().str.casefold()
    lookup_df[lookup_date_column] = pd.to_datetime(lookup_df[lookup_date_column], errors="coerce")
    lookup_df = lookup_df.dropna(subset=[lookup_date_column])

    if lookup_df.empty:
        raise ValueError(f"{diagnostics_prefix} Lookup does not contain usable rows for partner '{partner_name}'.")

    wb_map = (
        lookup_df[[lookup_order_column, lookup_date_column]]
        .drop_duplicates(subset=[lookup_order_column], keep="first")
        .set_index(lookup_order_column)[lookup_date_column]
    )

    result_df = processed_df.copy()
    result_df["_order_key"] = (
        result_df[processed_order_column].astype(str).str.strip().str.casefold()
    )

    mapped_dates = result_df["_order_key"].map(wb_map)
    first_issue = pd.to_datetime(result_df[processed_date_column], errors="coerce")
    valid_mask = first_issue.notna() & mapped_dates.notna()
    welcome_mask = valid_mask & (first_issue < mapped_dates)

    print(f"{diagnostics_prefix} Lookup rows after filtering {partner_name}: {len(lookup_df)}")
    print(f"{diagnostics_prefix} Matches via {processed_order_column}: {int(mapped_dates.notna().sum())}")
    print(f"{diagnostics_prefix} Rows with usable dates: {int(valid_mask.sum())}")
    print(f"{diagnostics_prefix} Rows flagged as {output_column}: {int(welcome_mask.sum())}")

    result_df[output_column] = 0
    result_df.loc[welcome_mask, output_column] = 1

    result_df.drop(columns=["_order_key"], inplace=True)

    return result_df


def assign_revenue_date_generic(
    processed_df: pd.DataFrame,
    *,
    calendar_year_or_not: bool,
    partner_name: str,
    period_column: str,
    lookup_path: Path | str | None = None,
    calendar_file: str | None = None,
    sheet_name: str | None = None,
    calendar_period_candidates: Sequence[str] = ("Period #", "Period", "Period#", "Period Num"),
    output_column: str = "Revenue Date",
) -> pd.DataFrame:
    """
    Assign Revenue Date either from the first day of the current month or via a partner-specific calendar lookup.
    """
    if period_column not in processed_df.columns:
        raise KeyError(f"Processed DataFrame missing required column: {period_column}")

    result_df = processed_df.copy()

    def _format_date(ts: pd.Timestamp) -> str:
        return f"{ts.month}/{ts.day}/{str(ts.year)[-2:]}"

    if calendar_year_or_not:
        current = pd.Timestamp.today().normalize().replace(day=1)
        formatted = _format_date(current)
        result_df[output_column] = formatted
        return result_df

    if lookup_path is None or calendar_file is None:
        raise ValueError("lookup_path and calendar_file must be provided when calendar_year_or_not is False.")

    calendar_df = load_excel_file(
        path=lookup_path,
        file_name=calendar_file,
        sheet_name=sheet_name,
    )

    calendar_df = calendar_df.copy()
    calendar_df.columns = [str(col).strip() for col in calendar_df.columns]

    period_col = None
    for candidate in calendar_period_candidates:
        if candidate in calendar_df.columns:
            period_col = candidate
            break
    if period_col is None:
        raise KeyError("Calendar DataFrame missing a period column.")

    partner_lower = partner_name.casefold()
    partner_columns = [
        col
        for col in calendar_df.columns
        if partner_lower in col.casefold()
    ]
    if not partner_columns:
        raise KeyError(f"Calendar DataFrame does not contain a column for partner '{partner_name}'.")

    partner_column = partner_columns[0]

    def _normalize(series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.strip()
            .str.casefold()
            .str.replace(r"\.0+$", "", regex=True)
        )

    calendar_df["_period_key"] = _normalize(calendar_df[period_col])
    calendar_df = calendar_df[calendar_df["_period_key"].ne("")]
    calendar_df[partner_column] = pd.to_datetime(calendar_df[partner_column], errors="coerce")
    calendar_df = calendar_df.dropna(subset=[partner_column])

    if calendar_df.empty:
        raise ValueError("Calendar lookup does not contain usable dates.")

    period_map = (
        calendar_df.drop_duplicates("_period_key", keep="last")
        .set_index("_period_key")[partner_column]
    )

    processed_period_key = _normalize(result_df[period_column])
    mapped_dates = processed_period_key.map(period_map)

    current_year = pd.Timestamp.today().year
    adjusted_dates = mapped_dates.apply(
        lambda dt: dt.replace(year=current_year) if pd.notna(dt) else pd.NaT
    )

    formatted_dates = adjusted_dates.apply(
        lambda dt: _format_date(dt) if pd.notna(dt) else pd.NA
    )

    matched_count = adjusted_dates.notna().sum()
    print(f"[RevenueDate] Calendar rows: {len(calendar_df)}")
    print(f"[RevenueDate] Periods matched: {matched_count} / {len(result_df)}")

    result_df[output_column] = formatted_dates

    return result_df
