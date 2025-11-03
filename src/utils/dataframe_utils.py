from typing import List

import pandas as pd


def rearrange_columns(df: pd.DataFrame, column_order: List[str]) -> pd.DataFrame:
    """
    Rearranges DataFrame columns according to a given list,
    skipping any columns that are not present in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame whose columns should be rearranged.
    column_order : list[str]
        The desired column order. Columns missing in the DataFrame
        are ignored safely.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with columns reordered as requested.

    Example
    -------
    >>> df = pd.DataFrame({
    ...     "B": [2, 3],
    ...     "A": [1, 2],
    ...     "C": [5, 6]
    ... })
    >>> rearrange_columns(df, ["A", "B", "D"])
       A  B  C
    0  1  2  5
    1  2  3  6
    """
    existing_columns = [col for col in column_order if col in df.columns]
    # remaining_columns = [col for col in df.columns if col not in existing_columns]
    # reordered_df = df[existing_columns + remaining_columns]
    reordered_df = df[existing_columns]

    return reordered_df
