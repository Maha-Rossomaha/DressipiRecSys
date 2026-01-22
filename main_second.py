from typing import List, Optional

import pandas as pd


def _apply_scores_merge(
    out_df: pd.DataFrame,
    scores_df: Optional[pd.DataFrame],
    keys: List[str],
    tag: str,
) -> pd.DataFrame:
    if scores_df is None or not isinstance(scores_df, pd.DataFrame) or scores_df.empty:
        print(f"[{tag}] scores_df is empty -> skip")
        return out_df

    join_keys = [k for k in keys if k in scores_df.columns and k in out_df.columns]
    if not join_keys:
        print(f"[{tag}] no common keys with RAW -> skip")
        return out_df

    cols_to_add = [c for c in scores_df.columns if c not in join_keys and c not in out_df.columns]
    if not cols_to_add:
        print(f"[{tag}] no new score/status columns -> skip")
        return out_df

    scores_part = scores_df[join_keys + cols_to_add]

    print(f"[{tag}] merge on keys: {join_keys}, add_cols: {len(cols_to_add)}")
    return out_df.merge(scores_part, on=join_keys, how="left")


def main(
    df: pd.DataFrame,
    nm_ids: List[str],
    nm_date: List[str],
    df_with_best_scores_g1: Optional[pd.DataFrame] = None,
    df_with_best_scores_g2: Optional[pd.DataFrame] = None,
    df_with_prom_scores_g1: Optional[pd.DataFrame] = None,
    df_with_prom_scores_g2: Optional[pd.DataFrame] = None
):
    
    print("")
    print("nm_ids", nm_ids)
    print("nm_date", nm_date)

    keys = (nm_ids if nm_ids is not None else []) + (nm_date if nm_date is not None else [])
    print("keys", keys)
    print("")
    
    for key in keys:
        if key not in df.columns:
            raise ValueError(f"Key column '{key}' not found in DataFrame")
    
    out_df1 = df.copy()
    out_df1 = _apply_scores_merge(out_df1, df_with_best_scores_g1, keys, tag="BEST_G1")
    out_df1 = _apply_scores_merge(out_df1, df_with_best_scores_g2, keys, tag="BEST_G2")

    out_df2 = df.copy()
    out_df2 = _apply_scores_merge(out_df2, df_with_prom_scores_g1, keys, tag="PROM_G1")
    out_df2 = _apply_scores_merge(out_df2, df_with_prom_scores_g2, keys, tag="PROM_G2")

    print("Initial RAW df shape:", df.shape)
    print("Output Best Scores df shape:", out_df1.shape)
    print("Output Prom Scores df shape:", out_df2.shape)
    print()

    return {
        "out_df1": out_df1,
        "out_df2": out_df2
    }
