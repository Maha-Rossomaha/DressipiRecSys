from typing import Optional, List
import pandas as pd


def _apply_scores_merge(
    out_df: pd.DataFrame, 
    scores_df: pd.DataFrame, 
    keys: List[str], 
    tag: str
) -> pd.DataFrame:
    if scores_df is None or not isinstance(scores_df, pd.DataFrame) or scores_df.empty:
        print(f"[{tag}] scores_df is empty -> skip")
        return out_df

    join_keys = [k for k in keys if k in scores_df.columns and k in out_df.columns]
    if not join_keys:
        print(f"[{tag}] no common keys with RAW -> skip")
        return out_df

    cols_to_add = [c for c in scores_df.columns if c not in join_keys and c not in out_df.columns]
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
    
    print('')
    print('nm_ids', nm_ids)
    print('nm_date', nm_date)

    keys = (nm_ids if nm_ids is not None else []) + (nm_date if nm_date is not None else [])
    print('keys', keys)
    print('')
    
    for key in keys:
        if key not in df.columns:
            raise ValueError(f"Key column '{key}' not found in DataFrame")
    
    out_df1 = df
    out_df1 = _apply_scores_merge(out_df1, df_with_best_scores_g1, keys, tag="BEST_G1")
    out_df1 = _apply_scores_merge(out_df1, df_with_best_scores_g2, keys, tag="BEST_G2")

    has_prom = (
        (df_with_prom_scores_g1 is not None and isinstance(df_with_prom_scores_g1, pd.DataFrame) and not df_with_prom_scores_g1.empty)
        or
        (df_with_prom_scores_g2 is not None and isinstance(df_with_prom_scores_g2, pd.DataFrame) and not df_with_prom_scores_g2.empty)
    )

    out_df2 = None
    if has_prom:
        out_df2 = df
        out_df2 = _apply_scores_merge(out_df2, df_with_prom_scores_g1, keys, tag="PROM_G1")
        out_df2 = _apply_scores_merge(out_df2, df_with_prom_scores_g2, keys, tag="PROM_G2")
    else:
        print("[PROM] no prom dfs -> out_df2=None")

    print('Initial RAW df shape:', df.shape)
    print('Output Best Scores df shape:', out_df1.shape)
    if out_df2 is not None:
        print('Output RAW df shape:', out_df2.shape)
    else:
        print('Initial Prom Scores df was empty')
    print()

    return {
        "out_df1": out_df1,
        "out_df2": out_df2
    }