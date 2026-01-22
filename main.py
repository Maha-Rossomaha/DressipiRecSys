from processing import process_dataframes
import pandas as pd


def _drop_key_duplicates(sample_df: pd.DataFrame, keys: list, sample_name: str) -> pd.DataFrame:
    non_key_columns = [col for col in sample_df.columns if col not in keys]
    if non_key_columns:
        null_metrics_mask = sample_df[non_key_columns].isna().all(axis=1)
        if null_metrics_mask.any():
            null_metrics_df = sample_df[null_metrics_mask]
            null_metrics_df = null_metrics_df.drop_duplicates(subset=keys)
            sample_df = (
                pd.concat([sample_df[~null_metrics_mask], null_metrics_df])
                .sort_index()
                .reset_index(drop=True)
            )

    duplicate_by_keys = sample_df.duplicated(subset=keys).sum()
    duplicate_by_all = sample_df.duplicated().sum()
    if duplicate_by_all:
        raise ValueError(
            "Found duplicate rows for sample "
            f"{sample_name}. Duplicates by keys: {duplicate_by_keys}."
        )
    if duplicate_by_keys:
        sample_df = sample_df.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)
    return sample_df


def _validate_missing_keys(sample_df: pd.DataFrame, keys: list, sample_name: str) -> None:
    if not keys:
        return
    missing_mask = sample_df[keys].isna().any(axis=1)
    if missing_mask.any():
        missing_counts = sample_df.loc[missing_mask, keys].isna().sum().to_dict()
        raise ValueError(
            "Found missing values in key columns for sample "
            f"{sample_name}. Missing counts: {missing_counts}."
        )


def main(roles: dict, **kwargs):
    possible_keys = [k for k, v in roles.items() if v[0] in ["id", "date"]]

    print('Kwargs input:')
    for k, v in kwargs.items(): 
        print(k, type(v))
    
    samples = {
        k: v for k, v in kwargs.items()
        if k.startswith("in") and isinstance(v, pd.DataFrame) and not v.empty
    }

    groups = {}
    for name, df in samples.items():
        keys = tuple(sorted(set(possible_keys) & set(df.columns)))
        if not keys:
            print("DF without keys was found")
            continue
        _validate_missing_keys(df, list(keys), name)
        df = _drop_key_duplicates(df, list(keys), name)
        groups.setdefault(keys, {})[name] = df

    group_items = list(groups.items())
    tmp_elem = ' '.join([str(el[0]) for el in group_items])
    assert len(group_items) <= 2, 'Too much groups were found: ' + tmp_elem

    outputs = {
        "out_df1_g1": None,
        "out_df2_g1": None,
        "out_df1_g2": None,
        "out_df2_g2": None
    }
    for idx, (group_keys, group_samples) in enumerate(group_items, start=1):
        out_df1, out_df2 = process_dataframes(group_samples, list(group_keys))

        outputs[f"out_df1_g{idx}"] = out_df1
        outputs[f"out_df2_g{idx}"] = out_df2

    return outputs
