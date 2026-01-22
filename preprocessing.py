from typing import Dict, List, Optional, Tuple

import pandas as pd


VERSION_SUFFIXES = ("new", "prom")


def split_metric_column(column_name: str) -> Optional[Tuple[str, str, str]]:
    """Разбирает имя колонки на (kind, base_name, version)."""
    for suffix in VERSION_SUFFIXES:
        suffix_token = f"_{suffix}"
        if column_name.endswith(suffix_token):
            base = column_name[: -len(suffix_token)]
            if base.startswith("status_"):
                return "status", base[len("status_") :], suffix
            return "score", base, suffix
    return None


def build_metric_index(
    sample_df: pd.DataFrame, keys: List[str]
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Готовит индекс метрик по базе/версии для одного датафрейма."""
    metrics: Dict[str, Dict[str, Dict[str, str]]] = {}
    for col in sample_df.columns:
        if col in keys:
            continue
        parsed = split_metric_column(col)
        if not parsed:
            continue
        kind, base, version = parsed
        metrics.setdefault(base, {}).setdefault(version, {})[kind] = col
    return metrics


def process_dataframes(
    samples_dict: Dict[str, pd.DataFrame], 
    keys: List[str]
) -> Tuple[pd.DataFrame]: 
    """
    Получает словарь датафреймов
    Возвращает первый датафрейм: все скоры лучших моделей (если были), иначе prom
    Второй датафрейм (опционально): если в каждом датафрейме есть prom скоры, то их
    """
    df1_frames = []
    df2_frames = []

    all_bases = set()
    prom_bases = set()

    sample_metrics = {}
    for sample_name, sample_df in samples_dict.items():
        metrics = build_metric_index(sample_df, keys)
        sample_metrics[sample_name] = metrics
        for base, versions in metrics.items():
            if "new" in versions or "prom" in versions:
                all_bases.add(base)
            if "prom" in versions:
                prom_bases.add(base)

    has_full_prom = bool(all_bases) and all_bases.issubset(prom_bases)

    for sample_name, sample_df in samples_dict.items():
        metrics = sample_metrics[sample_name]
        df1_cols = []
        df1_rename = {}
        df2_cols = []
        df2_rename = {}

        for base, versions in metrics.items():
            chosen_version = "new" if "new" in versions else "prom" if "prom" in versions else None
            if chosen_version:
                score_col = versions[chosen_version].get("score")
                status_col = versions[chosen_version].get("status")
                if score_col:
                    df1_cols.append(score_col)
                    df1_rename[score_col] = base
                if status_col:
                    df1_cols.append(status_col)
                    df1_rename[status_col] = f"status_{base}"

            if has_full_prom and "prom" in versions:
                score_col = versions["prom"].get("score")
                status_col = versions["prom"].get("status")
                if score_col:
                    df2_cols.append(score_col)
                    df2_rename[score_col] = base
                if status_col:
                    df2_cols.append(status_col)
                    df2_rename[status_col] = f"status_{base}"

        if df1_cols:
            temp_df = sample_df[keys + df1_cols].copy().rename(columns=df1_rename)
            df1_frames.append(temp_df)

        if df2_cols:
            temp_df = sample_df[keys + df2_cols].copy().rename(columns=df2_rename)
            df2_frames.append(temp_df)

    def merge_frames(frames: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if not frames:
            return None
        merged = frames[0]
        for frame in frames[1:]:
            overlap = set(merged.columns) & set(frame.columns) - set(keys)
            if overlap:
                frame = frame.drop(columns=sorted(overlap))
            if len(frame.columns) == len(keys):
                continue
            merged = merged.merge(frame, on=keys, how="inner")
        return merged

    merged_extra_1 = merge_frames(df1_frames)
    merged_extra_2 = merge_frames(df2_frames) if has_full_prom else None

    return merged_extra_1, merged_extra_2
