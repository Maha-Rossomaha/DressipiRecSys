from typing import Dict, List, Tuple
import pandas as pd
import re


def extract_suffix(metric_name):
    """Извлекает суффикс из названия метрики."""
    # Для обычных метрик: mv123456_pd_bki_new → pd_bki
    pattern = r'^(.+)_(new|prom)$'
    # Для статусов: status_mv123456_pd_bki_new → pd_bki  
    status_pattern = r'^status_(.+)_(new|prom)$'
    
    match = re.match(pattern, metric_name) or re.match(status_pattern, metric_name)
    return match.group(1) if match else None


def process_dataframes(
    samples_dict: Dict[str, pd.DataFrame], 
    keys: List[str]
) -> Tuple[pd.DataFrame]: 
    """
    Получает словарь датафреймов
    Возвращает первый датафрейм: все скоры лучших моделей (если были), иначе prom
    Второй датафрейм (опционально): если в каждом датафрейме есть prom скоры, то их
    """
    df1_columns_info = []
    df2_columns_info = []
    
    all_new_scores = set()
    all_found_prom_scores = set()
    
    new_suffixes = set()
    prom_suffixes = set()
    
    # Сначала собираем информацию о всех метриках
    for sample_name, sample_df in samples_dict.items():
        metric_columns = [col for col in sample_df.columns if col not in keys]
        
        # Новые скоры
        new_scores = [col for col in metric_columns if col.endswith('_new') and not col.startswith('status')]
        for score in new_scores:
            suffix = extract_suffix(score)
            if suffix:
                new_suffixes.add(suffix)
                all_new_scores.add(score)
                
        # Пром скоры - УБРАТЬ УСЛОВИЕ col.startswith('mv')
        prom_scores = [col for col in metric_columns if col.endswith('_prom')]  # Изменено здесь
        for score in prom_scores:
            suffix = extract_suffix(score)
            if suffix:
                prom_suffixes.add(suffix)
                all_found_prom_scores.add(score)
    
    print(f"new_suffixes: {new_suffixes}")
    print(f"prom_suffixes: {prom_suffixes}")
    print(f"has_all_expected_prom_scores: {new_suffixes.issubset(prom_suffixes)}")
    print(f"has_any_prom_score: {len(prom_suffixes) > 0}")
    
    # Теперь собираем информацию для формирования датафреймов
    for sample_name, sample_df in samples_dict.items():
        metric_columns = [col for col in sample_df.columns if col not in keys]

        new_scores = [col for col in metric_columns if col.endswith('_new') and not col.startswith('status_')]
        prom_scores = [col for col in metric_columns if col.endswith('_prom') and not col.startswith('status_')]
        new_statuses = [col for col in metric_columns if col.endswith('_new') and col.startswith('status_')]
        prom_statuses = [col for col in metric_columns if col.endswith('_prom') and col.startswith('status_')]
        
        # Для первого датафрейма (основные скоры)
        if new_scores:
            # Предпочитаем new скоры
            for col in new_scores + new_statuses:
                short_name = col.replace('_new', '')
                df1_columns_info.append({
                    'sample_df': sample_df,
                    'original_col': col,
                    'short_name': short_name
                })
        elif prom_scores:
            # Если new нет, берем prom
            for col in prom_scores + prom_statuses:
                short_name = col.replace('_prom', '')
                df1_columns_info.append({
                    'sample_df': sample_df,
                    'original_col': col,
                    'short_name': short_name
                })
        
        # Для второго датафрейма (prom скоры, если есть все необходимые)
        has_all_expected_prom_scores = new_suffixes.issubset(prom_suffixes)
        has_any_prom_score = len(prom_suffixes) > 0
        
        if has_all_expected_prom_scores and has_any_prom_score:
            for score_col in prom_scores:
                base_name = score_col.replace('_prom', '')
                status_col = None
                
                expected_status_name = f"status_{base_name}"
                status_candidates = [
                    col for col in prom_statuses 
                    if col.replace('_prom', '') == expected_status_name
                ]
                status_col = status_candidates[0] if status_candidates else None
                
                cols_to_take = [score_col]
                if status_col:
                    cols_to_take.append(status_col)
                
                short_name = score_col.replace('_prom', '')
                df2_columns_info.append({
                    'sample_df': sample_df,
                    'original_cols': cols_to_take,
                    'short_names': [col.replace('_prom', '') for col in cols_to_take]
                })
    
    print(f"df1_columns_info length: {len(df1_columns_info)}")
    print(f"df2_columns_info length: {len(df2_columns_info)}")
    
    # Формируем датафреймы
    merged_extra_1 = None
    merged_extra_2 = None
    
    if df1_columns_info:
        merge_dfs = []
        for info in df1_columns_info:
            temp_df = info['sample_df'][keys + [info['original_col']]].copy()
            temp_df = temp_df.rename(columns={info['original_col']: info['short_name']})
            merge_dfs.append(temp_df)
        
        if merge_dfs:
            merged_extra_1 = merge_dfs[0]
            for temp_df in merge_dfs[1:]:
                merged_extra_1 = merged_extra_1.merge(temp_df, on=keys, how='inner')
    
    if df2_columns_info:
        merge_dfs = []
        for info in df2_columns_info:
            temp_df = info['sample_df'][keys + info['original_cols']].copy()
            rename_dict = {old: new for old, new in zip(info['original_cols'], info['short_names'])}
            temp_df = temp_df.rename(columns=rename_dict)
            merge_dfs.append(temp_df)
        
        if merge_dfs:
            merged_extra_2 = merge_dfs[0]
            for temp_df in merge_dfs[1:]:
                merged_extra_2 = merged_extra_2.merge(temp_df, on=keys, how='inner')
    
    return merged_extra_1, merged_extra_2