import pandas as pd


def main(
    features: pd.DataFrame = None,   #сэмпл с фичами
    prom_score: pd.DataFrame = None, #сэмпл с фичами
    sample_1: pd.DataFrame = None,
    sample_2: pd.DataFrame = None,
    sample_3: pd.DataFrame = None,
    sample_4: pd.DataFrame = None
    ):

    features.columns = [k.lower() for k in features.columns]
    sample_dfs = {
        'sample_1': sample_1,
        'sample_2': sample_2,
        'sample_3': sample_3,
        'sample_4': sample_4,
    }
    provided_samples = {name: df for name, df in sample_dfs.items() if isinstance(df, pd.DataFrame)}
    for name, df in provided_samples.items():
        df.columns = [k.lower() for k in df.columns]
    
    short_target = None  
    keys = []
    for elem in features.columns_metadata:
        if elem['role'].lower() in ['date', 'id']:
            keys.append(elem['columnName'])
        elif elem['role'].lower() == 'mr1':
            short_target = elem['columnName'].lower()
        elif elem['role'].lower() == 'target':
            target = elem['columnName'].lower()  
    
    print(short_target, target)
    
    if short_target:
        targets = [short_target, target]    
    else: 
        targets = [target]
            
    features.columns = [k if k not in targets else k+'_raw' for k in features.columns]
    
    if not isinstance(prom_score, pd.DataFrame):
        merge_columns = list(set(features.columns) & set(sample_1.columns))
        print('merge_columns', merge_columns)
        sample_columns = [set(df.columns) for df in provided_samples.values()]
        common_sample_columns = set.intersection(*sample_columns) if sample_columns else set()
        baseline_candidates = sorted(common_sample_columns - set(features.columns) - {'integral_mode'})
        print('baseline_candidates', baseline_candidates)
        score_column = baseline_candidates[0] if baseline_candidates else None
        print('baseline_column', score_column)
        integral_mode_samples = [name for name, df in provided_samples.items() if 'integral_mode' in df.columns]
        print('integral_mode_samples', integral_mode_samples)
        
        if len(merge_columns) != len(keys):
            print(f'В датафреймах со скорами лишние колонки: {list(set(merge_columns) - set(keys))}')
        
        features_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in features.columns_metadata.copy()]
        sample_1_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in sample_1.columns_metadata.copy()]
        
        new_metadata = features_metadata + [x for x in sample_1_metadata if x['columnName'] not in merge_columns+targets]
        full_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in new_metadata]
        
        full_metadata = [x if x['columnName'] != score_column else {'columnName': x['columnName'],
                                   'role': 'Baseline',
                                   'type': 'Interval',
                                   'statistics': None} for x in full_metadata]
        print('*'*100)
        print(full_metadata)
        print('*'*100)
        full_metadata = [x if x['columnName'] != 'integral_mode' else {'columnName': x['columnName'],
                                   'role': 'Excluded',
                                   'type': x['type'],
                                   'statistics': None} for x in full_metadata]
        full_metadata = [x if ((x['role'] !='Other') | (x['columnName'] in merge_columns) | (x['columnName'] != 'integral_mode')) else {'columnName': x['columnName'],
                                   'role': 'Excluded',
                                   'type': x['type'],
                                   'statistics': None} for x in full_metadata]
        print('-'*100)
        print(full_metadata)
        print('-'*100)
        #full_metadata = [x if x['role'] != 'ID' else {'columnName': x['columnName'],
        #                           'role': 'Other',
        #                           'type': x['type'],
        #                           'statistics': None} for x in full_metadata]
        #print('~'*100)
        #print(full_metadata)
        #print('~'*100)
        
        if len(merge_columns) == 0:
            print('В датафреймах нет общих колонок')
            full_df_1 = None
            full_df_2 = None
            full_df_3 = None
            full_df_4 = None
        else:
            features_duplicates = features.duplicated(subset=merge_columns).sum()
            sample_duplicates = {}
            for name, df in provided_samples.items():
                subset = merge_columns + ['integral_mode'] if name == 'sample_1' and 'integral_mode' in df.columns else merge_columns
                sample_duplicates[name] = df.duplicated(subset=subset).sum()
            
            if features_duplicates != 0:
                print(f'В выборке features обнаружено {features_duplicates} дубликатов')
                features = features.drop_duplicates(subset=merge_columns)
            
            for name, dup_count in sample_duplicates.items():
                if dup_count != 0:
                    print(f'В выборке {name} обнаружено {dup_count} дубликатов')
                    subset = merge_columns + ['integral_mode'] if name == 'sample_1' and 'integral_mode' in provided_samples[name].columns else merge_columns
                    provided_samples[name] = provided_samples[name].drop_duplicates(subset=subset)

            sample_1 = provided_samples.get('sample_1')
            sample_2 = provided_samples.get('sample_2')
            sample_3 = provided_samples.get('sample_3')
            sample_4 = provided_samples.get('sample_4')

            full_df_1 = pd.merge(left=features, right=sample_1, how='inner', on=merge_columns) if sample_1 is not None else None
            full_df_2 = pd.merge(left=features, right=sample_2, how='inner', on=merge_columns) if sample_2 is not None else None
            full_df_3 = pd.merge(left=features, right=sample_3, how='inner', on=merge_columns) if sample_3 is not None else None
            full_df_4 = pd.merge(left=features, right=sample_4, how='inner', on=merge_columns) if sample_4 is not None else None
            
            if full_df_1 is not None:
                full_df_1.columns_metadata = full_metadata
            if full_df_2 is not None:
                full_df_2.columns_metadata = full_metadata
            if full_df_3 is not None:
                full_df_3.columns_metadata = full_metadata
            if full_df_4 is not None:
                full_df_4.columns_metadata = full_metadata
            
    else:
        prom_score.columns = [k.lower() for k in prom_score.columns]
        prom_score.columns = [k if k not in targets else k+'_prom' for k in prom_score.columns]
        merge_columns = list(set(features.columns) & set(sample_1.columns) & set(prom_score.columns))
        
        print('merge_columns+targets', merge_columns+targets)
        sample_columns = [set(df.columns) for df in provided_samples.values()]
        common_sample_columns = set.intersection(*sample_columns) if sample_columns else set()
        baseline_candidates = sorted(common_sample_columns - set(features.columns) - {'integral_mode'})
        print('baseline_candidates', baseline_candidates)
        baseline_column = baseline_candidates[0] if baseline_candidates else None
        print('baseline_column', baseline_column)
        integral_mode_samples = [name for name, df in provided_samples.items() if 'integral_mode' in df.columns]
        print('integral_mode_samples', integral_mode_samples)
                
        for elem in prom_score.columns_metadata:
            if elem['columnName'].lower() not in merge_columns+targets:
                if elem['columnName'].lower() != 'status':
                    mr2_column = elem['columnName'].lower()
                else:
                    status_column = elem['columnName'].lower()
        
        features_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in features.columns_metadata.copy()]
        sample_1_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in sample_1.columns_metadata.copy()]
        prom_score_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in prom_score.columns_metadata.copy()]
        
        new_metadata = features_metadata + [x for x in sample_1_metadata if x['columnName'] not in merge_columns+targets] + [x for x in prom_score_metadata if x['columnName'] not in merge_columns+targets]
        full_metadata = [{'columnName': x['columnName'].lower(),
                          'role': x['role'],
                          'type': x['type'],
                          'statistics': None} for x in new_metadata]
        
        full_metadata = [x if x['columnName'] != baseline_column else {'columnName': x['columnName'],
                                   'role': 'Baseline',
                                   'type': 'Interval',
                                   'statistics': None} for x in full_metadata]
        
        full_metadata = [x if x['columnName'] != mr2_column else {'columnName': x['columnName'],
                                   'role': 'MR2',
                                   'type': 'Interval',
                                   'statistics': None} for x in full_metadata]
        
        full_metadata = [x if x['columnName'] != status_column else {'columnName': x['columnName'],
                                   'role': 'Excluded',
                                   'type': 'Interval',
                                   'statistics': None} for x in full_metadata]
        
        print('*'*100)
        print(full_metadata)
        print('*'*100)
        full_metadata = [x if x['columnName'] != 'integral_mode' else {'columnName': x['columnName'],
                                   'role': 'Excluded',
                                   'type': x['type'],
                                   'statistics': None} for x in full_metadata]
        full_metadata = [x if ((x['role'] !='Other') | (x['columnName'] in merge_columns) | (x['columnName'] != 'integral_mode')) else {'columnName': x['columnName'],
                                   'role': 'Excluded',
                                   'type': x['type'],
                                   'statistics': None} for x in full_metadata]
        print('-'*100)
        print(full_metadata)
        print('-'*100)
        #full_metadata = [x if x['role'] != 'ID' else {'columnName': x['columnName'],
        #                           'role': 'Other',
        #                           'type': x['type'],
        #                           'statistics': None} for x in full_metadata]
        #print('~'*100)
        #print(full_metadata)
        #print('~'*100)
        
        if len(merge_columns) == 0:
            print('В датафреймах нет общих колонок')
            full_df_1 = None
            full_df_2 = None
            full_df_3 = None
            full_df_4 = None
        else:
            features_duplicates = features.duplicated(subset=merge_columns).sum()
            prom_score_duplicates = prom_score.duplicated(subset=merge_columns).sum()
            sample_duplicates = {}
            for name, df in provided_samples.items():
                subset = merge_columns + ['integral_mode'] if name == 'sample_1' and 'integral_mode' in df.columns else merge_columns
                sample_duplicates[name] = df.duplicated(subset=subset).sum()
            
            if features_duplicates != 0:
                print(f'В выборке features обнаружено {features_duplicates} дубликатов')
                features = features.drop_duplicates(subset=merge_columns)
                
            if prom_score_duplicates != 0:
                print(f'В выборке features обнаружено {prom_score_duplicates} дубликатов')
                prom_score = prom_score.drop_duplicates(subset=merge_columns)
            
            for name, dup_count in sample_duplicates.items():
                if dup_count != 0:
                    print(f'В выборке {name} обнаружено {dup_count} дубликатов')
                    subset = merge_columns + ['integral_mode'] if name == 'sample_1' and 'integral_mode' in provided_samples[name].columns else merge_columns
                    provided_samples[name] = provided_samples[name].drop_duplicates(subset=subset)

            sample_1 = provided_samples.get('sample_1')
            sample_2 = provided_samples.get('sample_2')
            sample_3 = provided_samples.get('sample_3')
            sample_4 = provided_samples.get('sample_4')

            full_df_1 = pd.merge(left=features, right=sample_1, how='inner', on=merge_columns) if sample_1 is not None else None
            full_df_1 = pd.merge(left=prom_score, right=full_df_1, how='inner', on=merge_columns) if full_df_1 is not None else None
            full_df_2 = pd.merge(left=features, right=sample_2, how='inner', on=merge_columns) if sample_2 is not None else None
            full_df_2 = pd.merge(left=prom_score, right=full_df_2, how='inner', on=merge_columns) if full_df_2 is not None else None
            full_df_3 = pd.merge(left=features, right=sample_3, how='inner', on=merge_columns) if sample_3 is not None else None
            full_df_3 = pd.merge(left=prom_score, right=full_df_3, how='inner', on=merge_columns) if full_df_3 is not None else None
            full_df_4 = pd.merge(left=features, right=sample_4, how='inner', on=merge_columns) if sample_4 is not None else None
            full_df_4 = pd.merge(left=prom_score, right=full_df_4, how='inner', on=merge_columns) if full_df_4 is not None else None
            
            if full_df_1 is not None:
                full_df_1.columns_metadata = full_metadata
            if full_df_2 is not None:
                full_df_2.columns_metadata = full_metadata
            if full_df_3 is not None:
                full_df_3.columns_metadata = full_metadata
            if full_df_4 is not None:
                full_df_4.columns_metadata = full_metadata
        
        
    return {'full_df_1': full_df_1,
            'full_df_2': full_df_2,
            'full_df_3': full_df_3,
            'full_df_4': full_df_4
           }
