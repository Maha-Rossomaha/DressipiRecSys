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
    sample_1.columns = [k.lower() for k in sample_1.columns]
    sample_2.columns = [k.lower() for k in sample_2.columns]
    sample_3.columns = [k.lower() for k in sample_3.columns]
    sample_4.columns = [k.lower() for k in sample_4.columns]
    
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
        print(merge_columns)
        
        for elem in sample_1.columns_metadata:
            if elem['columnName'].lower() not in merge_columns+targets:
                score_column = elem['columnName'].lower()
        
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
            sample_1_duplicates = sample_1.duplicated(subset=merge_columns + ['integral_mode'] if 'integral_mode' in sample_1.columns else merge_columns).sum()
            sample_2_duplicates = sample_2.duplicated(subset=merge_columns).sum()
            sample_3_duplicates = sample_3.duplicated(subset=merge_columns).sum()
            sample_4_duplicates = sample_4.duplicated(subset=merge_columns).sum()
            
            if features_duplicates != 0:
                print(f'В выборке features обнаружено {features_duplicates} дубликатов')
                features = features.drop_duplicates(subset=merge_columns)
            
            if sample_1_duplicates != 0:
                print(f'В выборке sample_1 обнаружено {sample_1_duplicates} дубликатов')
                sample_1 = sample_1.drop_duplicates(subset=merge_columns)
                
            if sample_2_duplicates != 0:
                print(f'В выборке sample_2 обнаружено {sample_2_duplicates} дубликатов')
                sample_2 = sample_2.drop_duplicates(subset=merge_columns)
                
            if sample_3_duplicates != 0:
                print(f'В выборке sample_3 обнаружено {sample_3_duplicates} дубликатов')
                sample_3 = sample_3.drop_duplicates(subset=merge_columns)
                
            if sample_4_duplicates != 0:
                print(f'В выборке sample_4 обнаружено {sample_4_duplicates} дубликатов')
                sample_4 = sample_4.drop_duplicates(subset=merge_columns)
            
            full_df_1 = pd.merge(left=features, right=sample_1, how='inner', on=merge_columns)
            full_df_2 = pd.merge(left=features, right=sample_2, how='inner', on=merge_columns)
            full_df_3 = pd.merge(left=features, right=sample_3, how='inner', on=merge_columns)
            full_df_4 = pd.merge(left=features, right=sample_4, how='inner', on=merge_columns)
            
            full_df_1.columns_metadata = full_metadata
            full_df_2.columns_metadata = full_metadata
            full_df_3.columns_metadata = full_metadata
            full_df_4.columns_metadata = full_metadata
            
    else:
        prom_score.columns = [k.lower() for k in prom_score.columns]
        prom_score.columns = [k if k not in targets else k+'_prom' for k in prom_score.columns]
        merge_columns = list(set(features.columns) & set(sample_1.columns) & set(prom_score.columns))
        
        print('merge_columns+targets', merge_columns+targets)
        for elem in sample_1.columns_metadata:
            if elem['columnName'].lower() not in merge_columns+targets:
                if elem['columnName'].lower() != 'integral_mode':
                    baseline_column = elem['columnName'].lower()
                
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
            sample_1_duplicates = sample_1.duplicated(subset=merge_columns + ['integral_mode'] if 'integral_mode' in sample_1.columns else merge_columns).sum()
            
            sample_2_duplicates = sample_2.duplicated(subset=merge_columns).sum()
            sample_3_duplicates = sample_3.duplicated(subset=merge_columns).sum()
            sample_4_duplicates = sample_4.duplicated(subset=merge_columns).sum()
            
            if features_duplicates != 0:
                print(f'В выборке features обнаружено {features_duplicates} дубликатов')
                features = features.drop_duplicates(subset=merge_columns)
                
            if prom_score_duplicates != 0:
                print(f'В выборке features обнаружено {prom_score_duplicates} дубликатов')
                prom_score = prom_score.drop_duplicates(subset=merge_columns)
            
            if sample_1_duplicates != 0:
                print(f'В выборке sample_1 обнаружено {sample_1_duplicates} дубликатов')
                sample_1 = sample_1.drop_duplicates(subset=merge_columns)
                
            if sample_2_duplicates != 0:
                print(f'В выборке sample_2 обнаружено {sample_2_duplicates} дубликатов')
                sample_2 = sample_2.drop_duplicates(subset=merge_columns)
                
            if sample_3_duplicates != 0:
                print(f'В выборке sample_3 обнаружено {sample_3_duplicates} дубликатов')
                sample_3 = sample_3.drop_duplicates(subset=merge_columns)
                
            if sample_4_duplicates != 0:
                print(f'В выборке sample_4 обнаружено {sample_4_duplicates} дубликатов')
                sample_4 = sample_4.drop_duplicates(subset=merge_columns)
            
            full_df_1 = pd.merge(left=features, right=sample_1, how='inner', on=merge_columns)
            full_df_1 = pd.merge(left=prom_score, right=full_df_1, how='inner', on=merge_columns)
            full_df_2 = pd.merge(left=features, right=sample_2, how='inner', on=merge_columns)
            full_df_2 = pd.merge(left=prom_score, right=full_df_2, how='inner', on=merge_columns)
            full_df_3 = pd.merge(left=features, right=sample_3, how='inner', on=merge_columns)
            full_df_3 = pd.merge(left=prom_score, right=full_df_3, how='inner', on=merge_columns)
            full_df_4 = pd.merge(left=features, right=sample_4, how='inner', on=merge_columns)
            full_df_4 = pd.merge(left=prom_score, right=full_df_4, how='inner', on=merge_columns)
            
            full_df_1.columns_metadata = full_metadata
            full_df_2.columns_metadata = full_metadata
            full_df_3.columns_metadata = full_metadata
            full_df_4.columns_metadata = full_metadata
        
        
    return {'full_df_1': full_df_1,
            'full_df_2': full_df_2,
            'full_df_3': full_df_3,
            'full_df_4': full_df_4
           }