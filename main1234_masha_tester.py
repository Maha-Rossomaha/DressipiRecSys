import pandas as pd

from main1234_masha import main


def make_df(data, roles):
    df = pd.DataFrame(data)
    df.columns = [c.lower() for c in df.columns]
    df.columns_metadata = [
        {
            'columnName': name.lower(),
            'role': role,
            'type': 'Interval',
            'statistics': None,
        }
        for name, role in roles.items()
    ]
    return df


def get_role(metadata, column_name):
    for item in metadata:
        if item['columnName'] == column_name:
            return item['role']
    return None


def test_only_sample_1_integral_mode_in_sample_1():
    features = make_df(
        {'id': [1, 2], 'target': [0, 1]},
        {'id': 'ID', 'target': 'Target'},
    )
    sample_1 = make_df(
        {'id': [1, 2], 'baseline_score': [0.1, 0.2], 'integral_mode': [1, 1]},
        {'id': 'ID', 'baseline_score': 'Other', 'integral_mode': 'Other'},
    )

    result = main(features=features, sample_1=sample_1)
    metadata = result['full_df_1'].columns_metadata
    assert get_role(metadata, 'baseline_score') == 'Baseline'
    assert get_role(metadata, 'integral_mode') == 'Excluded'
    assert result['full_df_2'] is None


def test_multiple_samples_integral_mode_everywhere():
    features = make_df(
        {'id': [1, 2], 'target': [0, 1]},
        {'id': 'ID', 'target': 'Target'},
    )
    sample_1 = make_df(
        {'id': [1, 2], 'baseline_score': [0.1, 0.2], 'integral_mode': [1, 1]},
        {'id': 'ID', 'baseline_score': 'Other', 'integral_mode': 'Other'},
    )
    sample_2 = make_df(
        {'id': [1, 2], 'baseline_score': [0.3, 0.4], 'integral_mode': [1, 1]},
        {'id': 'ID', 'baseline_score': 'Other', 'integral_mode': 'Other'},
    )

    result = main(features=features, sample_1=sample_1, sample_2=sample_2)
    metadata = result['full_df_1'].columns_metadata
    assert get_role(metadata, 'baseline_score') == 'Baseline'
    assert get_role(metadata, 'integral_mode') == 'Excluded'
    assert result['full_df_2'] is not None


def test_prom_score_with_multiple_samples():
    features = make_df(
        {'id': [1, 2], 'target': [0, 1]},
        {'id': 'ID', 'target': 'Target'},
    )
    prom_score = make_df(
        {'id': [1, 2], 'mr2_score': [0.5, 0.6], 'status': [1, 1]},
        {'id': 'ID', 'mr2_score': 'Other', 'status': 'Other'},
    )
    sample_1 = make_df(
        {'id': [1, 2], 'baseline_score': [0.1, 0.2], 'integral_mode': [1, 1]},
        {'id': 'ID', 'baseline_score': 'Other', 'integral_mode': 'Other'},
    )
    sample_2 = make_df(
        {'id': [1, 2], 'baseline_score': [0.3, 0.4], 'integral_mode': [1, 1]},
        {'id': 'ID', 'baseline_score': 'Other', 'integral_mode': 'Other'},
    )

    result = main(
        features=features,
        prom_score=prom_score,
        sample_1=sample_1,
        sample_2=sample_2,
    )
    metadata = result['full_df_1'].columns_metadata
    assert get_role(metadata, 'baseline_score') == 'Baseline'
    assert get_role(metadata, 'mr2_score') == 'MR2'
    assert get_role(metadata, 'status') == 'Excluded'
    assert get_role(metadata, 'integral_mode') == 'Excluded'


if __name__ == '__main__':
    test_only_sample_1_integral_mode_in_sample_1()
    test_multiple_samples_integral_mode_everywhere()
    test_prom_score_with_multiple_samples()
    print('All tests passed.')
