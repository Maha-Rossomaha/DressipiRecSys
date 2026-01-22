import pandas as pd

from preprocessing import process_dataframes


def make_df(columns, rows):
    return pd.DataFrame(rows, columns=columns)


def run_case(title, samples, keys):
    print(f"\n=== {title} ===")
    out_df1, out_df2 = process_dataframes(samples, keys)
    print("df1:")
    print(out_df1)
    print("df2:")
    print(out_df2)


def main():
    keys = ["c1", "c3"]

    df_a = make_df(
        ["c1", "c3", "pk_pd_score_new", "status_pk_pd_score_new"],
        [[1, "2024-01-01", 0.5, "ok"], [2, "2024-01-02", 0.6, "fail"]],
    )
    df_b = make_df(
        ["c1", "c3", "kk_score_new", "status_kk_score_new"],
        [[1, "2024-01-01", 0.2, "ok"], [2, "2024-01-02", 0.3, "ok"]],
    )

    run_case("only new scores", {"in_a": df_a, "in_b": df_b}, keys)

    df_c = make_df(
        ["c1", "c3", "pk_pd_score_prom", "status_pk_pd_score_prom"],
        [[1, "2024-01-01", 0.45, "ok"], [2, "2024-01-02", 0.55, "fail"]],
    )
    df_d = make_df(
        ["c1", "c3", "kk_score_prom"],
        [[1, "2024-01-01", 0.15], [2, "2024-01-02", 0.25]],
    )

    run_case("all prom scores", {"in_c": df_c, "in_d": df_d}, keys)

    df_e = make_df(
        ["c1", "c3", "pk_pd_score_new", "status_pk_pd_score_new"],
        [[1, "2024-01-01", 0.51, "ok"], [2, "2024-01-02", 0.61, "ok"]],
    )
    df_f = make_df(
        ["c1", "c3", "kk_score_prom", "status_kk_score_prom"],
        [[1, "2024-01-01", 0.19, "ok"], [2, "2024-01-02", 0.29, "ok"]],
    )

    run_case("mix new and prom", {"in_e": df_e, "in_f": df_f}, keys)


if __name__ == "__main__":
    main()
