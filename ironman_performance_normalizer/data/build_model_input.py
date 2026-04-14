from pathlib import Path

import pandas as pd


def build_model_input(
    weather_features_path: str,
    results_agg_path: str,
    output_path: str,
) -> pd.DataFrame:
    weather = pd.read_csv(weather_features_path, parse_dates=["event_date", "time"])
    results = pd.read_csv(results_agg_path)

    # basic checks
    if weather.duplicated(["race_id", "year"]).any():
        raise ValueError("Duplicate race_id-year pairs found in weather features")

    if results.duplicated(["race_id", "year"]).any():
        raise ValueError("Duplicate race_id-year pairs found in results aggregates")

    df = weather.merge(results, on=["race_id", "year"], how="inner")

    if df.empty:
        raise ValueError("Model input is empty after merge")

    if df["median_time_sec"].isna().any():
        raise ValueError("Missing median_time_sec values after merge")

    # Reorder columns for readability
    preferred_order = [
        "race_id",
        "year",
        "event_date",
        "time",
        "latitude",
        "longitude",
        "temp",
        "tmin",
        "tmax",
        "prcp",
        "wspd",
        "pres",
        "rhum",
        "hot_day_flag",
        "very_hot_day_flag",
        "rain_flag",
        "windy_day_flag",
        "humid_day_flag",
        "temp_range",
        "n_finishers",
        "winner_time_sec",
        "median_time_sec",
    ]

    existing_cols = [col for col in preferred_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + remaining_cols]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"{len(df)} model input rows created successfully")
    print(f"Saved to: {output_path}")

    return df


if __name__ == "__main__":
    df = build_model_input(
        weather_features_path="data/processed/weather_features.csv",
        results_agg_path="data/raw/results/results_agg.csv",
        output_path="data/processed/model_input.csv",
    )
    print(df.head())