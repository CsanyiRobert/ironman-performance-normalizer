from pathlib import Path

import pandas as pd


def build_weather_features(
    weather_path: str,
    output_path: str,
) -> pd.DataFrame:
    df = pd.read_csv(weather_path, parse_dates=["event_date", "time"])

    # Keep only the main weather variables needed for modeling
    keep_cols = [
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
    ]

    existing_cols = [col for col in keep_cols if col in df.columns]
    df = df[existing_cols].copy()

    # Drop duplicates just in case
    df = df.drop_duplicates(subset=["race_id", "year"])

    # Drop rows with missing essential weather data
    before = len(df)
    df = df.dropna(subset=["temp", "tmax", "prcp", "wspd", "rhum"])
    dropped = before - len(df)

    # Simple weather flags
    df["hot_day_flag"] = (df["tmax"] >= 28).astype(int)
    df["very_hot_day_flag"] = (df["tmax"] >= 32).astype(int)
    df["rain_flag"] = (df["prcp"] > 0).astype(int)
    df["windy_day_flag"] = (df["wspd"] >= 20).astype(int)
    df["humid_day_flag"] = (df["rhum"] >= 75).astype(int)

    # Optional helper features
    df["temp_range"] = df["tmax"] - df["tmin"]

    print(f"{len(df)} weather feature rows created successfully")
    print(f"Dropped {dropped} rows due to missing essential weather data")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"Saved to: {output_path}")

    return df


if __name__ == "__main__":
    df = build_weather_features(
        weather_path="data/raw/weather/weather_daily.csv",
        output_path="data/processed/weather_features.csv",
    )
    print(df.head())