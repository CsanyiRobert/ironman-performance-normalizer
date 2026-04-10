import pandas as pd
from pathlib import Path

from ironman_performance_normalizer.data.load_races import load_races
from ironman_performance_normalizer.data.load_race_dates import load_race_dates


def prepare_weather_input(
    races_path: str,
    race_dates_path: str,
    output_path: str,
) -> pd.DataFrame:
    races = load_races(races_path)
    race_dates = load_race_dates(race_dates_path)

    df = race_dates.merge(races, on="race_id", how="left")

    # post-merge checks
    assert df["event_date"].notna().all(), "Missing event_date after merge"
    assert df["latitude"].notna().all(), "Missing latitude after merge"
    assert df["longitude"].notna().all(), "Missing longitude after merge"

    print(f"{len(df)} race-date rows prepared for weather download")

    # ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # save
    df.to_csv(output_path, index=False)
    print(f"Saved to: {output_path}")

    return df


if __name__ == "__main__":
    df = prepare_weather_input(
        "/Users/csanyirobert/Documents/Data_science/Hobby_projects/Dataklub_hobbiprojekt/ironman-performance-normalizer/data/raw/races_catalog.csv",
        "/Users/csanyirobert/Documents/Data_science/Hobby_projects/Dataklub_hobbiprojekt/ironman-performance-normalizer/data/raw/race_dates.csv",
        "/Users/csanyirobert/Documents/Data_science/Hobby_projects/Dataklub_hobbiprojekt/ironman-performance-normalizer/data/interim/weather_input.csv",
    )
    print(df.head())