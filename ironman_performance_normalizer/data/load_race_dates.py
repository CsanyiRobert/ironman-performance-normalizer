import pandas as pd


def load_race_dates(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # parse date column
    df["event_date"] = pd.to_datetime(df["event_date"])

    # basic checks
    assert not df.duplicated(["race_id", "year"]).any(), "Duplicate race_id-year pairs found"
    assert df["event_date"].notna().all(), "Missing event_date found"

    # consistency checks
    assert (df["year"] == df["event_date"].dt.year).all(), \
        "Mismatch between 'year' and year extracted from 'event_date'"

    assert (df["event_month"] == df["event_date"].dt.month).all(), \
        "Mismatch between 'event_month' and month extracted from 'event_date'"

    print(f"{len(df)} race dates loaded successfully")

    return df


if __name__ == "__main__":
    df = load_race_dates("/Users/csanyirobert/Documents/Data_science/Hobby_projects/Dataklub_hobbiprojekt/ironman-performance-normalizer/data/raw/race_dates.csv")
    print(df.head())