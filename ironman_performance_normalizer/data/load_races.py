import pandas as pd

def load_races(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # basic checks
    assert df["race_id"].is_unique, "Duplicate race_id found"
    assert df["latitude"].notna().all(), "Missing latitude"
    assert df["longitude"].notna().all(), "Missing longitude"

    print(f"{len(df)} races loaded successfully")

    return df


if __name__ == "__main__":
    df = load_races("/Users/csanyirobert/Documents/Data_science/Hobby_projects/Dataklub_hobbiprojekt/ironman-performance-normalizer/data/raw/races_catalog.csv")
    print(df.head())