from pathlib import Path
import re

import pandas as pd


def hhmmss_to_seconds(time_str):
    if pd.isna(time_str):
        return pd.NA

    time_str = str(time_str).strip()

    if time_str in {"DNF", "DNS", "DSQ", ""}:
        return pd.NA

    parts = time_str.split(":")
    if len(parts) != 3:
        return pd.NA

    try:
        h, m, s = map(int, parts)
        total_seconds = h * 3600 + m * 60 + s

        # Exclude clearly invalid zero or negative durations
        if total_seconds <= 0:
            return pd.NA

        return total_seconds
    except ValueError:
        return pd.NA


def parse_filename(file_path: Path) -> tuple[str, int]:
    """
    Expected examples:
    ironmanfrankfurt2005-results.csv
    ironmanlanzarote2019-results.csv
    """
    name = file_path.name.lower()

    match = re.match(r"ironman([a-z]+)(\d{4})-results\.csv$", name)
    if not match:
        raise ValueError(
            f"Filename '{file_path.name}' does not match expected pattern "
            f"'ironman<race><year>-results.csv'"
        )

    race_name = match.group(1)
    year = int(match.group(2))

    race_map = {
        "frankfurt": "im_frankfurt",
        "lanzarote": "im_lanzarote",
    }

    if race_name not in race_map:
        raise ValueError(f"Unknown race name in filename: {race_name}")

    race_id = race_map[race_name]

    return race_id, year


def build_results_agg(results_dir: str, output_path: str) -> pd.DataFrame:
    results_path = Path(results_dir)

    if not results_path.exists():
        raise FileNotFoundError(f"Results directory not found: {results_dir}")

    files = sorted(results_path.glob("*-results.csv"))

    if not files:
        raise ValueError(f"No result CSV files found in: {results_dir}")

    rows = []

    for file_path in files:
        race_id, year = parse_filename(file_path)

        df = pd.read_csv(file_path)

        if "Overall Time" not in df.columns:
            raise ValueError(f"'Overall Time' column not found in: {file_path.name}")

        df["overall_time_sec"] = df["Overall Time"].apply(hhmmss_to_seconds)

        raw_valid_times = df["overall_time_sec"].dropna()

        if raw_valid_times.empty:
            print(f"⚠️ No valid finish times in {file_path.name}")
            continue

        # Debug: show suspiciously low times
        suspicious_times = raw_valid_times[raw_valid_times < 6 * 3600]
        if not suspicious_times.empty:
            print(f"⚠️ Suspicious low times in {file_path.name}: {suspicious_times.tolist()}")

        # Keep only realistic full Ironman finish times
        valid_times = raw_valid_times[raw_valid_times >= 6 * 3600]

        if valid_times.empty:
            print(f"⚠️ No realistic finish times in {file_path.name}")
            continue

        row = {
            "race_id": race_id,
            "year": year,
            "n_finishers": int(valid_times.shape[0]),
            "winner_time_sec": int(valid_times.min()),
            "median_time_sec": float(valid_times.median()),
        }

        rows.append(row)
        print(f"✅ Processed {file_path.name}")

    if not rows:
        raise ValueError("No result aggregates were created.")

    df_out = pd.DataFrame(rows).sort_values(["race_id", "year"]).reset_index(drop=True)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False)

    print(f"Saved to: {output_path}")

    return df_out


if __name__ == "__main__":
    df = build_results_agg(
        results_dir="data/raw/results",
        output_path="data/raw/results/results_agg.csv",
    )
    print(df.head())