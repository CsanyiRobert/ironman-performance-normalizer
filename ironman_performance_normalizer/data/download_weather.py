from pathlib import Path

import pandas as pd
import meteostat as ms


def fetch_daily_weather(lat: float, lon: float, date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch daily weather for a single race-date row.

    Strategy:
    1. Find nearby stations
    2. Try interpolation
    3. If interpolation fails, fall back to nearest-station daily data
    """
    point = ms.Point(lat, lon)

    # Find nearby stations
    stations = ms.stations.nearby(point, limit=4)

    # Build daily time series from nearby stations
    ts = ms.daily(stations, date, date)

    # Try interpolation first
    try:
        interpolated = ms.interpolate(ts, point)

        if interpolated is not None:
            data = interpolated.fetch()

            if data is not None and not data.empty:
                return data.reset_index()

    except Exception as e:
        print(f"Interpolation failed for ({lat}, {lon}) on {date.date()}: {e}")

    # Fallback: use nearest station directly
    try:
        station_df = stations.fetch()

        if station_df is None or station_df.empty:
            return pd.DataFrame()

        nearest_station_id = station_df.index[0]
        fallback_ts = ms.daily(ms.Station(nearest_station_id), date, date)
        fallback_data = fallback_ts.fetch()

        if fallback_data is None or fallback_data.empty:
            return pd.DataFrame()

        fallback_data = fallback_data.reset_index()
        fallback_data["source"] = "nearest_station"

        return fallback_data

    except Exception as e:
        print(f"Fallback failed for ({lat}, {lon}) on {date.date()}: {e}")
        return pd.DataFrame()


def download_weather(
    weather_input_path: str,
    output_path: str,
    limit: int | None = None,
) -> pd.DataFrame:
    df_input = pd.read_csv(weather_input_path, parse_dates=["event_date"])

    if limit is not None:
        df_input = df_input.head(limit).copy()

    results = []
    failures = []

    for _, row in df_input.iterrows():
        race_id = row["race_id"]
        year = row["year"]
        date = row["event_date"]
        lat = row["latitude"]
        lon = row["longitude"]

        try:
            weather = fetch_daily_weather(lat, lon, date)

            if weather.empty:
                print(f"⚠️ No data: {race_id} ({date.date()})")
                failures.append(
                    {
                        "race_id": race_id,
                        "year": year,
                        "event_date": date.date(),
                        "latitude": lat,
                        "longitude": lon,
                        "reason": "no_data",
                    }
                )
                continue

            weather["race_id"] = race_id
            weather["year"] = year
            weather["event_date"] = date
            weather["latitude"] = lat
            weather["longitude"] = lon

            results.append(weather)

            print(f"✅ {race_id} ({date.date()})")

        except Exception as e:
            print(f"❌ Error: {race_id} ({date.date()}): {e}")
            failures.append(
                {
                    "race_id": race_id,
                    "year": year,
                    "event_date": date.date(),
                    "latitude": lat,
                    "longitude": lon,
                    "reason": str(e),
                }
            )

    if not results:
        raise ValueError("No weather data downloaded")

    df_out = pd.concat(results, ignore_index=True)

    preferred_order = [
        "race_id",
        "year",
        "event_date",
        "time",
        "latitude",
        "longitude",
        "source",
        "tavg",
        "tmin",
        "tmax",
        "prcp",
        "snow",
        "wdir",
        "wspd",
        "wpgt",
        "pres",
        "temp",
        "rhum",
    ]

    existing_cols = [col for col in preferred_order if col in df_out.columns]
    remaining_cols = [col for col in df_out.columns if col not in existing_cols]
    df_out = df_out[existing_cols + remaining_cols]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False)

    print(f"\nSaved weather data to: {output_path}")

    if failures:
        failures_path = str(Path(output_path).with_name("weather_daily_failures.csv"))
        pd.DataFrame(failures).to_csv(failures_path, index=False)
        print(f"Saved failures to: {failures_path}")

    return df_out


if __name__ == "__main__":
    df = download_weather(
        weather_input_path="data/interim/weather_input.csv",
        output_path="data/raw/weather/weather_daily.csv",
        limit=None,
    )
    print(df.head())