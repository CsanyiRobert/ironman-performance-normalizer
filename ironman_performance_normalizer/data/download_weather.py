"""
Download daily weather data for Ironman race events using Meteostat.

This script reads a race-level input file containing event dates and race
coordinates, downloads daily weather data for each race, and saves the result
to disk.

Parameters
----------
weather_input_path : str
    Path to the input CSV file containing one row per race event.

output_path : str
    Path to the output CSV file where the downloaded daily weather data
    will be saved.

limit : int | None, default=None
    Optional row limit for testing. If provided, only the first `limit`
    rows of the input file are processed.

Inputs
------
Input CSV file with at least the following columns:
- race_id : str
    Unique race identifier.
- year : int
    Race year.
- event_date : str or datetime-like
    Date of the race event.
- latitude : float
    Latitude of the race location.
- longitude : float
    Longitude of the race location.

Outputs
-------
Main output CSV:
- race_id : str
- year : int
- event_date : datetime
- time : datetime
- latitude : float
- longitude : float
- tavg : float
    Average air temperature in °C.
- tmin : float
    Minimum air temperature in °C.
- tmax : float
    Maximum air temperature in °C.
- prcp : float
    Total precipitation in mm.
- wspd : float
    Average wind speed in km/h.
- rhum : float
    Relative humidity in %.

Additional output CSV (if failures occur):
- weather_daily_failures.csv
    Contains rows for race events where no weather data could be downloaded.

Notes
-----
The script uses the following retrieval strategy for each race event:
1. Find nearby weather stations
2. Try interpolation
3. If interpolation fails, fall back to the nearest station
"""

from pathlib import Path

import pandas as pd
import meteostat as ms


def ensure_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure that all required weather columns exist in the DataFrame.

    If tavg is not available from Meteostat, it is approximated as:
    (tmin + tmax) / 2.
    """
    selected_columns = [
        "time",
        "tavg",  # average air temperature in °C
        "tmin",  # minimum air temperature in °C
        "tmax",  # maximum air temperature in °C
        "prcp",  # total precipitation in mm
        "wspd",  # average wind speed in km/h
        "rhum",  # relative humidity in %
    ]

    for col in selected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    if "tmin" in df.columns and "tmax" in df.columns:
        missing_tavg = df["tavg"].isna()
        df.loc[missing_tavg, "tavg"] = (
            df.loc[missing_tavg, "tmin"] + df.loc[missing_tavg, "tmax"]
        ) / 2

    return df[selected_columns]

def fetch_daily_weather(lat: float, lon: float, date: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch daily weather for a single race-date row.

    Strategy
    --------
    1. Find nearby stations
    2. Try interpolation
    3. If interpolation fails, fall back to nearest-station daily data

    Parameters
    ----------
    lat : float
        Latitude of the race location.
    lon : float
        Longitude of the race location.
    date : pd.Timestamp
        Race date.

    Returns
    -------
    pd.DataFrame
        A one-row DataFrame containing daily weather variables,
        or an empty DataFrame if no data is available.
    """
    point = ms.Point(lat, lon)

    # Find nearby stations
    stations = ms.stations.nearby(point, limit=4)

    # Try interpolation first
    try:
        ts = ms.daily(stations, date, date)
        interpolated = ms.interpolate(ts, point)

        if interpolated is not None:
            data = interpolated.fetch()

            if data is not None and not data.empty:
                data = data.reset_index()
                return ensure_weather_columns(data)

    except Exception as e:
        print(f"Interpolation failed for ({lat}, {lon}) on {date.date()}: {e}")

    # Fallback: use nearest station directly
    try:
        station_df = stations.fetch()

        if station_df is None or station_df.empty:
            return pd.DataFrame()

        nearest_station_id = station_df.index[0]
        fallback_ts = ms.daily(nearest_station_id, date, date)
        fallback_data = fallback_ts.fetch()

        if fallback_data is None or fallback_data.empty:
            return pd.DataFrame()

        fallback_data = fallback_data.reset_index()
        return ensure_weather_columns(fallback_data)

    except Exception as e:
        print(f"Fallback failed for ({lat}, {lon}) on {date.date()}: {e}")
        return pd.DataFrame()


def download_weather(
    weather_input_path: str,
    output_path: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Download daily weather data for all race events listed in the input file.

    Parameters
    ----------
    weather_input_path : str
        Path to the input CSV file containing race dates and coordinates.

    output_path : str
        Path to the output CSV file.

    limit : int | None, default=None
        Optional row limit for testing.

    Returns
    -------
    pd.DataFrame
        DataFrame containing downloaded daily weather data.
    """
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
        "tavg",
        "tmin",
        "tmax",
        "prcp",
        "wspd",
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