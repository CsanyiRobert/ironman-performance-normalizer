"""
Download daily weather data for Ironman race locations using Meteostat.

This script retrieves historical daily weather data for given race locations
and dates, and saves the processed dataset for further modeling.

Parameters
----------
input_path : str
    Path to the input CSV file containing race metadata (e.g. location, date).

output_path : str
    Path where the processed weather dataset will be saved.

Inputs
------
- CSV file with at least the following columns:
    - race_id (str): unique identifier of the race
    - latitude (float): latitude of the race location
    - longitude (float): longitude of the race location
    - date (str or datetime): race date

Outputs
-------
- CSV file containing daily weather features for each race:
    - tavg : average air temperature in °C
    - tmin : minimum air temperature in °C
    - tmax : maximum air temperature in °C
    - prcp : total precipitation in mm
    - wspd : average wind speed in km/h
    - rhum : relative humidity in %

Notes
-----
- Uses Meteostat Daily API
- Missing values are not handled in this script
- Each row corresponds to one race-day observation
"""

from pathlib import Path
import pandas as pd
from meteostat import Point, Daily


def download_weather(input_path: str, output_path: str) -> None:
    df = pd.read_csv(input_path)

    weather_data = []

    for _, row in df.iterrows():
        race_id = row["race_id"]
        lat = row["latitude"]
        lon = row["longitude"]
        date = pd.to_datetime(row["date"])

        location = Point(lat, lon)

        # Fetch daily weather data for the race date
        data = Daily(location, date, date).fetch()

        if data.empty:
            continue

        data = data.reset_index()

        weather_data.append({
            "race_id": race_id,
            "date": date,

            "tavg": data.at[0, "tavg"],
            "tmin": data.at[0, "tmin"],
            "tmax": data.at[0, "tmax"],
            "prcp": data.at[0, "prcp"],
            "wspd": data.at[0, "wspd"],
            "rhum": data.at[0, "rhum"],
        })

    result = pd.DataFrame(weather_data)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)


if __name__ == "__main__":
    input_path = "data/processed/races_with_locations.csv"
    output_path = "data/processed/weather_data.csv"

    download_weather(input_path, output_path)