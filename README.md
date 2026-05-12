# Ironman Performance Normalizer

A data science project to normalize Ironman race performances across different courses and environmental conditions.

The goal is to understand how weather and course characteristics (e.g., elevation, distance) influence race outcomes, and to build a model that enables fair comparison across different Ironman events.

---

## 1. Project Overview

Ironman race times are not directly comparable across events due to differences in:

- course difficulty (e.g. elevation)
- environmental conditions (temperature, wind, precipitation)

This project aims to quantify these effects and build a model that explains how external factors influence race performance.

The long-term goal is to enable fair comparison of performances across different races by accounting for course and weather conditions.

---

## 2. Data

### 2.1 Data Sources

The dataset is constructed from multiple sources:

- **Race metadata**
  - Official Ironman website
  - Includes race location, coordinates, and course information

- **Race dates**
  - Manually collected from official and archival sources

- **Weather data**
  - Retrieved via the `meteostat` Python package
  - Daily historical weather based on race date and location

- **Race results**
  - Downloaded manually from Ironman results pages
  - Aggregated to race-level metrics (e.g. median time)

---

### 2.2 Data Structure

The final modeling dataset (`model_input.csv`) contains one row per:

`race_id` + `year`

Feature groups:

**Weather features**

- `tavg` – average temperature (°C)
- `wspd` – wind speed (km/h)
- `prcp` – precipitation (mm)
- `rhum` – relative humidity (%)

**Course features**

- `bike_distance_km` – bike course length (km)
- `run_distance_km` – run course length (km)
- `bike_elevation_gain_m` – total bike elevation gain (m)
- `run_elevation_gain_m` – total run elevation gain (m)

**Target variables**

- `median_time_sec`
- `winner_time_sec`

---

### 2.3 Data Limitations

- Some race editions were cancelled (e.g. 2020–2021 due to COVID-19)
- Race dates were manually collected and may contain minor inaccuracies
- Weather data is based on the closest available weather station
- One race-year observation (IRONMAN Austria 2005) was excluded due to missing weather data
- Course data is based on recent course profiles and may not fully reflect historical route changes
- The dataset currently includes a limited number of races, which constrains model generalization

---

## 3. Methodology

The project follows a structured data pipeline:

1. **Data ingestion**
   - Load race metadata and race dates

2. **Weather data preparation**
   - Generate race-date-location inputs
   - Download historical weather data using Meteostat

3. **Feature engineering**
   - Create weather features
   - Add course-related features (distance, elevation)

4. **Results aggregation**
   - Parse raw results
   - Convert time strings to seconds
   - Compute aggregate metrics (median, winner time)

5. **Dataset construction**
   - Merge all data sources into a single modeling dataset
---

## 4. Results

A baseline linear regression model was trained to predict:

`median_time_sec`

using weather and course features.

**Key findings:**

- Wind speed (`wspd`) has a strong positive effect on race time  
  → higher wind leads to slower performances

- Bike elevation gain is a major determinant of performance  
  → more elevation significantly increases finishing times

- After including course features, the effect of race identity is largely reduced  
  → indicating that elevation and distance explain most structural differences between races

- Temperature and precipitation have smaller but plausible effects

These results are based on a limited dataset and should be interpreted as exploratory rather than definitive.
---

## 5. Next Steps

Planned improvements:

- Expand dataset to include additional Ironman races
- Incorporate more detailed course features (e.g. elevation profiles, terrain types)
- Improve weather representation (e.g. hourly data, race-time conditions)
- Explore alternative targets:
  - normalized performance metrics
  - pace-based indicators
- Evaluate more advanced models (e.g. regularized regression, tree-based models)

---

## Project Organization
The project follows a structured data pipeline from raw data collection to processed modeling datasets.

```text
data/
├── raw/
│   ├── races_catalog.csv        # race metadata (location, course characteristics)
│   ├── race_dates.csv           # race dates by year
│   ├── results/                 # raw race result files (per race-year)
│   └── weather/                 # downloaded daily weather data
│
├── interim/
│   └── weather_input.csv        # race-date-location input for weather download
│
├── processed/
│   ├── weather_features.csv     # engineered weather features
│   └── model_input.csv          # final dataset for modeling
│
ironman_performance_normalizer/
├── data/
│   ├── load_races.py                # load and validate race metadata
│   ├── load_race_dates.py           # load and validate race dates
│   ├── prepare_weather_input.py     # create weather download input
│   ├── download_weather.py          # download weather data via Meteostat
│   ├── build_weather_features.py    # create weather features
│   ├── build_results_agg.py         # aggregate race results (median, winner time)
│   └── build_model_input.py         # merge all data into modeling dataset
│
├── modeling/
│   ├── train.py                 # model training scripts (future use)
│   └── predict.py               # prediction/inference scripts (future use)
│
notebooks/
└── baseline_model.ipynb         # exploratory analysis and baseline modeling
```


---

This project demonstrates an end-to-end data science workflow from data collection and feature engineering to modeling and interpretation.