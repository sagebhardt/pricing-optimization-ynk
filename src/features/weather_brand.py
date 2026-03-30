"""Weather features from Open-Meteo API.

Fetches historical weather data per region and adds weekly aggregates
as ML features. Cached locally to avoid re-fetching.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from pathlib import Path

import httpx

from config.weather import REGION_COORDS, OPEN_METEO_URL, WEATHER_VARS

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw"


def fetch_weather_for_region(region: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch daily weather from Open-Meteo for a region. Returns cached if available."""
    if region not in REGION_COORDS:
        return pd.DataFrame()

    cache_path = CACHE_DIR / f"weather_{region.split(':')[0].strip()}.parquet"

    # Use cache if it covers our date range
    if cache_path.exists():
        cached = pd.read_parquet(cache_path)
        if len(cached) > 0:
            cached_max = cached["date"].max()
            if str(cached_max.date()) >= end_date:
                return cached

    lat, lon = REGION_COORDS[region]
    try:
        resp = httpx.get(OPEN_METEO_URL, params={
            "latitude": lat, "longitude": lon,
            "start_date": start_date, "end_date": end_date,
            "daily": WEATHER_VARS,
            "timezone": "America/Santiago",
        }, timeout=20)
        if resp.status_code != 200:
            return pd.DataFrame()

        data = resp.json().get("daily", {})
        df = pd.DataFrame({
            "date": pd.to_datetime(data.get("time", [])),
            "temp_max": data.get("temperature_2m_max", []),
            "temp_min": data.get("temperature_2m_min", []),
            "precipitation": data.get("precipitation_sum", []),
        })
        df["region"] = region

        # Cache
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)
        return df

    except Exception as e:
        print(f"    Weather fetch failed for {region}: {e}")
        return pd.DataFrame()


def build_weekly_weather(regions: list, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch and aggregate weather to weekly level for all regions."""
    frames = []
    for region in regions:
        daily = fetch_weather_for_region(region, start_date, end_date)
        if len(daily) > 0:
            frames.append(daily)

    if not frames:
        return pd.DataFrame()

    daily_all = pd.concat(frames, ignore_index=True)
    daily_all["week"] = daily_all["date"].dt.to_period("W").dt.start_time

    weekly = daily_all.groupby(["region", "week"]).agg(
        avg_temp=("temp_max", lambda x: (x + daily_all.loc[x.index, "temp_min"]).mean() / 2),
        max_temp=("temp_max", "max"),
        min_temp=("temp_min", "min"),
        total_rain=("precipitation", "sum"),
        rain_days=("precipitation", lambda x: (x > 1.0).sum()),
    ).reset_index()

    return weekly


def add_weather_features(weekly_features: pd.DataFrame, stores: pd.DataFrame,
                         start_date: str = "2024-01-01") -> pd.DataFrame:
    """Add weather features to the weekly feature table.

    Merges on (centro → region, week).
    """
    if "region" not in stores.columns:
        print("    No region column in stores — skipping weather")
        return weekly_features

    # Map centro → region
    store_region = stores[["centro", "region"]].drop_duplicates()
    if "centro" not in store_region.columns:
        return weekly_features

    # Get date range from features
    end_date = str(weekly_features["week"].max().date())
    regions = store_region["region"].unique().tolist()

    print(f"    Fetching weather for {len(regions)} regions ({start_date} to {end_date})...")
    weather = build_weekly_weather(regions, start_date, end_date)

    if len(weather) == 0:
        print("    No weather data available")
        return weekly_features

    # Merge: features → store_region (centro→region) → weather (region, week)
    weekly_features = weekly_features.merge(store_region, on="centro", how="left")
    weekly_features = weekly_features.merge(weather, on=["region", "week"], how="left")

    # Derived: temperature deviation from 4-week rolling average
    if "avg_temp" in weekly_features.columns:
        weekly_features["temp_rolling_avg"] = (
            weekly_features.groupby("centro")["avg_temp"]
            .transform(lambda x: x.rolling(4, min_periods=1).mean())
        )
        weekly_features["temp_deviation"] = weekly_features["avg_temp"] - weekly_features["temp_rolling_avg"]
        weekly_features.drop(columns=["temp_rolling_avg"], inplace=True)

    # Rainy week flag
    weekly_features["is_rainy_week"] = (weekly_features.get("total_rain", 0) > 20).astype(int)

    # Clean up: drop region column (not needed for training)
    if "region" in weekly_features.columns:
        weekly_features.drop(columns=["region"], inplace=True)

    n_matched = weekly_features["avg_temp"].notna().sum()
    print(f"    Weather coverage: {n_matched:,}/{len(weekly_features):,} rows")

    return weekly_features
