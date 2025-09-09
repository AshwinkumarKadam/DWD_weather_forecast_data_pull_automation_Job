"""
weather_fetch.py

This script automates the retrieval of MOSMIX weather forecast data (both Large and Small products)
for station X009 (Cologne) from the Deutscher Wetterdienst (DWD) using the Wetterdienst Python package.

Features:
- Fetches selected meteorological parameters for MOSMIX-L and MOSMIX-S.
- Saves data as timestamped Excel files to user-friendly dynamic directories.
- Schedules automatic data downloads every 30 minutes (MOSMIX-L)
  and every 15 minutes (MOSMIX-S).
- Suitable for running continuously on any OS with Python installed.
- Uses 'schedule' library for task scheduling.

Requirements:
- wetterdienst
- pandas
- schedule
- openpyxl (for Excel export)
  
Before running:
- Install required packages with:
    pip install wetterdienst pandas schedule openpyxl
- Optionally set the environment variable MOSMIX_DATA_DIR 
  to change the base folder for data storage.
  If unset, defaults to ~/mosmix_data directory.
"""

import schedule
import time
import os
from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest, DwdMosmixType
from wetterdienst import Settings
import pandas as pd


# Base directory for saving data files (environment-configurable)
BASE_DIR = os.getenv("MOSMIX_DATA_DIR", os.path.join(os.path.expanduser("~"), "mosmix_data"))


def get_output_dir(product_type: str) -> str:
    """
    Get or create the output directory for given MOSMIX product type.

    Args:
        product_type (str): 'MOSMIX-L' or 'MOSMIX-S'

    Returns:
        str: Full path to directory where Excel files will be saved.
    """
    path = os.path.join(BASE_DIR, product_type)
    os.makedirs(path, exist_ok=True)
    return path


def fetch_L_weather():
    """
    Fetch MOSMIX-L (Large) forecast data for station X009 and save as Excel.

    Fetches a selection of parameters including radiation and cloud cover,
    converts timestamps to naive datetime, and saves to an Excel file with a timestamped filename.
    """
    settings = Settings(ts_shape="wide", ts_humanize=True)

    request = DwdMosmixRequest(
        parameter=[
            "radiation_global",
            "cloud_cover_between_2_to_7_km",
            "cloud_cover_below_1000_ft",
            "FF",  # Wind speed
            "TTT"  # Temperature
        ],
        settings=settings,
        mosmix_type=DwdMosmixType.LARGE,
    )

    stations = request.filter_by_station_id(station_id=["X009"])
    response = next(stations.values.query())
    df = response.df.to_pandas()

    # Localize timezone-naive for consistent timestamp formatting
    df['date'] = df['date'].dt.tz_localize(None)

    # Filename based on first forecast date/time
    filename = df.iloc[0]['date'].strftime('L_%Y_%m_%d_%H_%M_%S')

    output_dir = get_output_dir("MOSMIX-L")
    path = os.path.join(output_dir, f"{filename}.xlsx")

    df.to_excel(path, index=False)

    print(f"[MOSMIX-L] Data saved to: {path}")


def fetch_S_weather():
    """
    Fetch MOSMIX-S (Small) forecast data for station X009 and save as Excel.

    Same structure and parameters as MOSMIX-L but for the smaller, hourly-updated product.
    """
    settings = Settings(ts_shape="wide", ts_humanize=True)

    request = DwdMosmixRequest(
        parameter=[
            "radiation_global",
            "cloud_cover_between_2_to_7_km",
            "cloud_cover_below_1000_ft",
            "FF",  # Wind speed
            "TTT"  # Temperature
        ],
        settings=settings,
        mosmix_type=DwdMosmixType.SMALL,
    )

    stations = request.filter_by_station_id(station_id=["X009"])
    response = next(stations.values.query())
    df = response.df.to_pandas()

    df['date'] = df['date'].dt.tz_localize(None)
    filename = df.iloc[0]['date'].strftime('S_%Y_%m_%d_%H_%M_%S')

    output_dir = get_output_dir("MOSMIX-S")
    path = os.path.join(output_dir, f"{filename}.xlsx")

    df.to_excel(path, index=False)

    print(f"[MOSMIX-S] Data saved to: {path}")


def main():
    """
    Schedule periodic fetching of MOSMIX-L and MOSMIX-S at different intervals.
    
    MOSMIX-L is fetched every 30 minutes.
    MOSMIX-S is fetched every 15 minutes.
    
    The scheduler runs indefinitely until manually stopped.
    """
    schedule.every(30).minutes.do(fetch_L_weather)
    schedule.every(15).minutes.do(fetch_S_weather)

    print(
        f"Scheduler started.\n"
        f"Base directory for data: {BASE_DIR}\n"
        f"MOSMIX-L fetch every 30 minutes; MOSMIX-S fetch every 15 minutes.\n"
        f"Press Ctrl+C to stop."
    )

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check once per minute for pending jobs
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")


if __name__ == "__main__":
    main()
