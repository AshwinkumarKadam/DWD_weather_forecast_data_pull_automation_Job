import schedule
import time
from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest, DwdMosmixType
from wetterdienst import Settings
import pandas as pd

def fetch_L_weather():
    settings = Settings(ts_shape="wide", ts_humanize=True)

    request = DwdMosmixRequest(
        parameter=["radiation_global","cloud_cover_between_2_to_7_km","cloud_cover_below_1000_ft","FF","TTT"],
        settings=settings,
        mosmix_type=DwdMosmixType.LARGE
    )
    stations = request.filter_by_station_id(station_id=["X009"])
    response = next(stations.values.query())
    cologne_station_df = response.df.to_pandas()
    cologne_station_df['date'] = cologne_station_df['date'].dt.tz_localize(None)
    filename = cologne_station_df.iloc[0]['date'].strftime('L_%Y_%m_%d_%H_%M_%S')
    path = rf'D:\thesis data files\Automated_data_download_MOSMIX_X009\MOSMIX-L\{filename}.xlsx'
    cologne_station_df.to_excel(path, index=False)
    # print(f"MOSMIX-L data saved to: {path}")

def fetch_S_weather():
    settings = Settings(ts_shape="wide", ts_humanize=True)

    request = DwdMosmixRequest(
        parameter=["radiation_global","cloud_cover_between_2_to_7_km","cloud_cover_below_1000_ft","FF","TTT"],
        settings=settings,
        mosmix_type=DwdMosmixType.SMALL
    )
    stations = request.filter_by_station_id(station_id=["X009"])
    response = next(stations.values.query())
    cologne_station_df = response.df.to_pandas()
    cologne_station_df['date'] = cologne_station_df['date'].dt.tz_localize(None)
    filename = cologne_station_df.iloc[0]['date'].strftime('S_%Y_%m_%d_%H_%M_%S')
    path = rf'D:\thesis data files\Automated_data_download_MOSMIX_X009\MOSMIX-S\{filename}.xlsx'
    cologne_station_df.to_excel(path, index=False)
    # print(f"MOSMIX-S data saved to: {path}")

# Schedule each function separately every 5 minutes
schedule.every(30).minutes.do(fetch_L_weather)
schedule.every(15).minutes.do(fetch_S_weather)

while True:
    schedule.run_pending()
    time.sleep(180)

