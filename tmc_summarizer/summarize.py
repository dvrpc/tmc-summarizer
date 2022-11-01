"""
tmc_summarizer.summarize.py
---------------------------

This module automates the import of
all .xls files in a given folder with
``write_summary_file()`` as long as
they meet the criteria defined within
``files_to_process()``

Usage
-----

    In [1]: from tmc_summarizer import write_summary_file

    In [2]: write_summary_file('my/raw/folder', 'my/output/folder')

    Out [2]:
        Reading 150315_US13BristolPikeBathSt.xls
        Reading ...
        Reading 150309_US13BristolPike_and_WalnutAve.xls

        -> Wrote TMC summary to data/cleaned/TMC Summary 2020-07-01 21-22-49.xlsx
        -> Runtime: 0:00:03.622940

"""

import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union
from tmc_summarizer.data_model import TMC_File, geocode_tmc
from tmc_summarizer.helpers import zip_files
import statistics


def files_to_process(folder: Path) -> list:
    """Make a list of files to process. File names must meet
    the following criteria:
        - file ends in ``.xls``
        - filename has at least 1 underscore
        - text before the first underscore can be converted to an integer

    :param folder: folder where files are stored
    :type folder: Path
    :return: list of files that meet criteria
    :rtype: list
    """

    # Get a list of all .xls files in the folder
    files = list(folder.glob("**/*.xls"))

    # Remove any files that don't have proper naming conventions
    for f in files:

        # Make sure there is at least 1 underscore
        if "_" not in str(f.name):
            print(f"No underscores, skipping {f.name}")
            files.remove(f)

        # Make sure that the Location ID is an integer
        parts = str(f.name).split("_")

        try:
            _ = int(parts[0])

        except ValueError:
            print(f"Bad Location ID, skipping {f.name}")
            files.remove(f)

    return files


def get_network_peak_hour_df(df: pd.DataFrame, start, end):
    """Creates a network peak hour summary df.
    Should only be run AFTER all TMCs are created,
    as TMCs are created by intersection peaks, then compiled to network"""
    try:
        df.index = df.index.time
    except:
        df.index = df.index
    # Filter the total dataframe by the start/end times
    df_peak = df.loc[(df.index >= start) & (df.index < end)]

    # Delete the "total_hourly" column as it makes no sense to sum
    del df_peak["total_hourly"]

    return df_peak.sum().to_frame().T


def get_df_peak(df: pd.DataFrame, start, end):
    try:
        df.index = df.index.time
    except:
        df.index = df.index
    # Filter the total dataframe by the start/end times
    df_peak = df.loc[(df.index >= start) & (df.index < end)]
    return df_peak


def df_network_peak_hour_heavy_pct(
    start, end, df_total: pd.DataFrame, df_cars: pd.DataFrame
):
    cars_copy = df_cars.rename(
        columns={
            "EB Peds Xwalk": "EB Xwalk Xings",
            "WB Peds Xwalk": "WB Xwalk Xings",
            "NB Peds Xwalk": "NB Xwalk Xings",
            "SB Peds Xwalk": "SB Xwalk Xings",
        }
    )
    peak_total = get_network_peak_hour_df(df_total, start, end)
    peak_cars = get_network_peak_hour_df(cars_copy, start, end)
    return (1 - peak_cars / peak_total) * 100


def network_peak_hour_factor(df_peak: pd.DataFrame):
    """Returns the NETWORK peak hour factor for a given df_peak dataframe"""
    fifteen_min_peaks = list(df_peak["total_15_min"])
    hourlymax = df_peak["total_hourly"].iat[-1]
    peak_hour_factor = hourlymax / (4 * max(fifteen_min_peaks))
    return peak_hour_factor


def write_summary_file(
    input_folder: Union[Path, str],
    output_folder: Union[Path, str] = None,
    geocode_helper: str = None,
) -> Path:
    """
    Create a new ``.xlsx`` summary file.

    This file has two tabs:
        - ``Summary`` contains a single line-item for each TMC
        - ``Detail`` has 4 line-items per TMC:
            - AM Peak Hour Total
            - AM Peak Hour Percent Heavy Vehicles
            - PM Peak Hour Total
            - PM Peak Hour Percent Heavy Vehicles

    TODO: review this format. Maybe 4 tabs instead of  4 rows?

    Outputs a ZIP file with the Excel file and optional geojson file.

    :param input_folder: folder where TMC data is stored
    :type input_folder: Path
    :param output_folder: folder where output ``.xlsx`` file will be stored
    :type output_folder: Path, optional
    :param geocode_helper: text that gets appended to the location
                           name to assist with geocoding precision.
    :type geocode_helper: str, optional but HIGHLY recommended!
    :return: filepath of the new summary ZIP file
    :rtype: Path
    """
    start_time = datetime.now()

    metadata = []
    detailed_data = []

    # these two lists exist to add the peak hours, in seconds, so they can be averaged for the network later
    am_peak_hour_list = []
    pm_peak_hour_list = []

    # created specifically to grab the actual datetime data for later use in the get_network_peak_hour function
    am_peak_hour_times = []
    pm_peak_hour_times = []

    input_folder = Path(input_folder)

    # Use the specified output folder
    if output_folder:
        output_folder = Path(output_folder)
    # If none is specified, write to the input folder
    else:
        output_folder = Path(input_folder)

    now_txt_1 = start_time.strftime("%Y-%m-%d %H-%M-%S")
    now_txt_2 = start_time.strftime("%Y_%m_%d_%H_%M_%S")

    output_xlsx_filepath = output_folder / ("TMC Summary " + now_txt_1 + ".xlsx")
    output_geojson_filepath = output_folder / (
        "tmc_locations_" + now_txt_2 + ".geojson"
    )
    output_zip_file = output_folder / ("tmc_summary_" + now_txt_2 + ".zip")

    all_tmcs = []

    # Extract dataframes from each file, put into appropriate list
    for file in files_to_process(input_folder):
        tmc = TMC_File(file)

        # Single-row metadata DF
        metadata.append(tmc.df_meta)

        # For each cut listed below, get single-row DF
        # -> (am_total, am_heavy_pct, pm_total, pm_heavy_pct)

        for timeperiod in ["am", "pm"]:
            meta_data_peak = list(tmc.df_meta.loc[:, f"{timeperiod}_peak_raw"])
            time = meta_data_peak[0][0].to_pydatetime()
            seconds = (time.hour * 60 + time.minute) * 60 + time.second
            if timeperiod == "am":
                am_peak_hour_list.append(seconds)
                am_peak_hour_times.append(time)
            elif timeperiod == "pm":
                pm_peak_hour_list.append(seconds)
                pm_peak_hour_times.append(time)
            else:
                print("Not a valid time period")

            for dtype in ["total", "heavy_pct"]:
                identifier = f"{timeperiod}_{dtype}"

                # Grab the appropriate dataframe
                df = tmc.peak_data[identifier]

                # Insert data into extra columns up front
                df.insert(
                    0, "peak_hour_factor", tmc.meta[f"{timeperiod}_peak_hour_factor"]
                )
                df.insert(0, "time", tmc.meta[f"{timeperiod}_peak"])
                df.insert(0, "period", timeperiod)
                df.insert(0, "dtype", dtype)
                df.insert(0, "location_id", tmc.location_id)
                df.insert(0, "location_name", tmc.meta["location_name"])

                detailed_data.append(df)

        all_tmcs.append(tmc)

    # Merge each list of dataframes into its own combined dataframe
    df_meta = pd.concat(metadata)
    df_meta["location_id"] = df_meta["location_id"].astype(int)
    df_meta = df_meta.sort_values("location_id", ascending=True)

    df_detail = pd.concat(detailed_data)
    df_detail["location_id"] = df_detail["location_id"].astype(int)
    df_detail = df_detail.sort_values("location_id", ascending=True)

    # Add network peak hour in a nice format
    am_peak_hr_seconds = statistics.median(am_peak_hour_list)
    am_end = am_peak_hr_seconds + 3600
    pm_peak_hr_seconds = statistics.median(pm_peak_hour_list)
    pm_end = pm_peak_hr_seconds + 3600

    am_network_peak_hour = str(timedelta(seconds=am_peak_hr_seconds))
    am_network_end = str(timedelta(seconds=am_end))
    pm_network_peak_hour = str(timedelta(seconds=pm_peak_hr_seconds))
    pm_network_end = str(timedelta(seconds=pm_end))

    # Add network peak hour TIMES in a usable format for get_network_peak function. specifically returns times, not timedeltas or seconds
    am_network_peak_start_time = am_peak_hour_times[len(am_peak_hour_times) // 2]
    am_network_peak_end_time = am_network_peak_start_time + timedelta(hours=1)
    pm_network_peak_start_time = pm_peak_hour_times[len(pm_peak_hour_times) // 2]
    pm_network_peak_end_time = pm_network_peak_start_time + timedelta(hours=1)

    df_meta = df_meta.drop(columns=["am_peak_raw", "pm_peak_raw"])
    df_meta.insert(
        4, "pm_network_peak", (f"{pm_network_peak_hour} to {pm_network_end}")
    )
    df_meta.insert(
        4, "am_network_peak", (f"{am_network_peak_hour} to {am_network_end}")
    )
    df_meta = df_meta.drop(columns=["am_peak_hour_factor", "pm_peak_hour_factor"])

    # Clear data from detail, fill in by looking up network peak hour and peak hour factor
    df_meta = df_meta.set_index("location_id")
    df_detail = df_detail.reset_index(drop=True)

    tmc_dfs = {
        "am_dict": {},
        "pm_dict": {},
    }  # Makes a dict of one-row dataframes that contains the volumes using the NETWORK peak hour instead of intersection peak hour
    heavy_vehicle_dfs = {
        "am_dict": {},
        "pm_dict": {},
    }  # Same as above but for percentages, not volumes
    peak_hr_factors = {
        "am_dict": {},
        "pm_dict": {},
    }  # Same as above but for peak hour factors
    car_dfs = {
        "am_dict": {},
        "pm_dict": {},
    }  # Same as above but for peds in xwalk (which lives in cars tab)
    heavy_vehicle_for_bikes_dfs = {
        "am_dict": {},
        "pm_dict": {},
    }  # Same as above but for bikes in xwalk (which lives in heavy vehicles tab)

    for tmc in all_tmcs:
        tmc_id = tmc.location_id
        tmc_id = int(tmc_id)

        am_df = get_network_peak_hour_df(
            tmc.df_total,
            am_network_peak_start_time.time(),
            am_network_peak_end_time.time(),
        )
        pm_df = get_network_peak_hour_df(
            tmc.df_total,
            pm_network_peak_start_time.time(),
            pm_network_peak_end_time.time(),
        )

        am_cars_df = get_network_peak_hour_df(
            tmc.df_cars,
            am_network_peak_start_time.time(),
            am_network_peak_end_time.time(),
        )

        pm_cars_df = get_network_peak_hour_df(
            tmc.df_cars,
            pm_network_peak_start_time.time(),
            pm_network_peak_end_time.time(),
        )
        am_heavy_df = get_network_peak_hour_df(
            tmc.df_heavy,
            am_network_peak_start_time.time(),
            am_network_peak_end_time.time(),
        )

        pm_heavy_df = get_network_peak_hour_df(
            tmc.df_heavy,
            pm_network_peak_start_time.time(),
            pm_network_peak_end_time.time(),
        )

        tmc_dfs["am_dict"][tmc_id] = am_df  # nests am_df into tmc_dfs dict
        tmc_dfs["pm_dict"][tmc_id] = pm_df

        am_hv_pc = df_network_peak_hour_heavy_pct(
            am_network_peak_start_time.time(),
            am_network_peak_end_time.time(),
            tmc.df_total,
            tmc.df_cars,
        )
        pm_hv_pc = df_network_peak_hour_heavy_pct(
            pm_network_peak_start_time.time(),
            pm_network_peak_end_time.time(),
            tmc.df_total,
            tmc.df_cars,
        )
        heavy_vehicle_dfs["am_dict"][
            tmc_id
        ] = am_hv_pc  # nests heavy vehicle percentages into hv dict
        heavy_vehicle_dfs["pm_dict"][tmc_id] = pm_hv_pc

        am_network_peak_hour_factor = network_peak_hour_factor(
            get_df_peak(
                tmc.df_total,
                am_network_peak_start_time.time(),
                am_network_peak_end_time.time(),
            )
        )
        pm_network_peak_hour_factor = network_peak_hour_factor(
            get_df_peak(
                tmc.df_total,
                pm_network_peak_start_time.time(),
                pm_network_peak_end_time.time(),
            )
        )

        # nests dfs into peak_hr_factors dict
        peak_hr_factors["am_dict"][tmc_id] = am_network_peak_hour_factor
        peak_hr_factors["pm_dict"][tmc_id] = pm_network_peak_hour_factor

        # drop car and heavy vehicle one line dfs into dicts
        car_dfs["am_dict"][tmc_id] = am_cars_df
        car_dfs["pm_dict"][tmc_id] = pm_cars_df
        heavy_vehicle_for_bikes_dfs["am_dict"][tmc_id] = am_heavy_df
        heavy_vehicle_for_bikes_dfs["pm_dict"][tmc_id] = pm_heavy_df

    df_detail.loc[df_detail["period"] == "am", "time"] = df_meta.at[
        1, "am_network_peak"
    ]
    df_detail.loc[df_detail["period"] == "pm", "time"] = df_meta.at[
        1, "pm_network_peak"
    ]

    def update_time_period_totals(key, time: str):
        condition = (
            (df_detail["period"] == f"{time}")
            & (df_detail["dtype"] == "total")
            & (df_detail["location_id"] == key)
        )
        df_detail.loc[condition, "EB U":"total_15_min"] = tmc_dfs[f"{time}_dict"][
            key
        ].values

    def update_time_period_heavy_vehicles(key, time: str):
        condition = (
            (df_detail["period"] == f"{time}")
            & (df_detail["dtype"] == "heavy_pct")
            & (df_detail["location_id"] == key)
        )
        df_detail.loc[condition, "EB U":"total_15_min"] = heavy_vehicle_dfs[
            f"{time}_dict"
        ][key].values

    def update_peak_hour_factors(key, time: str):
        condition = (
            (df_detail["period"] == f"{time}")
            & (df_detail["location_id"] == key)
            & (df_detail["dtype"] == "total")
        )
        condition2 = (
            (df_detail["period"] == f"{time}")
            & (df_detail["location_id"] == key)
            & (df_detail["dtype"] == "heavy_pct")
        )
        df_detail.loc[condition, "peak_hour_factor"] = peak_hr_factors[f"{time}_dict"][
            key
        ]
        df_detail.loc[condition2, "peak_hour_factor"] = 0

    def update_bike_ped_info(key, time: str):
        """grabs correct bike/ped info from the cars and heavy vehicles sheets, respectively"""
        condition = (
            (df_detail["period"] == f"{time}")
            & (df_detail["location_id"] == key)
            & (df_detail["dtype"] == "total")
        )
        condition2 = (
            (df_detail["period"] == f"{time}")
            & (df_detail["location_id"] == key)
            & (df_detail["dtype"] == "heavy_pct")
        )
        directions = ["EB", "WB", "NB", "SB"]
        for direction in directions:
            df_detail.loc[
                condition, f"{direction} Bikes Xwalk"
            ] = heavy_vehicle_for_bikes_dfs[f"{time}_dict"][key][
                f"{direction} Bikes Xwalk"
            ][
                0
            ]
            df_detail.loc[condition2, f"{direction} Bikes Xwalk"] = 0

        for direction in directions:
            df_detail.loc[condition, f"{direction} Peds Xwalk"] = car_dfs[
                f"{time}_dict"
            ][key][f"{direction} Peds Xwalk"][0]
            df_detail.loc[condition2, f"{direction} Peds Xwalk"] = 0

    for key in tmc_dfs["am_dict"]:
        update_time_period_totals(key, "am")
        update_time_period_heavy_vehicles(key, "am")
        update_peak_hour_factors(key, "am")
        update_bike_ped_info(key, "am")

    for key in tmc_dfs["pm_dict"]:
        update_time_period_totals(key, "pm")
        update_time_period_heavy_vehicles(key, "pm")
        update_peak_hour_factors(key, "pm")
        update_bike_ped_info(key, "pm")

    df_detail = df_detail.rename(
        columns={
            "EB Xwalk Xings": "EB Bikes Xwalk",
            "WB Xwalk Xings": "WB Bikes Xwalk",
            "NB Xwalk Xings": "NB Bikes Xwalk",
            "SB Xwalk Xings": "SB Bikes Xwalk",
        },
        errors="raise",
    )
    reordered_cols = [
        "location_name",
        "location_id",
        "dtype",
        "period",
        "time",
        "peak_hour_factor",
        "EB U",
        "EB Left",
        "EB Thru",
        "EB Right",
        "EB Peds Xwalk",
        "EB Bikes Xwalk",
        "WB U",
        "WB Left",
        "WB Thru",
        "WB Right",
        "WB Peds Xwalk",
        "WB Bikes Xwalk",
        "NB U",
        "NB Left",
        "NB Thru",
        "NB Right",
        "NB Peds Xwalk",
        "NB Bikes Xwalk",
        "SB U",
        "SB Left",
        "SB Thru",
        "SB Right",
        "SB Peds Xwalk",
        "SB Bikes Xwalk",
        "total_15_min",
    ]
    # have to do this after reorder/renames
    for key in tmc_dfs["am_dict"]:
        update_bike_ped_info(key, "am")

    for key in tmc_dfs["pm_dict"]:
        update_bike_ped_info(key, "pm")

    df_detail = df_detail[reordered_cols]
    df_detail = df_detail.rename(columns={"total_15_min": "total_60_min"})

    # removes duplicate {direction} bike columns (can't figure out where they came from, but they're the exact same)
    df_detail = df_detail.loc[:, ~df_detail.columns.duplicated()].copy()

    # Write Summary and Detail tabs out to file
    writer = pd.ExcelWriter(output_xlsx_filepath, engine="xlsxwriter")

    workbook = writer.book
    header_format = workbook.add_format({"bold": True, "font_size": 18})

    df_meta.to_excel(writer, sheet_name="Summary")
    df_detail.to_excel(writer, sheet_name="Detail")

    writer.sheets["Summary"].set_column(1, 15, 18)
    writer.sheets["Detail"].set_column(1, 15, 20)

    # Write raw data tabs
    all_tmcs.sort(key=lambda x: int(x.location_id))
    for tmc in all_tmcs:

        kwargs = {"sheet_name": tmc.location_id, "startrow": 1, "startcol": 0}

        tmc.df_total.to_excel(writer, **kwargs)

        # Add titles to Row 1 as we go...
        worksheet = writer.sheets[tmc.location_id]
        worksheet.write(0, kwargs["startcol"], "TOTAL Vehicles", header_format)
        worksheet.set_column(kwargs["startcol"], kwargs["startcol"], 21)

        kwargs["startcol"] += 24

        tmc.df_heavy.to_excel(writer, **kwargs)
        worksheet.write(0, kwargs["startcol"], "HEAVY Vehicles", header_format)
        worksheet.set_column(kwargs["startcol"], kwargs["startcol"], 21)

        kwargs["startcol"] += 24

        tmc.df_pct_heavy.to_excel(writer, **kwargs)
        worksheet.write(0, kwargs["startcol"], "PERCENT HEAVY Vehicles", header_format)
        worksheet.set_column(kwargs["startcol"], kwargs["startcol"], 21)

    writer.save()
    print(f"\n-> Wrote TMC summary to {output_xlsx_filepath}")

    # files_to_zip = [output_xlsx_filepath]

    # Geocode the data and save a shapefile if the helper is provided
    # ---------------------------------------------------------------

    if geocode_helper:

        # Geocode the location
        for tmc in all_tmcs:
            lat, lon, _ = geocode_tmc(tmc, geocode_helper)
            tmc.meta["lat"] = lat
            tmc.meta["lon"] = lon

        # Create a shapefile of the locations
        df = pd.DataFrame([tmc.meta for tmc in all_tmcs])
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))

        gdf.to_file(output_geojson_filepath, driver="GeoJSON")
        print(f"-> Wrote point geojson to {output_geojson_filepath}")

        # files_to_zip.append(output_geojson_filepath)

    # print(f"-> Zipping output files up...")
    # zip_files(output_zip_file, files_to_zip)

    end_time = datetime.now()

    runtime = end_time - start_time
    print(f"-> Runtime: {runtime}")

    return output_xlsx_filepath, output_geojson_filepath


if __name__ == "__main__":

    # local filepaths
    project_root = Path("/Volumes/SanDisk2TB/code/turning-movement-count-summarizer")
    data_folder = project_root / "data" / "cleaned"
    output_folder = project_root / "data" / "outputs_aaron"

    # Execute!
    write_summary_file(data_folder, output_folder)
