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
            elif timeperiod == "pm":
                pm_peak_hour_list.append(seconds)
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
    df_detail = pd.concat(detailed_data)

    # Add network peak hour in a nice format
    am_peak_hr_seconds = statistics.median(am_peak_hour_list)
    am_end = am_peak_hr_seconds + 3600
    pm_peak_hr_seconds = statistics.median(pm_peak_hour_list)
    pm_end = pm_peak_hr_seconds + 3600

    am_network_peak_hour = str(timedelta(seconds=am_peak_hr_seconds))
    am_network_end = str(timedelta(seconds=am_end))
    pm_network_peak_hour = str(timedelta(seconds=pm_peak_hr_seconds))
    pm_network_end = str(timedelta(seconds=pm_end))

    df_meta = df_meta.drop(columns=["am_peak_raw", "pm_peak_raw"])
    df_meta.insert(
        4, "pm_network_peak", (f"{pm_network_peak_hour} to {pm_network_end}")
    )
    df_meta.insert(
        4, "am_network_peak", (f"{am_network_peak_hour} to {am_network_end}")
    )
    df_meta = df_meta.drop(columns=["am_peak_hour_factor", "pm_peak_hour_factor"])

    # Write Summary and Detail tabs out to file
    writer = pd.ExcelWriter(output_xlsx_filepath, engine="xlsxwriter")

    workbook = writer.book
    header_format = workbook.add_format({"bold": True, "font_size": 18})

    df_meta.to_excel(writer, sheet_name="Summary")
    df_detail.to_excel(writer, sheet_name="Detail")

    writer.sheets["Summary"].set_column(1, 15, 18)
    writer.sheets["Detail"].set_column(1, 15, 20)

    # Write raw data tabs
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
