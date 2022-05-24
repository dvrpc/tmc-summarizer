"""
tmc_summarizer.data_model.py
----------------------------

This module defines the ``TMC_File`` class.
See class docstring for more info.

Usage
-----

    In [1]: from tmc_summarizer import TMC_File

    In [2]: tmc = TMC_File('data/cleaned/150314_US13BristolPikeCommerceDr.xls')

    In [3]: tmc.meta

    Out [3]:{'location_id': '150314',
             'location_name': 'US 13 Bristol Pike and Commerce Cir',
             'date': '2019-08-27',
             'time': '05:45 to 19:15',
             'am_peak': '07:00 to 08:00',
             'pm_peak': '16:30 to 17:30',
             'leg_nb': 'Commerce Cir',
             'leg_sb': 'Commerce Cir',
             'leg_eb': 'US 13 Bristol Pike ',
             'leg_wb': 'US 13 Bristol Pike',
             'filepath': 'data/cleaned/150314_US13BristolPikeCommerceDr.xls'}

    In [4]: tmc.peak_data['am_total'].T

    Out [4]:    SB U              0
                SB Left          38
                SB Thru           0
                SB Right          8
                SB Peds Xwalk     0
                WB U              0
                WB Left           0
                WB Thru         695
                WB Right         89
                WB Peds Xwalk     0
                NB U              0
                NB Left          16
                NB Thru           7
                NB Right          4
                NB Peds Xwalk     0
                EB U              1
                EB Left          21
                EB Thru         775
                EB Right          0
                EB Peds Xwalk     0
                total_15_min   1654

"""
import os
import pandas as pd
from pathlib import Path
from typing import Union
from datetime import datetime, time, timedelta
import googlemaps
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

GMAPS_API_KEY = os.getenv("GMAPS_API_KEY")


class TMC_File:
    """
    This class encapsulates all aspects of a single
    Turning-Movement-Count (TMC) file.

    The model assumes a standardized format, as documented
    within the ``test_data`` folder within this repository.

    Functionality
    -------------
        - upon creation, load metadata from the ``Information`` tab
        - load the three data tabs into their own dataframes
        - use matrix math to calculate the percent heavy dataframe
        - identify AM and PM peak hours
        - sum the four 15-minute blocks in the AM and PM peaks
    """

    def __init__(self, filepath: Union[Path, str], geocode_helper: str = None):

        # Ensure filepath is a Path type and extract the ID
        # i.e. file name = "150315_ProjectNamePlaceName.xls"
        # --------------------------------------------------

        self.filepath = Path(filepath)
        self.location_id = self.filepath.name.split("_")[0]

        print("Reading", self.filepath.name)

        # Load the INFORMATION tab data on place names
        # --------------------------------------------

        location_kwargs = {
            "sheet_name": "Information",
            "header": None,
            "usecols": "A:B",
            "names": ["place_type", "place_name"],
        }

        self.df_info_location = pd.read_excel(self.filepath, **location_kwargs).dropna()

        # Load the INFORMATION tab data on date / time
        # --------------------------------------------
        location_kwargs["skiprows"] = 1
        location_kwargs["usecols"] = "D:G"
        location_kwargs["names"] = ["count_no", "co_no_2", "date", "time"]

        self.df_info_time = pd.read_excel(self.filepath, **location_kwargs)
        self.df_info_time = self.df_info_time[self.df_info_time["count_no"].notna()]
        self.df_info_time.drop(
            self.df_info_time.tail(2).index, inplace=True
        )  # drop last n rows
        self.df_info_time.drop(
            columns=["co_no_2"], inplace=True
        )  # deals with exttra column due to merged cells
        self.df_info_time = self.df_info_time.reset_index(inplace=False)
        self.df_info_time.drop(columns=["index"], inplace=True)

        # Parse data from the INFO tab
        # ----------------------------

        self.date = None
        self.start_time = ""
        self.end_time = ""
        self.location_name = ""
        self.legs = {}

        # Get the location_name and leg names
        for _, row in self.df_info_location.iterrows():

            if row.place_type == "Intersection Name":
                self.location_name = row.place_name
            elif row.place_type == "City":
                self.city_name = row.place_name
            elif row.place_type == "State":
                self.state_name = row.place_name
            else:
                self.legs[row.place_type.upper()] = row.place_name

        # Get the date and start/end times
        for _, row in self.df_info_time.iterrows():

            if row.count_no == "Date and Time of Start of Count 1":
                self.date = row.date
                self.start_time = row.time
            elif row.count_no == "Date and Time of End of Count 1":
                self.end_date = row.date
                self.end_time = row.time

        # Read the DATA tabs into dataframes
        # ----------------------------------

        self.df_cars = self.read_data_tab("Cars")
        self.df_heavy = self.read_data_tab("Heavy Vehicles")
        # self.bikes = self.read_data_tab("Bicycles")
        self.df_total = self.read_data_tab("TOTAL")

        # # Calculate the percent heavy dataframe
        # # -------------------------------------
        self.df_pct_heavy = (1 - self.df_cars / self.df_total) * 100

        # # Expose all metadata as a dictionary and dataframe
        # # -------------------------------------------------

        self.meta = {
            "location_id": self.location_id,
            "location_name": self.location_name,
            "date": self.date.strftime("%Y-%m-%d"),
            "time": f"{self.start_time} to {self.end_time}",
            "am_peak": self.peak_hour_text("AM"),
            "pm_peak": self.peak_hour_text("PM"),
        }

        for direction in ["NORTH", "SOUTH", "EAST", "WEST"]:
            direction = f"{direction}BOUND STREET"
            if direction in self.legs:
                meta_name = f"leg_{direction[0].lower()}b"
                self.meta[meta_name] = self.legs[direction]

        self.meta["filepath"] = str(self.filepath)

        self.df_meta = pd.Series(self.meta).to_frame().T

        # Collect the peak hour data into a dictionary of dataframes
        # ----------------------------------------------------------

        self.peak_data = {
            "am_total": self.df_peak_hour(self.df_total, "AM"),
            "am_heavy_pct": self.df_peak_hour_heavy_pct("AM"),
            "pm_total": self.df_peak_hour(self.df_total, "PM"),
            "pm_heavy_pct": self.df_peak_hour_heavy_pct("PM"),
        }

    def read_data_tab(self, tabname: str) -> pd.DataFrame:
        """
        Generic function to read data from any of the vehicle tabs.
        """

        df = pd.read_excel(
            self.filepath,
            skiprows=3,
            header=None,
            names=self.flatten_headers(tabname),
            sheet_name=tabname
        ).dropna(subset=['SB U'])

        df["datetime"] = None

        for idx, row in df.iterrows():
            date_holder = self.date
            date_holder = datetime.date(date_holder)
            try:
                row.time = datetime.time(row.time)
            except: 
                row.time = row.time
            df.at[idx, "datetime"] = datetime.combine(self.date, row.time)

        del df["time"]
        del df["date"]

        # Set the dataframe index to the timestamp
        df = df.set_index("datetime")

        df = self.add_15_min_totals(df)
        df = self.add_hourly_totals(df)

        return df

    def flatten_headers(self, tabname: str) -> list:
        """
        Transform a multi-level header into a single header row.

        For example:
            - 'Southbound / U Turns' becomes 'SB U'
            - 'Eastbound / Straight Through' becomes 'EB Thru'
        """

        replacements_level_1 = {
            "Southbound": "SB ",
            "Westbound": "WB ",
            "Northbound": "NB ",
            "Eastbound": "EB ",
        }

        replacements_level_2 = {
            "u turns": "U",
            "left turns": "Left",
            "straight through": "Thru",
            "right turns": "Right",
            "ped crossings": "Peds Xwalk",
            "bikes in crosswalk": "Bikes Xwalk",
            "bicycles in crosswalk": "Bikes Xwalk",
            "time": "time",
            "date": "date",
            # handle the expected typos!
            "bikes in croswalk": "Bikes Xwalk",
            "peds in croswalk": "Peds Xwalk",
            "crosswalk crossings": "Xwalk Xings",
        }

        df = pd.read_excel(self.filepath, nrows=3, header=None, sheet_name=tabname)
        headers = []

        # Start off with a blank l1
        l1 = ""

        for col in df.columns:
            level_1 = df.at[1, col]
            level_2 = df.at[2, col]
            # Update the l1 anytime a value is found
            if not pd.isna(level_1):
                l1 = replacements_level_1[level_1]

            # Warn the user if the file has unexpected headers!
            # If it does, use the raw value instead of our nicely formatted one
            if level_2.lower() in replacements_level_2:
                l2 = replacements_level_2[level_2.lower()]
            else:
                msg = f"!!! '{level_2}' isn't included in the lookup. It won't be renamed."
                print(msg)
                l2 = level_2

            headers.append(l1 + l2)

        return headers

    def add_15_min_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a column named 'total_15_min' that sums volumes
        from each of the other columns, for each row.

        :param df: input dataframe
        :type df: pd.DataFrame
        :return: modified dataframe with new column
        :rtype: pd.DataFrame
        """
        # TODO: qa that this is right from Excel

        # For each row, sum all columns and put the result into "total_15_min"
        df["total_15_min"] = df.iloc[:, :].sum(axis=1)

        return df

    def add_hourly_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a column named 'total_hourly' that sums 15-minute
        volumes for a 1-hour period. It works backwards, so the
        value on any row represents that 15-minute timeperiod plus
        the three other 15-minute blocks prior.

        :param df: input dataframe
        :type df: pd.DataFrame
        :return: modified dataframe with new column
        :rtype: pd.DataFrame
        """

        # Get the column index for the 15 minute totals
        col_idx_15_min = df.columns.get_loc("total_15_min")

        df["total_hourly"] = 0

        for idx, row in df.iterrows():
            end = idx + timedelta(minutes=15)
            start = end - timedelta(hours=1)

            hourly_total = df.iloc[
                (df.index >= start) & (df.index < end), col_idx_15_min
            ].sum()

            df.at[idx, "total_hourly"] = hourly_total

        return df

    def get_peak_hour(self, period: str) -> (datetime, datetime):
        """
        Identify start / end times of the AM or PM peak hour.
        Return this information as a tuple with each value as a datetime.

        :param period: time of day, either 'AM' or 'PM'
        :type period: str
        :return: tuple with (start_time, end_time)
        :rtype: tuple with datetime
        """

        period = period.upper()

        noon = datetime.combine(self.date, time(hour=12))

        if period == "AM":
            df = self.df_total[(self.df_total.index < noon)]
        elif period == "PM":
            df = self.df_total[(self.df_total.index >= noon)]
        else:
            print("Period must be AM or PM")
            return

        final_15_min = df[["total_hourly"]].idxmax()[0]

        end = final_15_min + timedelta(minutes=15)
        start = end - timedelta(hours=1)

        return start, end

    def peak_hour_text(self, period: str) -> str:
        """
        Make a nice text format for the peak hour range. Choose AM or PM.

        :param period: time of day, either 'AM' or 'PM'
        :type period: str
        :return: text of peak hour
        :rtype: str
        """
        start, end = self.get_peak_hour(period)

        fmt = "%H:%M"

        return f"{start.strftime(fmt)} to {end.strftime(fmt)}"

    def df_peak_hour(self, df: pd.DataFrame, period: str) -> pd.DataFrame:

        start, end = self.get_peak_hour(period)

        # Filter the total dataframe by the start/end times
        df_peak = df.loc[(df.index >= start) & (df.index < end)]

        # Delete the "total_hourly" column as it makes no sense to sum
        del df_peak["total_hourly"]

        return df_peak.sum().to_frame().T

    def df_peak_hour_heavy_pct(self, period: str) -> pd.DataFrame:
        peak_total = self.df_peak_hour(self.df_total, period)
        peak_light = self.df_peak_hour(self.df_cars, period)

        return (1 - peak_light / peak_total) * 100

    def all_raw_data(self, summary_col: bool = False) -> pd.DataFrame:
        """Combine the light and heavy tables together
        into one table with LOTS of columns.
        """

        # Add Light or Heavy as a prefix to the column names
        df_cars = self.df_cars.rename(
            columns={old: "Light " + old for old in self.df_cars.columns}
        )
        df_heavy = self.df_heavy.rename(
            columns={old: "Heavy " + old for old in self.df_heavy.columns}
        )

        # Concatenate the two dataframes together. They share the same index.
        df = pd.concat([df_cars, df_heavy], axis=1, sort=False)

        if summary_col:
            # Remove all columns except the 'total_15_min' columns
            for col in df.columns:
                if col not in ["Heavy total_15_min", "Light total_15_min"]:
                    df.drop(col, axis=1, inplace=True)

        else:
            # Remove the 'total_' columns
            for col in ["total_15_min", "total_hourly"]:
                for wt in ["Light", "Heavy"]:
                    df.drop(f"{wt} {col}", axis=1, inplace=True)

        return df

    def filter_df_by_start_end_time(
        self,
        df,
        start_time: str = "7:00",
        end_time: str = "12:00",
        as_type: str = "datetime",
    ):
        # Parse the text input into usable values
        # i.e. '5:15' -> 5, 15
        a_hr, a_min = start_time.split(":")
        b_hr, b_min = end_time.split(":")

        # Build a datetime for the start and end windows
        a_time = time(hour=int(a_hr), minute=int(a_min))
        b_time = time(hour=int(b_hr), minute=int(b_min))

        if as_type == "datetime":
            a = datetime.combine(self.date, a_time)
            b = datetime.combine(self.date, b_time)

        else:
            a = a_time
            b = b_time

        # Filter the raw data by start and end windows
        dff = df[(df.index >= a) & (df.index < b)]

        return dff

    def treemap_df(self,
                   start_time: str = "7:00",
                   end_time: str = "12:00") -> pd.DataFrame:
        """
        Create a dataframe that can be dropped into a plotly treemap figure.

        Using a start and end time filter, provide a single number for
        movements, broken out by weight, leg, and movement.

        Usage
        -----
            >>> tmc = TMC_File('my/path.xls')
            >>> df = tmc.treemap_df('3:00', '15:15')
            >>> fig = px.treemap(df, path=['wt', 'leg', 'movement'], values=0)

        """

        # Get the full raw dataset
        df = self.all_raw_data()

        dff = self.filter_df_by_start_end_time(df, start_time=start_time, end_time=end_time)

        # Sum all values so there's one row instead of one for every 15 minutes
        treemap_df = pd.DataFrame(dff.sum())

        treemap_df.rename(columns={0: "total"}, inplace=True)

        # Add labeling columns
        treemap_df["full_movement"] = treemap_df.index
        treemap_df["wt"] = ""
        treemap_df["leg"] = ""
        treemap_df["movement"] = ""

        # Parse out the values for each row
        for idx, row in treemap_df.iterrows():
            split_value = idx.split(" ")

            wt = split_value[0]
            leg = split_value[1]
            movement = " ".join(split_value[2:])

            if 'Peds' in movement or 'Bikes' in movement:
                wt = movement.split(' ')[0]
                movement = 'Xwalk'

            treemap_df.at[idx, "wt"] = wt
            treemap_df.at[idx, "leg"] = leg
            treemap_df.at[idx, "movement"] = movement

        return treemap_df


def geocode_tmc(tmc: TMC_File, geocode_helper: str):
    """
    Use Aaron's secret Google Maps API key to geocode
    the location of this count. Adding a 'geocode_helper'
    when creating the TMC_File object will help improve the
    accuracy of the result.

    :return: lat, lon, and full geocode result
    :rtype: thruple
    """

    geocode_txt = tmc.location_name + ", " + geocode_helper

    gmaps = googlemaps.Client(key=GMAPS_API_KEY)
    result = gmaps.geocode(geocode_txt)

    lat = result[0]["geometry"]["location"]["lat"]
    lon = result[0]["geometry"]["location"]["lng"]

    return (lat, lon, result)
