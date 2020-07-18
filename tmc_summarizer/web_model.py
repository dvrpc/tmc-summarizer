import pandas as pd
from pathlib import Path
from datetime import time
from sqlalchemy import create_engine, Integer


from .business_logic import flatten_headers


class TMC_Webapp_Upload_File:
    """
    Streamlined version of the main data model.

    The purpose here is to efficiently extract
    the data from Excel into SQL.

    """

    def __init__(self,
                 project_id: int,
                 file_id: int,
                 filepath: Path,):

        self._pid = project_id
        self._fid = file_id
        self._filepath = filepath

    def read_data(self,
                  tabname: str,
                  col_prefix: str):
        """
        Minimalist approach to reading the XLS files.

        Parameters
        ----------
            - tabname: name of tab in the excel file
            - col_prefix: whatever you want to use at
                          the beginning of the column names.
        """

        df = pd.read_excel(self._filepath,
                           skiprows=3,
                           header=None,
                           names=flatten_headers(self._filepath, tabname),
                           sheet_name=tabname).dropna()

        # Check all time values and ensure that each one
        # is formatted as a datetime.time. Some aren't by default!
        for idx, row in df.iterrows():

            if type(row.time) != time:
                hour, minute = row.time.split(":")
                df.at[idx, "time"] = time(
                    hour=int(hour),
                    minute=int(minute)
                )

        # Reindex on the time column
        df.set_index("time", inplace=True)

        # Clean up column names for SQL
        #  Prefix all columns. i.e. "SB U" -> "Light SB U"
        new_cols = {}
        for col in df.columns:
            nice_prefix = col_prefix.lower()
            nice_col = col.replace(" ", "_").lower()

            # Special handling for bikes and peds
            # If it's one of these, set it as the mode
            for val in ["bikes_", "peds_"]:
                if val in nice_col:
                    nice_col = nice_col.replace(val, "")
                    nice_prefix = val[:-1]

            new_cols[col] = f"{nice_prefix}_{nice_col}"

        df.rename(
            columns=new_cols,
            inplace=True
        )

        return df

    def spliced_light_and_heavy_df(self):

        df_light = self.read_data("Light Vehicles", "Light")
        df_heavy = self.read_data("Heavy Vehicles", "Heavy")

        df = pd.concat([df_light, df_heavy], axis=1, sort=False)

        df["fid"] = int(self._fid)

        return df

    def publish_to_database(self,
                            db_uri: str,
                            df: pd.DataFrame = None,
                            pg_table_name: str = None,):

        if not df:
            df = self.spliced_light_and_heavy_df()
        if not pg_table_name:
            pg_table_name = f"data_{self._pid}_raw"

        engine = create_engine(db_uri)

        kwargs = {
            "if_exists": "append",
            # "dtype": {col: Integer() for col in df.columns}
        }

        df.to_sql(pg_table_name, engine, **kwargs)

        engine.dispose()
