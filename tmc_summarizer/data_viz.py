import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px


def read_sql(query: str, uri: str, index_col="time") -> pd.DataFrame:
    """
    Use sqlalchemy to turn a SQL query into a pandas dataframe

    Parameters
    ----------
        - query: any valid SQL query
        - uri: db connection string
        - index_col: pandas index for the returned df

    Returns
    -------
        - dataframe of whatever you queried
    """

    engine = create_engine(uri)
    df = pd.read_sql(query, engine, index_col=index_col)
    engine.dispose()

    return df


def generate_timeseries_data(table_name: str,
                             uri: str,
                             start_time: str = "5:00",
                             end_time: str = "20:00",
                             fids_to_include: list = [x for x in range(14)],
                             modes_to_include: list = ["heavy", "light", "bikes", "peds"]):
    """
    This function creates a dataframe that is tailored to plotly.express.bar()
    """

    df_sample = read_sql(f"SELECT * FROM {table_name} LIMIT 1", uri)

    fids_to_include = [str(x) for x in fids_to_include]

    q = "SELECT time, fid, "

    for col in df_sample.columns:
        if col != 'fid':

            # Confirm it's in the mode list
            wt = col.split("_")[0]
            if wt in modes_to_include:
                q += f" {col}, "

    q = q[:-2] + f" FROM {table_name}"

    q += f"""
        WHERE time >= '{start_time}'
            AND time < '{end_time}'
            AND fid IN ({", ".join(fids_to_include)}) """

    df = read_sql(q, uri)

    return df


def generate_treemap_data(df_timeseries):
    """
    This function consumes the dataframe from df_timeseries()
    and transforms it to fit the plotly.express.treemap()
    """

    df_timeseries = df_timeseries.copy()

    # Update with a multi-index to include the file id, then remove the fid column
    df_timeseries.set_index([df_timeseries.fid, df_timeseries.index], inplace=True)
    del df_timeseries["fid"]

    # Stack the dataframe, then reset_index() to explode the multi-index into cols
    df_stacked = pd.DataFrame(
        df_timeseries.stack(), columns=["total"]
    ).reset_index()

    # Remove all rows where the total is zero
    df_filtered = df_stacked[df_stacked.total != 0]

    # Explode the
    attribute_cols = {
        0: "veh_class",
        1: "leg",
        2: "movement"
    }
    df_attrs = df_filtered['level_2'].str.split(
        '_', expand=True
    ).rename(columns=attribute_cols)

    df = pd.concat([df_filtered, df_attrs], axis=1, sort=False)
    df["hour"] = [x.hour for x in df["time"]]
    df["minute"] = [":" + str(x.minute) for x in df["time"]]

    for col in ["level_2", "time"]:
        del df[col]

    return df


def timeseries_figure(df_timeries,
                      plot_kwargs: dict = {
                        "facet_col": "fid",
                        "facet_col_wrap": 2,
                        "color_discrete_sequence": px.colors.qualitative.Dark24
                       }):
    fig = px.bar(df_timeries, **plot_kwargs)
    return fig


def treemap_figure(df_treemap,
                   figure_margin: dict = dict(l=20, r=20, t=20, b=20),
                   plot_kwargs: dict = {
                       "values": "total",
                       "path": ["veh_class", "fid", "leg", "movement", "hour", "minute"],
                    }):
    fig = px.treemap(df_treemap, **plot_kwargs)
    fig.update_layout(margin=figure_margin)
    return fig
