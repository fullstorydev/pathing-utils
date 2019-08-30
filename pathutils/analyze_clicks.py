"""analyze_clicks.py

A collection of utilities for extracting specific sessions based on what types of user clicks they contain.

"""
import pandas as pd

from pathutils import utils
from collections import defaultdict

CLICKTYPES = ["rage", "error", "dead"]

def click_counts_for_url(df: pd.DataFrame, url: str) -> dict:
    frame = df.loc[df["PageUrl"] == url]
    return click_counts(frame)

def click_counts(df: pd.DataFrame) -> dict:
    frame = df.loc[df["EventType"] == "click"]
    frame_rage = frame.dropna(subset=["EventModFrustrated"])
    frame_dead = frame.dropna(subset=["EventModDead"])
    frame_error = frame.dropna(subset=["EventModError"])
    event_cols = ["EventModFrustrated", "EventModDead", "EventModError"]
    frame_regular = frame[frame[event_cols].isnull().all(1)]
    clicks = {}
    clicks["rage"] = frame_rage.shape[0]
    clicks["dead"] = frame_dead.shape[0]
    clicks["error"] = frame_error.shape[0]
    clicks["normal"] = frame_regular.shape[0]
    return clicks

def build_clicktype_index(df: pd.DataFrame) -> dict:
    sessIndex = defaultdict(list)
    frame = df.loc[df["EventType"] == "click"]
    if "EventModFrustrated" in frame.columns:
        frame_rage = frame.dropna(subset=["EventModFrustrated"])
        sessIndex["rage"] = list(utils.get_sessions(frame_rage))
    else:
        sessIndex["rage"] = []
    if "EventModDead" in frame.columns:
        frame_dead = frame.dropna(subset=["EventModDead"])
        sessIndex["dead"] = list(utils.get_sessions(frame_dead))
    else:
        sessIndex["dead"] = []
    if "EventModError" in frame.columns:
        frame_error = frame.dropna(subset=["EventModError"])
        sessIndex["error"] = list(utils.get_sessions(frame_error))
    else:
        sessIndex["error"] = []
    return sessIndex

def filter_dataset_by_clicktype(df: pd.DataFrame, clicktype: str) -> pd.DataFrame:
    if clicktype not in CLICKTYPES:
        print("Error: unknown click type: " + clicktype)
        return None
    si = build_clicktype_index(df)
    filtered = utils.filter_traffic(df, session=si[clicktype])
    return filtered

def remove_non_navigation(df: pd.DataFrame) -> pd.DataFrame:
    """Removes non-navigation events from the dataframe

    :param df: original dataframe
    :return: dataframe with non-navigation events removed
    """
    df_nav = df.loc[df["EventType"] == "navigate"]
    return df_nav
