"""analyze_traffic.py

A collection of useful functions for analyzing events data in the context of user pathing.

Most functions would work with a pandas dataframe that will be indexed by sid (User ID + Session ID)

Now a part of pathutils package

Author: Gregory Larchev
Date: May 30, 2019
"""

import matplotlib

try:
    matplotlib.use("TkAgg")
except:
    print("Looks like your system doesn't support TkAgg backend. If you're running the script from the command line, \
          there is a small chance plots won't display correctly.")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os

from pathutils import analyze_clicks
from pathutils import utils
from pathutils import manage_resolutions
from pathutils import url_regex_resolver
from pathutils.utils import pseudo_beaker

from collections import Counter, defaultdict
from textwrap import wrap
from urllib.parse import urlparse


# some useful internal constants
NUMBEROFLOOPS = "numberOfLoops"
REFERAL = "PageRefererUrl"
PAGEURL = "PageUrl"
RESOLVEDURL = "ResolvedUrl"
UNKNOWN = "Unknown"
CLEANPATH = "pg_clean_path"
RFNETLOC = "rf_netloc"
RFPATH = "rf_path"
RFRESOLVEDURL = "RefResolvedUrl"


def add_loop_count(events: pd.DataFrame, colName: str):
    """
    add_loop_count modifies a Dataframe in-place to add a loop count column. Currently the loop count
    alogrithm is extremely naive (it counts the repeating URLs), but we can improve later.

    :param events: pd.Dataframe with events data. It's a MultiIndex with sid as the key.
    :param colName: The name of the column containing the URLs. We made this a parameter so that we can define
    'loops' as we see fit (either original or cleaned up URLs can be used).
    :return:
    """
    unique_session_ids = utils.get_sessions(events)
    for idx, sid in enumerate(unique_session_ids):
        sess_df = events.loc[sid]
        events.loc[sid, NUMBEROFLOOPS] = number_of_loops(sess_df[colName].tolist())


def number_of_loops(sessList: list) -> int:
    """
    number_of_loops counts the number of loops in a list by naively looking at the most frequently repeating element

    :param sessList: list of URLs inside a given session.
    :return: number of Loops
    """
    c = Counter(sessList)
    return c.most_common(1)[0][1] - 1


def add_ref_resolved_url(events: pd.DataFrame):
    """
    add_ref_resolved_url adds a resolved reference URL to DataFrame in RFRESOLVEDURL column.

    :param events: events DataFrame
    :return:
    """
    events[RFRESOLVEDURL] = events.apply(construct_resolved_ref_url, axis=1)


def construct_resolved_ref_url(row: pd.Series):
    """
    construct_resolved_ref_url constructs a resolved reference URL based on RFNETLOC and RFPATH columns

    :param row: events DataFrame row
    :return:
    """
    netloc = row[RFNETLOC]
    rfPath = row[RFPATH]
    if len(netloc) == 0:
        return np.NaN
    schemeNetloc = "https://" + netloc
    fullpath = schemeNetloc + rfPath
    return fullpath


def add_resolved_url(events: pd.DataFrame):
    """
    add_resolved_url adds a resolved URL column to a DataFrame in RESOLVEDURL column. It requires the DataFrame to already have two
    columns, defined in PAGEURL and CLEANPATH variables.

    :param events: events DataFrame
    :return:
    """
    events[RESOLVEDURL] = events.apply(construct_resolved_url, axis=1)


def construct_resolved_url(row: pd.Series):
    """
    construct_resolved_url constructs a resolved URL based on two columns (PAGEURL and CLEANPATH) in pandas Series

    :param row: events DataFrame row
    :return: resolved URL
    """
    pageUrl = row[PAGEURL]
    cleanPath = row[CLEANPATH]
    parsedUrl = urlparse(pageUrl)
    schemeNetloc = parsedUrl.scheme + "://" + parsedUrl.netloc
    if type(cleanPath) == list:
        fullpath = [schemeNetloc] + cleanPath
    else:
        fullpath = [schemeNetloc]
    return "/".join(fullpath)


def build_session_index(events: pd.DataFrame, colName: str) -> dict:
    """
    build_session_index builds an inverted index of values in 'colName' to list of sessions.

    :param events: events DataFrame
    :param colName: column name to use for building index
    :return: Index of URLs to sets of SIDs
    """
    unique_session_ids = utils.get_sessions(events)
    sessIndex = defaultdict(set)
    for idx, sid in enumerate(unique_session_ids):
        sess_df = events.loc[sid]
        for url in set(sess_df[colName].tolist()):
            sessIndex[url].add(sid)
    return sessIndex


def get_funnel_in_outs(
    events: pd.DataFrame,
    sessionIndex: dict,
    funnel: list,
    colName: str,
    referalColName: str,
) -> (dict, dict):
    """
    get_funnel_in_outs returns 2 dictionaries (one for ingress, one for egress) with ingress and egress counts for a
    specified funnel

    :param events: events DataFrame
    :param sessionIndex: inverted index of URLs to SIDs
    :param funnel: funnel list
    :param colName: column name to use
    :param referalColName: referral column name
    :return: dictionaries of ingress and egress counts
    """
    sessFound = get_unordered_sessions_for_funnel(sessionIndex, funnel)
    sessOrdered = get_sessions_with_ordered(
        events, sessFound, funnel, colName, strict=True
    )
    egressCounts = defaultdict(int)
    ingressCounts = defaultdict(int)
    funnelMatches = utils.filter_events(events, session=sessOrdered)
    uniqSids = utils.get_sessions(funnelMatches)
    for idx, sid in enumerate(uniqSids):
        sess_df = funnelMatches.loc[sid]
        for index in get_sublist_indices(
            funnel, sess_df[colName].tolist(), strict=True
        ):
            if index == 0:
                ingress = sess_df.iloc[index].loc[referalColName]
                if not type(ingress) == str:
                    ingress = UNKNOWN
            else:
                ingress = sess_df.iloc[index - 1].loc[colName]
            if index + len(funnel) == len(sess_df[colName].tolist()):
                egress = UNKNOWN
            else:
                egress = sess_df.iloc[index + len(funnel)].loc[colName]
            ingressCounts[ingress] += 1
            egressCounts[egress] += 1
    return ingressCounts, egressCounts


def get_funnel_conversion_stats(
    events: pd.DataFrame, sessionIndex: dict, funnel: list, colName: str
) -> list:
    sessionCounts = []
    for i in range(len(funnel)):
        subfunnel = funnel[: i + 1]
        sessFound = get_unordered_sessions_for_funnel(sessionIndex, subfunnel)
        sessOrdered = get_sessions_with_ordered(
            events, sessFound, subfunnel, colName, strict=True
        )
        sessionCounts.append(len(sessOrdered))
    return zip(funnel, sessionCounts)


def get_session_link(sid: str, OrgId: str, is_staging: bool) -> str:
    if len(sid) != 32:
        raise ValueError("Expect sid to be 32 characters long")
    userId = sid[:16]
    sessionId = sid[16:]
    urlDict = pseudo_beaker(
        userId,
        sessionId,
        replay=False,
        scope=False,
        browser=None,
        OrgId=OrgId,
        is_staging=is_staging,
    )
    return urlDict["session_url"]


def get_sessions_for_funnel(
    events: pd.DataFrame,
    funnel: list,
    useResolvedUrls: bool,
    OrgId: str = None,
    is_staging: bool = False,
    strict: bool = True,
    numSessions: int = 0,
) -> list:
    """Get a list of sessions where each session contains the specified funnel

    :param events: events DataFrame
    :param funnel: funnel of interest
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :param OrgId: FullStory OrgId for the organization
    :param is_staging: set to True if FullStory staging environment should be used (for debugging purposes)
    :param strict: If `True`, the session has to follow the funnel steps in exact order (with no diversions between the steps). The `False` option is currently not supported.
    :param numSessions: number of sessions to return (if 0, return all available)
    :return: list of session URLs
    """
    if useResolvedUrls:
        columnToUse = RESOLVEDURL
    else:
        columnToUse = PAGEURL
    if useResolvedUrls:
        url_regex_resolver.resolve_urls(
            events, manage_resolutions.get_regex_dict(), PAGEURL, RESOLVEDURL
        )
    sids = build_and_get_sids_for_funnel(events, funnel, columnToUse, strict)
    if numSessions != 0:
        sids = sids[:numSessions]
    sessions = list(map(lambda p: get_session_link(p, OrgId, is_staging), sids))
    return sessions

def get_sessions_for_funnel_and_click(
    events: pd.DataFrame,
    funnel: list,
    clicktype: str,
    useResolvedUrls: bool,
    OrgId: str = None,
    is_staging: bool = False,
    strict: bool = True,
    numSessions: int = 0
) -> list:
    """Get a list of sessions for the specified funnel, where each session has to contain a click of the specified type

    :param events: full events DataFrame (that includes non-navigate events)
    :param funnel: funnel of interest
    :param clicktype: the type of click ("rage", "dead", or "error") that we want to filter for
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :param OrgId: FullStory OrgId for the organization
    :param is_staging: set to True if FullStory staging environment should be used (for debugging purposes)
    :param strict: If `True`, the session has to follow the funnel steps in exact order (with no diversions between the steps). The `False` option is currently not supported.
    :param numSessions: number of sessions to return (if 0, return all available)
    :return: list of session URLs
    """
    filtered = analyze_clicks.filter_dataset_by_clicktype(events, clicktype)
    filtered = analyze_clicks.remove_non_navigation(filtered)
    return get_sessions_for_funnel(filtered, funnel, useResolvedUrls, OrgId, is_staging, strict, numSessions)

def build_and_get_sids_for_funnel(
    events: pd.DataFrame, funnel: list, colName: str, strict: bool = True
) -> list:
    si = build_session_index(events, colName)
    return get_sids_for_funnel(events, si, funnel, colName, strict)


def get_sids_for_funnel(
    events: pd.DataFrame,
    sessionIndex: dict,
    funnel: list,
    colName: str,
    strict: bool = True,
) -> list:
    sessFound = get_unordered_sessions_for_funnel(sessionIndex, funnel)
    sessOrdered = get_sessions_with_ordered(events, sessFound, funnel, colName, strict)
    return sessOrdered


def get_counts_for_url(sessionIndex: dict) -> dict:
    counts = defaultdict(int)
    for key in sessionIndex:
        counts[key] = len(sessionIndex[key])
    return counts


def print_in_outs(ingressCounts: dict, egressCounts: dict):
    """
    print_in_outs sorts and prints the ingress and egress count dictionaries

    :param ingressCounts: ingress counts
    :param egressCounts: egress counts
    :return:
    """
    ingresses = sorted(ingressCounts.items(), key=lambda x: x[1], reverse=True)
    egresses = sorted(egressCounts.items(), key=lambda x: x[1], reverse=True)
    print("Ingresses:")
    for i in ingresses:
        print(i[0] + " : " + str(i[1]))
    print("")
    print("Egresses:")
    for e in egresses:
        print(e[0] + " : " + str(e[1]))


def plot_in_outs(ingressCounts: dict, egressCounts: dict):
    ingresses = sorted(ingressCounts.items(), key=lambda x: x[1], reverse=True)
    egresses = sorted(egressCounts.items(), key=lambda x: x[1], reverse=True)
    intags, incounts = zip(*ingresses)
    intags = ["\n".join(wrap(i, 60)) for i in intags]
    extags, excounts = zip(*egresses)
    extags = ["\n".join(wrap(i, 60)) for i in extags]

    fig1, ax1 = plt.subplots(figsize=(16, 9))
    ypos = np.arange(len(intags))
    ax1.barh(ypos, incounts)
    ax1.set_yticks(ypos)
    ax1.set_yticklabels(intags)
    ax1.invert_yaxis()
    ax1.set_title("Ingress")
    ax1.grid(linestyle=":")
    ax1.set_axisbelow(True)
    plt.tight_layout()

    fig2, ax2 = plt.subplots(figsize=(16, 9))
    ypos2 = np.arange(len(extags))
    ax2.barh(ypos2, excounts)
    ax2.set_yticks(ypos2)
    ax2.set_yticklabels(extags)
    ax2.invert_yaxis()
    ax2.set_title("Egress")
    ax2.grid(linestyle=":")
    ax2.set_axisbelow(True)
    plt.tight_layout()

    plt.show()


def plot_counts_by_freq(counts, topcounts: int, title: str, isSortedList: bool):
    """Plots a histogram of labels by their frequencies

    :param counts: either a dictionary with labels as keys and frequencies as values, or a sorted list of tuples
    :param topcounts: number of elements to include in the histogram
    :param title: histogram title
    :param isSortedList: If True, 'counts' is a sorted list. If False, 'counts' is a dictionary.
    :return:
    """
    if not isSortedList:
        counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    if topcounts > 0:
        counts = counts[:topcounts]
    countTags, countNums = zip(*counts)
    countTags = ["\n".join(wrap(i, 50)) for i in countTags]
    fig, ax = plt.subplots(figsize=(12,12))
    ypos = np.arange(len(countTags))
    ax.barh(ypos, countNums)
    ax.set_yticks(ypos)
    ax.set_yticklabels(countTags)
    ax.invert_yaxis()
    ax.set_title(title)
    ax.grid(linestyle=":")
    ax.set_axisbelow(True)
    plt.show()


def get_sessions_with_ordered(
    events: pd.DataFrame,
    sessUnordered: set,
    funnel: list,
    colName: str,
    strict: bool = True,
) -> list:
    """
    get_sessions_with_ordered returns a list of sessions which contain the specified funnel in the same order. One of its arguments
    is a set of sessions containing URLs in the funnel in any order. If strict is set to True, the order in the funnel is followed
    exactly (no additional URLs in between). (Alternative is currently not implemented)

    :param events: events DataFrame
    :param sessUnordered: set of sessions with URLs of interest in any order
    :param funnel: funnel list
    :param colName: column name to use
    :param strict: if True, enforce the funnel order strictly
    :return: list of sessions containing the funnel
    """
    filteredEvents = utils.filter_events(events, session=list(sessUnordered))
    uniqSids = utils.get_sessions(filteredEvents)
    sessOrdered = []
    for idx, sid in enumerate(uniqSids):
        sess_df = filteredEvents.loc[sid]
        if len(get_sublist_indices(funnel, sess_df[colName].tolist(), strict)) > 0:
            sessOrdered.append(sid)
    return sessOrdered


def get_sublist_indices(funnel: list, column: list, strict: bool) -> list:
    """
    get_sublist_indices returns a list of indices of the column list at which the funnel starts. The 'strict' argument means that
    funnel needs to match the sublist (or sublists) of column exactly. (Alternative is not currently implemented)

    :param funnel: funnel list
    :param column: column list
    :param strict: if True, enforce the funnel order strictly
    :return: list of starting indices for funnel sublist
    """
    funnelStartIndices = []
    if strict:
        funnelLen = len(funnel)
        starts = [i for i, x in enumerate(column) if x == funnel[0]]
        for start in starts:
            if funnel == column[start : start + funnelLen]:
                funnelStartIndices.append(start)
    return funnelStartIndices


def get_unordered_sessions_for_funnel(sessionIndex: dict, funnel: list) -> set:
    """
    get_unordered_sessions_for_funnel returns a set of sessions that contain all of URLs in the funnel, as
    determined by the passed-in inverted index

    :param sessionIndex: inverted index for URLs
    :param funnel: funnel list
    :return: set of sessions containing URLs in funnel in any order
    """
    sessSets = []
    for url in funnel:
        sessSets.append(sessionIndex[url])
    if len(sessSets) == 0:
        return None
    sessFound = sessSets[0].intersection(*sessSets[1:])
    return sessFound


def get_hauser_as_df(
    folder: str, navigate_only: bool = True, no_robots: bool = True
) -> pd.DataFrame:
    """Import JSON data from Hauser data export tool into a Pandas dataframe.

    :param folder: path to the Hauser data folder
    :param navigate_only: Only use "navigate" event types (default True)
    :param no_robots: Filter out devices identifying themselves as robots
       (default True)
    :return: dataframe of event data
    """
    if os.path.isdir(folder):
        df = None
        for f in os.listdir(folder):
            if f.endswith(".csv"):
                raise IOError(
                    "It looks like you're trying to analyze CSV records. This is not currently supported. Please export your data as JSON."
                )
            if f.endswith(".json"):
                f = os.path.join(folder, f)
                if df is None:
                    df = pd.read_json(f, orient="records")
                else:
                    dftemp = pd.read_json(f, orient="records")
                    df = df.append(dftemp, sort=False)
                print("Read file: " + f)
        if navigate_only:
            df = df.loc[df["EventType"] == "navigate"].copy()
        if no_robots:
            df = df.loc[df["PageDevice"] != "Robot"].copy()
        return df
    else:
        print("Warning: " + folder + " is not a directory")
        return None
