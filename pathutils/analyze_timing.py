"""analyze_timing.py

A collection of utilities for extracting and viewing timing data (such as the amount of time users spend on each page)
from session data.

"""
import numpy as np
import plotly.graph_objects as gobj

from pandas import DataFrame

from pathutils import analyze_clicks, analyze_traffic, manage_resolutions, url_regex_resolver

EVENTSTART = "EventStart"

def get_timing_for_funnel(eventsfull: DataFrame, funnel: list, useResolvedUrls: bool) -> list:
    """Get a list of funnel step times (amounts of time users spend before navigating to next step) for a funnel

    :param eventsfull: full events DataFrame (that includes non-navigate events)
    :param funnel: funnel of interest
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :return: list of funnel step times for each step
    """
    funneltimes = []
    for i in range(len(funnel)):
        funneltimes.append([])
    events = analyze_clicks.remove_non_navigation(eventsfull)
    if useResolvedUrls:
        columnToUse = analyze_traffic.RESOLVEDURL
    else:
        columnToUse = analyze_traffic.PAGEURL
    if useResolvedUrls:
        url_regex_resolver.resolve_urls(events, manage_resolutions.get_regex_dict(), analyze_traffic.PAGEURL, analyze_traffic.RESOLVEDURL)
    si = analyze_traffic.build_session_index(events, columnToUse)
    sessFound = analyze_traffic.get_unordered_sessions_for_funnel(si, funnel)
    sessOrdered = analyze_traffic.get_sessions_with_ordered(events, sessFound, funnel, columnToUse, strict=True)
    for sid in sessOrdered:
        sess_df = events.loc[sid]
        indices = analyze_traffic.get_sublist_indices(funnel, sess_df[columnToUse].tolist(), True)
        for index in indices:
            timestamps = []
            timespent = []
            for i in range(len(funnel)):
                timestamps.append(sess_df.iloc[index + i].loc[EVENTSTART])
                if i > 0:
                    delta = timestamps[i] - timestamps[i - 1]
                    funneltimes[i-1].append(delta.total_seconds())
    return funneltimes


def print_timing_averages(funnel: list, funneltimes: list):
    """Prints average and median timing values for each step of the funnel

    :param funnel: funnel of interest
    :param funneltimes: list of funnel timing values (produced by get_timing_for_funnel function)
    :return:
    """
    for i in range(len(funnel) - 1):
        print(funnel[i] + " --> " + funnel[i + 1])
        av = np.average(np.asarray(funneltimes[i], dtype=np.float32))
        med = np.median(np.asarray(funneltimes[i], dtype=np.float32))
        print("Average: " + str(av) + " seconds")
        print("Median: " + str(med) + " seconds")


def plot_timing_data(funnel: list, funneltimes: list, step: int = -1):
    """Plot a histogram of funnel step time values for one or more steps of the funnel

    :param funnel: funnel of interest
    :param funneltimes: list of funnel timing values (produced by get_timing_for_funnel function)
    :param step: Step of the funnel to plot the histogram for. If a negative number, overlaid histograms are plotted for all steps
    :return:
    """
    if step < 0:
        fig = gobj.Figure()
        for i in range(len(funnel) - 1):
            fig.add_trace(gobj.Histogram(x=np.asarray(funneltimes[i], dtype=np.float32),
                                         name=funnel[i]))
        fig.update_layout(barmode='overlay')
        fig.update_traces(opacity=0.5)
        fig.show()
    else:
        if step >= len(funnel) - 1:
            raise ValueError(
                "Step " + str(step) + " is not valid for this funnel"
            )
        fig = gobj.Figure(data=[gobj.Histogram(x=np.asarray(funneltimes[step], dtype=np.float32))])
        fig.update_layout(title=gobj.layout.Title(text=funnel[step]))
        fig.show()
