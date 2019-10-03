#!/usr/bin/env python3

"""frequent_funnel.py

Find most common funnels of specified length through specified URL

"""
import argparse

from collections import defaultdict
from pandas import DataFrame

from pathutils import analyze_traffic, utils, url_regex_resolver, manage_resolutions


def get_top_funnels(funurl, funlen, useResolvedUrls, folder, limit_rows, numResults):
    df = analyze_traffic.get_hauser_as_df(folder)
    df = utils.preproc_events(df)
    funnelCounts = get_top_funnels_df(funurl, funlen, useResolvedUrls, df, limit_rows)
    print_top_funnel_counts(funnelCounts, numResults)


def get_top_funnels_df(funurl: str, funlen: int, useResolvedUrls: bool, events: DataFrame, limit_rows: int = 0) -> dict:
    """Get top funnels of specified length which contain the specified URL

    :param funurl: URL that should be contained in the funnel
    :param funlen: funnel length
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :param events: events DataFrame
    :param limit_rows: number of rows of events DataFrame to use (use all rows if 0)
    :return: dictionary of funnels and their frequencies
    """
    if useResolvedUrls:
        columnToUse = analyze_traffic.RESOLVEDURL
    else:
        columnToUse = analyze_traffic.PAGEURL
    if limit_rows != 0:
        events = events.head(limit_rows)
    if useResolvedUrls:
        url_regex_resolver.resolve_urls(events, manage_resolutions.get_regex_dict(), analyze_traffic.PAGEURL, analyze_traffic.RESOLVEDURL)
    si = analyze_traffic.build_session_index(events, columnToUse)
    funnelCounts = get_funnel_lists(events, si, funurl, funlen, columnToUse)
    return funnelCounts


def get_funnel_lists(events, sessIndex, funurl, funlen, columnToUse):
    sessions = sessIndex[funurl]
    filteredEvents = utils.filter_events(events, session=list(sessions))
    uniqSids = utils.get_sessions(filteredEvents)
    funnelCounts = defaultdict(int)
    for idx, sid in enumerate(uniqSids):
        sess_df = filteredEvents.loc[sid]
        sess_funnels = get_funnels_for_session(sess_df[columnToUse].tolist(), funurl, funlen)
        for fun in sess_funnels:
            funnelCounts[fun] += 1
    return funnelCounts


def print_top_funnel_counts(funnelCounts: dict, numToShow: int):
    """Prints specified number of funnels and their frequencies

    :param funnelCounts: a dictionary of funnels and their frequencies
    :param numToShow: number of funnels to show
    :return:
    """
    counts = sorted(funnelCounts.items(), key=lambda x: x[1], reverse=True)
    counts = counts[:numToShow]
    for c in counts:
        print("\n".join(c[0]))
        print("Count: " + str(c[1]))


def get_funnels_for_session(pathlist, url, funlen):
    funnelSet = set()
    if len(pathlist) < funlen:
        return funnelSet
    indices = [i for i,x in enumerate(pathlist) if x == url]
    for ix in indices:
        if ix - funlen + 1 >= 0:
            startSubfunnel = ix - funlen + 1
        else:
            startSubfunnel = 0
        if ix + funlen >= len(pathlist):
            endSubfunnel = len(pathlist) - 1
        else:
            endSubfunnel = ix + funlen - 1
        start = startSubfunnel
        while(True):
            funnelSet.add(tuple(pathlist[start:start + funlen]))
            if start + funlen - 1 == endSubfunnel:
                break
            start += 1
    return funnelSet


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find most common funnels of specified length through specified URL")
    parser.add_argument("hauser_folder", type=str, help="Path to folder containg data exported from hauser (as json)")
    parser.add_argument("url", type=str, help="URL that a funnel should go through")
    parser.add_argument("funnelLength", type=int, help="Length of the funnels to consider")
    parser.add_argument("numResults", type=int, help="Number of results to show")
    parser.add_argument("--useResolvedUrls", dest="useResolvedUrls", action="store_const", const=True,
                        help="Use resolved page URLs")
    parser.add_argument("--limit_rows", type=int, default=0,
                        help="Limit the number of rows in the dataset")
    args = parser.parse_args()
    get_top_funnels(args.url, args.funnelLength, args.useResolvedUrls, args.hauser_folder, args.limit_rows, args.numResults)
