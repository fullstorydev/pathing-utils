#!/usr/bin/env python3

"""funnel_stats.py

Print conversion statistics for a funnel

"""

import argparse
import json

from pandas import DataFrame

from pathutils import analyze_traffic, utils, url_regex_resolver, manage_resolutions

def print_in_outs(folder, funnelFile, useResolvedUrls, limit_rows):
    with open(funnelFile, "r") as fread:
        tFile = json.load(fread)
    funnel = tFile["funnel"]
    events = analyze_traffic.get_hauser_as_df(folder)
    events = utils.preproc_events(events)
    funnelCounts = get_funnel_stats(events, funnel, useResolvedUrls, limit_rows)
    print_funnelcounts(funnelCounts)


def get_funnel_stats(events: DataFrame, funnel: list, useResolvedUrls: bool, limit_rows: int = 0) -> list:
    """Get conversion statistics for a funnel

    :param events: events DataFrame
    :param funnel: funnel of interest
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :param limit_rows: number of rows of events DataFrame to use (use all rows if 0)
    :return: sorted list of funnel conversions by step
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
    funnelCounts = analyze_traffic.get_funnel_conversion_stats(events, si, funnel, columnToUse)
    funnelCounts = list(funnelCounts)
    return funnelCounts


def print_funnelcounts(funnelCounts):
    for funstep in funnelCounts:
        perc = float(funstep[1])/funnelCounts[0][1]*100
        perc = f'{perc:.2f}'
        print(funstep[0] + " : " + perc + "%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print conversion statistics for a funnel")
    parser.add_argument("hauser_folder", type=str, help="Path to folder containg data exported from hauser (as json)")
    parser.add_argument("funnel", type=str, help="Path to json file containing the funnel")
    parser.add_argument("--useResolvedUrls", dest="useResolvedUrls", action="store_const", const=True, help="Use resolved page URLs")
    parser.add_argument("--limit_rows", type=int, default=0, help="Limit the number of rows in the dataset")
    args = parser.parse_args()
    print_in_outs(args.hauser_folder, args.funnel, args.useResolvedUrls, args.limit_rows)