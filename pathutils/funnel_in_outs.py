#!/usr/bin/env python3

"""funnel_in_outs.py

Print inflow and outflow statistics for a funnel

"""

import argparse
import json

from pandas import DataFrame

from pathutils import analyze_traffic, utils, url_regex_resolver, manage_resolutions

def print_in_outs(folder, funnelFile, useResolvedUrls, limit_rows, doPlot):
    with open(funnelFile, "r") as fread:
        tFile = json.load(fread)
    funnel = tFile["funnel"]
    traffic = analyze_traffic.get_hauser_as_df(folder)
    traffic = utils.preproc_traffic(traffic)
    ingressCounts, egressCounts = get_in_outs(traffic, funnel, useResolvedUrls, limit_rows)
    if not doPlot:
        analyze_traffic.print_in_outs(ingressCounts, egressCounts)
    else:
        analyze_traffic.plot_in_outs(ingressCounts, egressCounts)


def get_in_outs(traffic: DataFrame, funnel: list, useResolvedUrls: bool, limit_rows: int = 0) -> (dict, dict):
    """Get information about inflows and outflows for a funnel

    :param traffic: traffic DataFrame
    :param funnel: funnel of interest
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :param limit_rows: number of rows of traffic DataFrame to use (use all rows if 0)
    :return: a pair of dictionaries, with inflow and outflow URL frequency counts
    """
    if useResolvedUrls:
        columnToUse = analyze_traffic.RESOLVEDURL
    else:
        columnToUse = analyze_traffic.PAGEURL
    if limit_rows != 0:
        traffic = traffic.head(limit_rows)
    if useResolvedUrls:
        url_regex_resolver.resolve_urls(traffic, manage_resolutions.get_regex_dict(), analyze_traffic.PAGEURL, analyze_traffic.RESOLVEDURL)
    si = analyze_traffic.build_session_index(traffic, columnToUse)
    ingressCounts, egressCounts = analyze_traffic.get_funnel_in_outs(traffic, si, funnel, columnToUse, analyze_traffic.REFERAL)
    return ingressCounts, egressCounts

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print inflow and outflow statistics for a funnel")
    parser.add_argument("hauser_folder", type=str, help="Path to folder containg data exported from hauser (as json)")
    parser.add_argument("funnel", type=str, help="Path to json file containing the funnel")
    parser.add_argument("--useResolvedUrls", dest="useResolvedUrls", action="store_const", const=True, help="Use resolved page URLs")
    parser.add_argument("--limit_rows", type=int, default=0, help="Limit the number of rows in the dataset")
    parser.add_argument("--plotInOuts", dest="plotInOuts", action="store_const", const=True, help="Plot inflows and outflows (instead of printing the values)")
    args = parser.parse_args()
    print_in_outs(args.hauser_folder, args.funnel, args.useResolvedUrls, args.limit_rows, args.plotInOuts)