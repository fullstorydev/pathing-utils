#!/usr/bin/env python3
"""
get_popular_urls.py

Find and display most frequently visited URLs

"""

import argparse
import pandas as pd

from pathutils import analyze_traffic, utils, url_regex_resolver, manage_resolutions

def print_popular(folder, useResolvedUrls, limit_rows, topCounts):
    df = analyze_traffic.get_hauser_as_df(folder)
    df = utils.preproc_traffic(df)
    urlCounts = get_popular(df, useResolvedUrls, limit_rows)
    print_top_url_counts(urlCounts, topCounts)


def get_popular(traffic: pd.DataFrame, useResolvedUrls: bool, limit_rows: int = 0) -> dict:
    """Returns a dictionary of visited URLs and visit counts for each URL

    :param traffic: traffic DataFrame
    :param useResolvedUrls: boolean indicating whether original or resolved URLs should be used
    :param limit_rows: number of rows from the original DataFrame to use (if 0, then use entire DataFrame)
    :return:
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
    urlCounts = analyze_traffic.get_counts_for_url(si)
    return urlCounts


def print_top_url_counts(counts: dict, topCounts: int):
    counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    if topCounts > 0:
        counts = counts[:topCounts]
    print("Most frequent URLs: ")
    for c in counts:
        print(c[0] + " : " + str(c[1]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prints most frequently visited URLs")
    parser.add_argument("hauser_folder", type=str, help="Path to folder containg data exported from hauser (as json)")
    parser.add_argument("--useResolvedUrls", dest="useResolvedUrls", action="store_const", const=True, help="Use resolved page URLs")
    parser.add_argument("--limit_rows", type=int, default=0, help="Limit the number of rows in the dataset")
    parser.add_argument("--limitTopCounts", type=int, default=0, help="Limit the number of top URLs printed")
    args = parser.parse_args()
    print_popular(args.hauser_folder, args.useResolvedUrls, args.limit_rows, args.limitTopCounts)