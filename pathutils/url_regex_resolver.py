"""url_regex_resolver.py

Creates a column in the dataframe with URLs which have been resolved according to the existing rules dictionary

"""
import pandas as pd
import re


def resolve_urls(traffic: pd.DataFrame, toReplace: dict, fromCol: str, toCol: str):
    traffic.loc[:,toCol] = traffic.apply(create_resolved_url, axis=1, args=(toReplace, fromCol))


def create_resolved_url(row: pd.Series, toReplace: dict, fromCol: str):
    origUrl = row[fromCol]
    for rex in toReplace:
        origUrl = re.sub(rex, toReplace[rex], origUrl)
    return origUrl
