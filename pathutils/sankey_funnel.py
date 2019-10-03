#!/usr/bin/env python3

"""sankey_funnel.py

Plot a sankey diagram for a funnel

"""

import argparse
import json
import plotly.graph_objects as gobj
import pandas as pd

from collections import defaultdict
from urllib.parse import urlparse
from urllib.parse import urlunparse

from pathutils import analyze_traffic, utils

from pathutils.funnel_stats import get_funnel_stats
from pathutils.funnel_in_outs import get_in_outs
from pathutils.utils import sorted_dict_items

OTHER = "Other"

def show_sankey(folder, funnelFile, useResolvedUrls, limitBranches, title):
    with open(funnelFile, "r") as fread:
        tFile = json.load(fread)
    funnel = tFile["funnel"]
    events = analyze_traffic.get_hauser_as_df(folder)
    events = utils.preproc_events(events)
    plot_funnel(title, events, funnel, useResolvedUrls, limitBranches)

def get_funnel_lists(title: str, events: pd.DataFrame, funnel: list, useResolvedUrls: bool, cutoff: int=10):
    labels = []
    colors = []
    sources = []
    targets = []
    values = []

    pink = "#F8598B"
    blue = "#438EE1"
    purple = "#685EC3"

    swatches = [pink, blue, purple]

    labelsToNodes = {}
    outputsToNodes = {}

    for i, fun in enumerate(funnel):
        labelsToNodes[fun] = i
        labels.append(fun)
        colors.append(swatches[0])

    nodecount = len(funnel)

    funnel_counts = get_funnel_stats(events, funnel, useResolvedUrls, 0)
    totalIn = funnel_counts[0][1]

    for i in range(len(funnel_counts) - 1):
        sources.append(labelsToNodes[funnel_counts[i][0]])
        targets.append(labelsToNodes[funnel_counts[i + 1][0]])
        values.append(funnel_counts[i + 1][1])

    # Add funnel sources
    subfunnel = funnel[:1]
    ingress, egress = get_in_outs(events, subfunnel, useResolvedUrls, 0)
    sortIn = sorted_dict_items(ingress, True)
    sortIn = sortIn[:cutoff]
    for input in sortIn:
        labels.append(input[0])
        colors.append(swatches[1])
        sources.append(nodecount)
        nodecount += 1
        targets.append(labelsToNodes[funnel[0]])
        values.append(input[1])

    listedIns = sum([i[1] for i in sortIn])
    otherIns = totalIn - listedIns
    labels.append(OTHER)
    colors.append(swatches[1])
    sources.append(nodecount)
    nodecount += 1
    targets.append(labelsToNodes[funnel[0]])
    values.append(otherIns)

    # Add funnel sinks
    for j in range(1, len(funnel) + 1):
        subfunnel = funnel[:j]
        ingress, egress = get_in_outs(events, subfunnel, useResolvedUrls, 0)
        sortOut = sorted_dict_items(egress, True)
        sortOutCut = sortOut[:cutoff]
        nextStepInFun = False
        for output in sortOutCut:
            if j < len(funnel) and output[0] == funnel[j]:
                nextStepInFun = True
                continue
            sources.append(labelsToNodes[subfunnel[-1]])
            if output[0] not in outputsToNodes.keys():
                labels.append(output[0])
                colors.append(swatches[2])
                targets.append(nodecount)
                outputsToNodes[output[0]] = nodecount
                nodecount += 1
            else:
                targets.append(outputsToNodes[output[0]])
            values.append(output[1])
        totalOut = sum([i[1] for i in sortOut])
        totalCut = sum([i[1] for i in sortOutCut])
        if totalOut != totalCut:
            if nextStepInFun:
                otherOut = totalOut - totalCut
            else:
                if len(subfunnel) < len(funnel):
                    otherOut = totalOut - totalCut - funnel_counts[len(subfunnel)][1]
                else:
                    otherOut = totalOut - totalCut
            sources.append(labelsToNodes[subfunnel[-1]])
            if OTHER not in outputsToNodes.keys():
                labels.append(OTHER)
                colors.append(swatches[2])
                targets.append(nodecount)
                outputsToNodes[OTHER] = nodecount
                nodecount += 1
            else:
                targets.append(outputsToNodes[OTHER])
            values.append(otherOut)

    labels, title2 = resolve_labels(funnel, labels)
    title = title + "   (" + title2 + ")"

    return labels, colors, sources, targets, values, title


def plot_funnel(title: str, events: pd.DataFrame, funnel: list, useResolvedUrls: bool, cutoff: int=10):
    """Plot sankey diagram for a funnel

    :param title: title for the sankey diagram
    :param events: events dataframe
    :param funnel: funnel to be plotted
    :param useResolvedUrls: indicates whether original or resolved URLs should be used
    :param cutoff: number of inflow/outflow nodes to plot for each sankey node (all remaining nodes get grouped into Other)
    :return:
    """
    labels, colors, sources, targets, values, title = get_funnel_lists(title, events, funnel, useResolvedUrls, cutoff)

    fig = gobj.Figure(data=[gobj.Sankey(
        arrangement="freeform",
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
            color=colors
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
        ))])

    fig.update_layout(title_text=title, font_size=12)
    fig.show()

def resolve_labels(funnel: list, labels: list):
    RESOLVER = "~"
    domain = get_common_domain(funnel)
    for i, label in enumerate(labels):
        if label.startswith(domain):
            labels[i] = label.replace(domain, RESOLVER, 1)
    title2 = RESOLVER + "  --  " + domain
    return labels, title2


def get_common_domain(funnel):
    if len(funnel) == 0:
        return None
    domains = defaultdict(int)
    for url in funnel:
        truncated = truncate_url(url)
        domains[truncated] += 1
    domains = sorted(domains.items(), key=lambda x: x[1], reverse=True)
    return domains[0][0]

def truncate_url(url):
    parts = urlparse(url)
    truncated = urlunparse((parts.scheme, parts.netloc, '', '', '', ''))
    return truncated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot a sankey diagram for a funnel")
    parser.add_argument("hauser_folder", type=str, help="Path to folder containg data exported from hauser (as json)")
    parser.add_argument("funnel", type=str, help="Path to json file containing the funnel")
    parser.add_argument("limitBranches", type=int, help="Limit the number of branches for each sankey node")
    parser.add_argument("title", type=str, help="Plot title")
    parser.add_argument("--useResolvedUrls", dest="useResolvedUrls", action="store_const", const=True, help="Use resolved page URLs")
    args = parser.parse_args()
    show_sankey(args.hauser_folder, args.funnel, args.useResolvedUrls, args.limitBranches, args.title)