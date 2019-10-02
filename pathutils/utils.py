"""utils.py

Various utilities

"""
import operator
import webbrowser

import pandas as pd


def sorted_dict_items(d, reverse=False):
    """Sorted (key, value) pairs by value.
    """
    result = sorted(d.items(), key=operator.itemgetter(1))
    if reverse:
        return result[::-1]
    else:
        return result

def get_beaker_lookup(UserId: str, SessionId: str, OrgId: str=None, is_staging: bool=False) -> dict:
    if is_staging:
        if OrgId is None:
            session_url_template = "https://app.staging.fullstory.com/ui/thefullstory.com/session/{UserId}:{SessionId}"
            scope_url_template = "https://app.staging.fullstory.com/admin/s/scope/scope.html?OrgId=thefullstory.com&UserId={UserId}&SessionId={SessionId}"
            return {"session_url": session_url_template.format(UserId=UserId, SessionId=SessionId),
                    "scope_url": scope_url_template.format(UserId=UserId, SessionId=SessionId)}
        else:
            session_url_template = "https://app.staging.fullstory.com/ui/{OrgId}/session/{UserId}:{SessionId}"
            scope_url_template = "https://app.staging.fullstory.com/admin/s/scope/scope.html?OrgId={OrgId}&UserId={UserId}&SessionId={SessionId}"
            return {"session_url": session_url_template.format(OrgId=OrgId, UserId=UserId, SessionId=SessionId),
                    "scope_url": scope_url_template.format(OrgId=OrgId, UserId=UserId, SessionId=SessionId)}
    else:
        if OrgId is None:
            session_url_template = "https://app.fullstory.com/ui/thefullstory.com/session/{UserId}:{SessionId}"
            scope_url_template = "https://app.fullstory.com/admin/s/scope/scope.html?OrgId=thefullstory.com&UserId={UserId}&SessionId={SessionId}"
            return {"session_url": session_url_template.format(UserId=UserId, SessionId=SessionId),
                    "scope_url": scope_url_template.format(UserId=UserId, SessionId=SessionId)}
        else:
            session_url_template = "https://app.fullstory.com/ui/{OrgId}/session/{UserId}:{SessionId}"
            scope_url_template = "https://app.fullstory.com/admin/s/scope/scope.html?OrgId={OrgId}&UserId={UserId}&SessionId={SessionId}"
            return {"session_url": session_url_template.format(OrgId=OrgId, UserId=UserId, SessionId=SessionId),
                    "scope_url": scope_url_template.format(OrgId=OrgId, UserId=UserId, SessionId=SessionId)}

def pseudo_beaker(UserId: str, SessionId: str, replay=True, scope=True, browser=None, OrgId: str=None, is_staging: bool=True) -> dict:
    """
    Mimic the Beaker admin tool in opening up one or both of session replay and
    Scope tools for a given User Id and Session Id.

    Option to specify a browser (e.g. "safari", "chrome") otherwise the system
    default is used.
    """
    url_dict = get_beaker_lookup(UserId, SessionId, OrgId, is_staging)
    if browser is None:
        w = webbrowser
    else:
        w = webbrowser.get(browser)
    if replay:
        w.open_new(url_dict["session_url"])
    if scope:
        w.open_new(url_dict["scope_url"])
    return url_dict


def get_sessions(events_df: pd.DataFrame) -> list:
    """
    Obtain a tuple of distinct session ids present in the input dataframe
    that has been multi-indexed by `preproc_events`.

    Input:
      events_df:  dataframe of events

    Output:
      list of distinct session ids present in the input
    """
    # get_level_values(0) only pulls the `sid` first part of the multi-index,
    # at position 0. The second index (at position 1) is the time-ordered
    # position
    return list(set(events_df.index.get_level_values(0)))


def preproc_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Input:
      events_df:  dataframe imported from BigQuery

    Output:
      Same dataframe with additional columns and datetime format for event time

    events_df is re-indexed according to
    unique sessions (sid, i), where `sid` = (UserId + SessionId) as a
    concatenated string, and `i`, the original integer index.

    The unique session index is also added as the `distinct_session_id`
    column in the dataframe. Get an iterable sequence of the session ids
    using `get_sessions`.

    Event start times are transformed to DateTime from string.
    """
    events_df["distinct_session_id"] = events_df["UserId"].astype(
        str
    ) + events_df["SessionId"].astype(str)

    # Time
    events_df["EventStart"] = pd.to_datetime(events_df["EventStart"])
    # events_df.sort_values("EventStart", inplace=True)

    events_df.set_index(
        pd.MultiIndex.from_arrays(
            (pd.Index(events_df["distinct_session_id"]), events_df.index),
            names=("sid", "i"),
        ),
        inplace=True,
    )

    # Note that `events_df.index.get_level_values(0)` will have repeats of the
    # unique session IDs over their corresponding rows, and so
    # it's of length `len(events_df)` not `len(unique_session_ids)`

    # sort event times per session
    events_df = (
        events_df.reset_index()
        .sort_values(["sid", "EventStart"], ascending=[1, 1])
        .set_index(["sid", "i"])
    )
    # create a proper incrementing integer index for each session, move unique
    # `i to a column
    events_df["idx"] = events_df.groupby("sid").cumcount()
    return events_df.reset_index().set_index(["sid", "idx"])


def filter_events(
    events_df: pd.DataFrame, org=None, session=None, start_time=None
) -> pd.DataFrame:
    """
    Inputs:
      events_df:  dataframe with multi-index
      org:         OrgId string or sequence of these
      session:     UserId+SessionId string or sequence of these
      start_time:  Singleton or sequence of pairs (t0, t1) bounding
                   first event time of a session, where times are given
                   in the format 'YYYY-MM-DD HH:MM:SS UTC'

    Output:
      New dataframe copy of input, filtered according to arguments.
    """
    # reduce dataset according to any org specification (per session)
    # (one or more orgs)
    if org is not None and org != "" and org != [""]:
        if isinstance(org, str):
            # singleton
            org = [org]
        events_df = events_df.loc[
            list(
                set(
                    events_df[events_df["OrgId"].isin(org)][
                        "distinct_session_id"
                    ]
                )
            )
        ]

    # reduce dataset according to any session specification (one or more)
    if session is not None and len(session) != 0:
        if isinstance(session, str):
            # singleton
            session = [session]
        events_df = events_df.loc[session]

    # reduce dataset according to any time specification (tuple of start, end
    # times)
    if start_time is not None:
        # assumes that only a pair is provided
        # transform to datetime format if not already
        t0, t1 = [pd.to_datetime(t) for t in start_time]
        # ensure only whole sessions are found that start in this range.
        # (assumes original data set does not truncate any sessions.)
        # get all session start times
        groups = events_df.groupby("distinct_session_id")["EventStart"].min()
        sids = groups[groups.between(t0, t1)].index
        events_df = events_df.loc[sids]
    return events_df