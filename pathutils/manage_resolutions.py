#!/usr/bin/env python3

"""manage_resolutions.py

Tool to manage regex URL rules. Possible commands are 'add', 'show', or 'delete'

"""

import argparse
import os
import pickle

PATHRULES = "pathrules.p"

def add_rule(regex: str, val: str):
    """Add a URL resolution rule

    :param regex: Regular expression for the rule
    :param val: Replacement value
    :return:
    """
    if os.path.exists(PATHRULES):
        rules = pickle.load(open(PATHRULES, "rb"))
    else:
        rules = {}
    rules[regex] = val
    pickle.dump(rules, open(PATHRULES, "wb"))


def show_rules():
    """Print all URL resolution rules

    :return:
    """
    if os.path.exists(PATHRULES):
        rules = pickle.load(open(PATHRULES, "rb"))
        for rule in rules:
            print(rule + " : " + rules[rule])
    else:
        print("Rules file doesn't exist")

def delete_rule(regex: str):
    """Delete a URL resolution rule

    :param regex: Regular expression for the rule
    :return:
    """
    if os.path.exists(PATHRULES):
        rules = pickle.load(open(PATHRULES, "rb"))
        del rules[regex]
        pickle.dump(rules, open(PATHRULES, "wb"))
    else:
        print("Rules file doesn't exist")

def get_regex_dict():
    if os.path.exists(PATHRULES):
        rules = pickle.load(open(PATHRULES, "rb"))
        return rules
    else:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tool to manage regex URL rules. Possible commands are 'add', 'show', or 'delete'")
    parser.add_argument("command", type=str, nargs="+")
    args = parser.parse_args()
    if args.command[0] == "add":
        add_rule(args.command[1], args.command[2])
    elif args.command[0] == "show":
        show_rules()
    elif args.command[0] == "delete":
        delete_rule(args.command[1])
    else:
        print("Error: not a correct command")
