import os
from typing import Tuple
import sys
import pandas as pd


def count_trues(x):
    """
    For Service Now api table, counts the number of true values in a column
    """
    try:
        output = x.value_counts().loc[True]
    except: # noqa
        output = 0
    return output


def get_hash_value(series):
    """
    Use this for ServiceNow data cells with link/value dictionaries. Returns
    the the non-display value (i.e. the hash)
    """
    output = series.apply(lambda x: x["value"] if isinstance(x, dict) else "")
    return output


def get_display_value(series):
    """
    Use this for ServiceNow data cells with link/value dictionaries. Returns
    the display value
    """
    output = series.apply(lambda x: x["display_value"] if isinstance(x, dict) else "")
    return output


def unpack_link_fields(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find columns that have 'link' in their value, indicating that it is linked Might be more robust
    than hardcoding in json, as this will update if any new fields are linked

    NOTE: This assumes we are passing these to the API:

        sysparm_display_value=all
        sysparm_exclude_reference_link=False

    """
    temp = input_df.applymap(
        lambda x: "link" in x.keys() if isinstance(x, dict) else False
    )
    link_cols = temp.columns[temp.apply(count_trues) > 0].to_list()

    for col in input_df.columns:
        if col in link_cols:
            input_df["_" + col] = get_hash_value(input_df[col])
            input_df[col + "_display_value"] = get_display_value(input_df[col])
            input_df[col] = input_df["_" + col]
            input_df = input_df.drop(columns=["_" + col])
        else:
            input_df[col] = get_display_value(input_df[col])
    return input_df


def load_credentials(username: str = None, password: str = None,
                     credentials_from_env: bool = True) -> Tuple[str, str]:

    if credentials_from_env:
        if username or password:
            raise Exception("credentials_from_env is exclusive with username and password")
        (result_username, result_password) = (os.getenv("SNOW_USER"), os.getenv("SNOW_PASS"))
        if not result_username or not result_password:
            raise Exception("No credentials passed and SNOW_USER and SNOW_PASS not set.")
    else:
        if not username or not password:
            raise Exception("Username and password must be passed when not loading from env.")
        (result_username, result_password) = (username, password)

    assert result_username is not None
    assert result_password is not None
    return (result_username, result_password)


def query_yes_no(question, default="no"):
    # Taken from: https://stackoverflow.com/a/3041990
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")
