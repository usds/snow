from unittest import TestCase
import pandas as pd
from servicenow_api_tools.utils import api_results_to_dataframe, dataframe_to_api_results

TestCase.maxDiff = None


def test_converters():
    # XXX: Even if the dataframe or the api_results starts with more native types (like bool or
    # numeric) it ignores them and converts everything to strings. I have found that this is what
    # the servicenow api returns even for types that have numeric values or values of true and
    # false, but I don't know if that's always true.
    dataframe = pd.DataFrame(
        columns=["foo", "bar"],
        data=[
            [{"subfoo": "1", "subfoo2": "2"}, "1"],
            [{"subfoo": "3", "subfoo2": "4"}, "2"],
        ])
    api_results = {
        "result": [
            {"foo": {"subfoo": "1", "subfoo2": "2"}, "bar": "1"},
            {"foo": {"subfoo": "3", "subfoo2": "4"}, "bar": "2"},
        ]}
    converted_dataframe = dataframe_to_api_results(dataframe)
    converted_api_results = api_results_to_dataframe(api_results)
    TestCase().assertDictEqual(api_results, converted_dataframe)
    pd.testing.assert_frame_equal(dataframe, converted_api_results)
