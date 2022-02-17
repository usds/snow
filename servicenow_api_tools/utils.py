from typing import Dict, List, Any, Union, Mapping
import pandas as pd


def dataframe_to_api_results(
        df: pd.DataFrame) -> Dict[str, Union[Mapping[Union[int, str], Any], List[Any]]]:
    return {"result": df.to_dict('records')}


def api_results_to_dataframe(result: Dict[str, List[Dict]]) -> pd.DataFrame:
    return pd.DataFrame(result["result"], dtype="object")
