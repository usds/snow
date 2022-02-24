import logging
from typing import Dict, List, Any, Union, Mapping
import pandas as pd
import os


DEBUG_LOG = os.getenv("SNOW_CLIENT_DEBUG_LOG")


def dataframe_to_api_results(
        df: pd.DataFrame) -> Dict[str, Union[Mapping[Union[int, str], Any], List[Any]]]:
    return {"result": df.to_dict('records')}


def api_results_to_dataframe(result: Dict[str, List[Dict]]) -> pd.DataFrame:
    return pd.DataFrame(result["result"], dtype="object")


def get_module_logger(module_name: str) -> logging.Logger:
    """Instantiates a logger object on stdout
    Args:
        module_name (str): Name of the module outputting the logs
    Returns:
        logger (Logging.logger): A logger object
    """
    logger = logging.getLogger(module_name)
    formatter = logging.Formatter(
        "%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s"
    )

    file_handler = logging.FileHandler(f"{module_name}.out")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler())

    logger.setLevel(logging.WARN)
    if DEBUG_LOG:
        logger.setLevel(logging.DEBUG)
    return logger
