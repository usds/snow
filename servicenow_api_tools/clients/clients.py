from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from typing import List, Tuple, Dict
import logging
import json
import os
import pandas as pd
import pathlib
import traceback
from servicenow_api_tools.clients import utils
from servicenow_api_tools.clients.endpoint import ServicenowRestEndpoint
from servicenow_api_tools.clients.querybuilder import AggregateQueryBuilder
from servicenow_api_tools.clients.querybuilder import TableQueryBuilder
from servicenow_api_tools.clients.querybuilder import UpdateQueryBuilder
from servicenow_api_tools.utils import api_results_to_dataframe


class AggregateAPIClient:
    def __init__(self, endpoint: ServicenowRestEndpoint):
        self.logger = logging.getLogger(__name__)
        self.endpoint = endpoint

    def query(self, table: str, group_by: List[str] = None, query: str = None, having: str = None,
              display_value: str = None, return_count: bool = True):
        aggregate_query = AggregateQueryBuilder(
            table=table,
            group_by=group_by,
            query=query,
            having=having,
            display_value=display_value,
            return_count=return_count)
        self.logger.debug(f"Running query {aggregate_query}")

        results = self.endpoint.get(str(aggregate_query))

        if not results:
            msg = f"No result running query {aggregate_query}"
            self.logger.error(msg)
            raise Exception(msg)

        if "error" in results:
            msg = f"Failed running query {aggregate_query}: {results}"
            self.logger.error(msg)
            raise Exception(msg)

        return results


class TableAPIClient:
    def __init__(self, endpoint: ServicenowRestEndpoint):
        self.logger = logging.getLogger(__name__)
        self.aggregate_client = AggregateAPIClient(endpoint)
        self.endpoint = endpoint

    def _split_offsets(self, batch_size: int, max_rows: int) -> List[Tuple[int, int]]:
        batch_offsets = []
        for offset in range(0, max_rows, batch_size):
            batch_offsets.append((offset, min(offset + batch_size, max_rows) - offset))
        return batch_offsets

    def _pull_batch(
            self, offset, limit, table, query_params, fields, unpack_link_fields, display_value):
        self.logger.info(f"Pulling {table} rows {offset} to {offset+limit}")

        self.logger.debug(f"Query params: {query_params}")
        result = self._query(
            table=table,
            query=query_params,
            offset=offset,
            limit=limit,
            fields=fields,
            unpack_link_fields=unpack_link_fields,
            display_value=display_value)
        self.logger.debug(f"Completed pull for {table} rows {offset} to {offset+limit}")
        return result

    def _query(self, table: str, query: str = None, fields: List[str] = None, limit: int = None,
               offset: int = None, display_value: str = None,
               unpack_link_fields: bool = True) -> pd.DataFrame:
        table_query = TableQueryBuilder(
            table=table,
            query=query,
            fields=fields,
            limit=limit,
            offset=offset,
            display_value=display_value)
        self.logger.debug(f"Running query {table_query}")

        results = self.endpoint.get(str(table_query))

        if "error" in results:
            msg = f"Failed running query {table_query}: {results}"
            self.logger.error(msg)
            raise Exception(msg)

        response_dataframe = api_results_to_dataframe(results)

        if len(response_dataframe) > 0 and unpack_link_fields:
            # The "link fields" have the hash of the object and the display name in a sub object.
            # This unpacks them and sets them as two top level columns of their own.
            #
            # NOTE: This assumes we are passing these to the API:
            #
            #           sysparm_display_value=all
            #           sysparm_exclude_reference_link=False
            #
            response_dataframe = utils.unpack_link_fields(response_dataframe)
        return response_dataframe

    def query(self, table: str, query: str = None, fields: List[str] = None, limit: int = None,
              offset: int = None, display_value: str = None,
              batch_size: int = 2000, max_workers: int = 8,
              unpack_link_fields: bool = True) -> pd.DataFrame:
        """
        Queries the ServiceNow API endpoint with the given parameters and returns the results as a
        pandas DataFrame.
        """
        if unpack_link_fields:
            assert display_value is None, (
                "Passing display_value is currently not supported with unpack_link_fields. "
                "The unpack_link_fields option sets {field_value} under \"{field_name}\" "
                "and {field_display_value} under \"{field_name}_display_value\" in the dataframe.")
        self.logger.debug(f"Running query: {query}")
        number_of_records = int(self.aggregate_client.query(
            table=table,
            query=query)["result"]["stats"]["count"])
        if type(limit) == int:
            number_of_records = min(number_of_records, limit)
        batch_offsets = self._split_offsets(batch_size, number_of_records)

        self.logger.info(f"Pulling {number_of_records} records from {table} table from ServiceNow")

        start_time = datetime.utcnow().timestamp()
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                response_list = list(
                    pool.map((lambda args, f=partial(
                        self._pull_batch, table=table,
                        query_params=query,
                        fields=fields,
                        unpack_link_fields=unpack_link_fields,
                        display_value=display_value): f(*args)), batch_offsets))
        except Exception as e:
            self.logger.error(f"Failure pulling {table}: {e}")
            self.logger.error(traceback.format_exc())

        end_time = datetime.utcnow().timestamp()
        self.logger.debug(f"Execution time {end_time - start_time} seconds")

        nonzero_dataframes = []
        for response_dataframe in response_list:
            if len(response_dataframe) > 0:
                nonzero_dataframes.append(response_dataframe)
        if len(nonzero_dataframes) > 0:
            result = pd.concat(nonzero_dataframes)
        else:
            result = pd.DataFrame()
        assert ((result.shape[0] < number_of_records + 100)
                and (result.shape[0] > number_of_records - 100)), (
            "Expected count and recieved off by more than 100.  "
            "This could either be a bug or because a lot changed while the query was running.  "
            f"Actual: {result.shape[0]}, Initial Count: {number_of_records}")

        return result


class TableAPIUpdateClient:
    def __init__(self, endpoint: ServicenowRestEndpoint):
        self.logger = logging.getLogger(__name__)
        self.table_client = TableAPIClient(endpoint)
        self.endpoint = endpoint
        self.queued_updates: Dict[str, List[Dict]] = {}

    def _get_original_records_for_queued_updates(self):
        original_records = {}
        for table_name, updates in self.queued_updates.items():
            sys_ids = []
            for obj in updates:
                sys_ids.append(obj['sys_id'])
            result = self.table_client.query(
                table=table_name,
                query=f'sys_idIN{",".join(sys_ids)}')
            if result.shape[0] != len(sys_ids):
                self.logger.error("Could not find records for sys_ids:")
                for sys_id in sys_ids:
                    if sys_id not in result['sys_id'].values:
                        self.logger.error(f"\tsys_id: {sys_id}")
                self.logger.error("Aborting.")
                raise Exception("Could not find records for sys_ids.")
            assert result.shape[0] == len(sys_ids), f"{result.shape[0]} != {len(sys_ids)}"
            assert result.shape[0] == len(updates), f"{result.shape[0]} != {len(updates)}"
            original_records[table_name] = result
        return original_records

    def _backup_records(self, records, backup_directory):
        for table_name, table_records in records.items():
            backup_directory = os.path.join(
                backup_directory,
                f'{datetime.now().strftime("%Y-%m-%d_%H%M%S")}',
                f"{table_name}.xlsx")
            pathlib.Path(backup_directory).mkdir(parents=True, exist_ok=True)
            backup_path = os.path.join(backup_directory, f"{table_name}.xlsx")
            table_records.to_excel(backup_path)
            self.logger.info(
                f"Backed up {table_records.shape[0]} rows for {table_name} to {backup_path}.")

    def queue_updates_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        Queues an entire dataframe of data for update.
        """
        for obj in df.to_dict("records"):
            # Apparently the type annotations on this function don't say it returns a dict, but we
            # are assuming that's what we're getting back.
            assert type(obj) == dict, "Did not get a dict back from df.to_dict"
            self.queue_update(table_name, obj)

    def queue_update(self, table_name: str, obj: Dict):
        """
        Queues a single update object for update.

        sys_id must be set on the object.
        """
        self.logger.info(f'Queueing update for {obj["sys_id"]}')
        if not obj["sys_id"]:
            raise ValueError("sys_id is required to post an update")
        if table_name not in self.queued_updates:
            self.queued_updates[table_name] = []
        self.queued_updates[table_name].append(obj)

    def _exec_updates_nocheck(self, update_ops: List[Dict]):
        for update_op in update_ops:
            self.endpoint.put(update_op["resource"], update_op["obj"])

    def exec_updates(self, backup_directory, dry_run=True):
        original_records = self._get_original_records_for_queued_updates()
        self._backup_records(original_records, backup_directory)
        update_ops = []
        for table_name, updates in self.queued_updates.items():
            for obj in updates:
                update_query = UpdateQueryBuilder(
                    table=table_name,
                    sys_id=obj['sys_id'])
                update_ops.append({
                    "resource": str(update_query),
                    "obj": obj,
                })

        for update_op in update_ops:
            obj = update_op['obj']
            old_record = original_records[table_name].loc[
                original_records[table_name]['sys_id'] == obj['sys_id']]
            assert old_record.shape[0] == 1, (
                f"Expected one row for this sys_id, found {old_record.shape[1]}")
            print(f'{update_op["resource"]}')
            for key in obj.keys():
                if obj[key] != old_record.iloc[0][key]:
                    print(f'\t- {key}: "{old_record.iloc[0][key]}" -> "{obj[key]}"')
        if dry_run:
            print("dry_run=True in exec_updates function, will only print what would be updated")
        utils.query_yes_no("Does this update look correct?")
        if dry_run:
            print("dry_run=True in exec_updates function, not executing. Update payload:")
            print(json.dumps(update_ops, indent=4, sort_keys=True))
        else:
            return self._exec_updates_nocheck(update_ops)
