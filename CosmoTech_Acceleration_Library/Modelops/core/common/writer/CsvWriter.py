# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import csv
import logging
import json
import ast
import os

from redis.commands.graph.query_result import QueryResult

from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil

logger = logging.getLogger(__name__)


class CsvWriter:
    """
    Csv Writer class
    """

    @staticmethod
    def _to_csv_format(val: any) -> str:
        if isinstance(val, bool):
            return str(val).lower()
        if isinstance(val, dict):
            return json.dumps(val)
        if str(val) == 'True' or str(val) == 'False':
            return str(val).lower()
        if str(val).startswith('{') and str(val).endswith('}'):
            try:
                return json.dumps(json.loads(val))
            except json.decoder.JSONDecodeError:
                return json.dumps(ast.literal_eval(str(val)))
        return str(val)

    @staticmethod
    def _to_cosmo_key(val: any) -> str:
        if str(val) == ModelUtil.dt_id_key:
            return ModelUtil.id_key
        return val

    @staticmethod
    def write_twin_data(export_dir: str,
                        file_name: str,
                        query_result: QueryResult,
                        delimiter: str = ',',
                        quote_char: str = '\"') -> None:
        headers = set()
        rows = []
        for raw_data in query_result.result_set:
            row = {}
            # read all graph link properties
            for i in range(len(raw_data)):  # TODO for the moment its only a len 1 list with the node
                row.update({
                    CsvWriter._to_cosmo_key(k): CsvWriter._to_csv_format(v)
                    for k, v in raw_data[i].properties.items()
                })
            headers.update(row.keys())
            rows.append(row)

        output_file_name = f'{export_dir}/{file_name}.csv'
        logger.debug(f"Writing CSV file {output_file_name}")
        with open(output_file_name, 'w') as csvfile:
            csv_writer = csv.DictWriter(csvfile,
                                        fieldnames=headers,
                                        delimiter=delimiter,
                                        quotechar=quote_char,
                                        quoting=csv.QUOTE_MINIMAL)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        logger.debug(f"... CSV file {output_file_name} has been written")

    @staticmethod
    def write_relationship_data(export_dir: str,
                                file_name: str,
                                query_result: QueryResult,
                                headers: list = [],
                                delimiter: str = ',',
                                quote_char: str = '\"') -> None:
        headers = {'source', 'target'}
        rows = []
        for raw_data in query_result.result_set:
            row = {'source': raw_data[0], 'target': raw_data[1]}
            row.update({k: CsvWriter._to_csv_format(v) for k, v in raw_data[2].properties.items()})
            headers.update(row.keys())
            rows.append(row)

        output_file_name = f'{export_dir}/{file_name}.csv'
        logger.debug(f"Writing CSV file {output_file_name}")
        with open(output_file_name, 'w') as csvfile:
            csv_writer = csv.DictWriter(csvfile,
                                        fieldnames=headers,
                                        delimiter=delimiter,
                                        quotechar=quote_char,
                                        quoting=csv.QUOTE_MINIMAL)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        logger.debug(f"... CSV file {output_file_name} has been written")

    @staticmethod
    def write_data(export_dir: str,
                   file_name: str,
                   input_rows: dict,
                   delimiter: str = ',',
                   quote_char: str = '\"') -> None:
        output_file_name = export_dir + file_name + '.csv'
        write_header = False
        if not os.path.exists(output_file_name):
            write_header = True

        headers = set()
        output_rows = []
        for row in input_rows:
            output_rows.append({CsvWriter._to_cosmo_key(k): CsvWriter._to_csv_format(v) for k, v in row.items()})
            headers.update(row.keys())

        logger.info(f"Writing file {output_file_name} ...")
        with open(output_file_name, 'a') as csvfile:
            csv_writer = csv.DictWriter(csvfile,
                                        fieldnames=headers,
                                        delimiter=delimiter,
                                        quotechar=quote_char,
                                        quoting=csv.QUOTE_MINIMAL)
            if write_header:
                csv_writer.writeheader()
            csv_writer.writerows(output_rows)
        logger.debug(f"... file {output_file_name} has been written")
