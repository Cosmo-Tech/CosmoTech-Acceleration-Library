# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import csv
import logging

from redis.commands.graph.query_result import QueryResult

from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil

logger = logging.getLogger(__name__)


class CsvWriter:
    """
    Csv Writer class
    """

    @staticmethod
    def write_twin_data(export_dir: str, file_name: str, query_result: QueryResult, headers: list = [],
                        delimiter: str = ',', quote_char: str = '\"') -> None:
        output_file_name = export_dir + file_name + '.csv'
        logger.debug(f"Writing CSV file {output_file_name}")
        csvfile = open(output_file_name, 'w')
        writer = csv.writer(csvfile, delimiter=delimiter, quotechar=quote_char, quoting=csv.QUOTE_ALL)
        if headers:
            writer.writerow(headers)
        for raw_data in query_result.result_set:
            for i in range(len(raw_data)):
                row = []
                for key, val in raw_data[i].properties.items():
                    if isinstance(val, bool):
                        row.append(str(val).lower())
                    elif str(val) == 'True' or str(val) == 'False':
                        row.append(str(val).lower())
                    else:
                        row.append(str(val))
                writer.writerow(row)
        csvfile.close()
        logger.debug(f"... CSV file {output_file_name} has been written")

    @staticmethod
    def write_relationship_data(export_dir: str, file_name: str, query_result: QueryResult, headers: list = [],
                                delimiter: str = ',', quote_char: str = '\"') -> None:
        output_file_name = export_dir + file_name + '.csv'
        logger.debug(f"Writing CSV file {output_file_name}")
        csvfile = open(output_file_name, 'w')
        writer = csv.writer(csvfile, delimiter=delimiter, quotechar=quote_char, quoting=csv.QUOTE_ALL)
        if headers:
            writer.writerow(headers)
        for raw_data in query_result.result_set:
            row = [raw_data[0], raw_data[1]]
            for key, val in raw_data[2].properties.items():
                property_name = str(key)
                if property_name != ModelUtil.src_key and property_name != ModelUtil.dest_key:
                    if isinstance(val, bool):
                        row.append(str(val).lower())
                    elif str(val) == 'True' or str(val) == 'False':
                        row.append(str(val).lower())
                    else:
                        row.append(str(val))
            writer.writerow(row)
        csvfile.close()
        logger.debug(f"... CSV file {output_file_name} has been written")
