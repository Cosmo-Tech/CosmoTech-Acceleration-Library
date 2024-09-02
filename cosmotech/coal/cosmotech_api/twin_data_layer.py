# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib
from csv import DictReader

from cosmotech.coal.utils.logger import LOGGER

ID_COLUMN = "id"

SOURCE_COLUMN = "src"

TARGET_COLUMN = "dest"


class CSVSourceFile:

    def __init__(self, file_path: pathlib.Path):
        self.file_path = file_path
        if not file_path.name.endswith(".csv"):
            raise ValueError(f"'{file_path}' is not a csv file")
        with open(file_path) as _file:
            dr = DictReader(_file)
            self.fields = list(dr.fieldnames)
        self.object_type = file_path.name[:-4]

        self.id_column = None
        self.source_column = None
        self.target_column = None

        for _c in self.fields:
            if _c.lower() == ID_COLUMN:
                self.id_column = _c
            if _c.lower() == SOURCE_COLUMN:
                self.source_column = _c
            if _c.lower() == TARGET_COLUMN:
                self.target_column = _c

        has_id = self.id_column is not None
        has_source = self.source_column is not None
        has_target = self.target_column is not None

        is_relation = all([has_source, has_target])

        if not has_id and not is_relation:
            LOGGER.error(f"'{file_path}' does not contains valid nodes or relationships")
            LOGGER.error(f"  - Valid nodes contains at least the property [bold yellow]{ID_COLUMN}[/]")
            LOGGER.error("  - Valid relationships contains at least the properties " +
                         f"[bold yellow]{ID_COLUMN}, {SOURCE_COLUMN}, {TARGET_COLUMN}[/]")
            raise ValueError(f"'{file_path}' does not contains valid nodes or relations")

        self.is_node = has_id and not is_relation

        self.content_fields = {_f: _f for _f in self.fields if
                               _f not in [self.id_column, self.source_column, self.target_column]}
        if has_id:
            self.content_fields[ID_COLUMN] = self.id_column
        if is_relation:
            self.content_fields[SOURCE_COLUMN] = self.source_column
            self.content_fields[TARGET_COLUMN] = self.target_column

    def reload(self, inplace: bool = False) -> 'CSVSourceFile':
        if inplace:
            self.__init__(self.file_path)
            return self
        return CSVSourceFile(self.file_path)

    def generate_query_insert(self) -> str:
        """
        Read a CSV file headers and generate a CREATE cypher query
        :return: the Cypher query for CREATE
        """

        field_names = sorted(self.content_fields.keys(), key=len, reverse=True)

        if self.is_node:
            query = ("CREATE (:" + self.object_type + " {" + ", ".join(
                f"{property_name}: ${self.content_fields[property_name]}" for property_name in field_names) + "})")
            # query = ("UNWIND $params AS params " +
            #          f"MERGE (n:{self.object_type}) " +
            #          "SET n += params")
        else:
            query = ("MATCH " +
                     "(source {" + ID_COLUMN + ":$" + self.source_column + "}),\n" +
                     "(target {" + ID_COLUMN + ":$" + self.target_column + "})\n" +
                     "CREATE (source)-[rel:" + self.object_type +
                     " {" + ", ".join(
                        f"{property_name}: ${self.content_fields[property_name]}" for property_name in
                        field_names) + "}" +
                     "]->(target)\n")
            # query = ("UNWIND $params AS params " +
            #          "MATCH (source {" + ID_COLUMN + ":params." + self.source_column + "})\n" +
            #          "MATCH (target {" + ID_COLUMN + ":params." + self.target_column + "})\n" +
            #          f"CREATE (from) - [rel:{self.object_type}]->(to)" +
            #          "SET rel += params")
        return query
