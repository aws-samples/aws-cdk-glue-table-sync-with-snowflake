# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from typing import List
from attr import dataclass


@dataclass
class Column:
    name: str
    type: str
    comment: str = None


@dataclass
class TableDefinition:
    """
    Defines table definition
    """

    database: str
    name: str
    columns: List[Column]
    partitions: List[Column]
    location: str
    file_format: str

    @classmethod
    def from_get_table(cls, get_table_response: dict):
        """
        Build table definition
        """

        table_input = get_table_response["Table"]
        if "StorageDescriptor" in get_table_response["Table"].keys():
            columns = table_input["StorageDescriptor"]["Columns"]
            partitions = table_input["PartitionKeys"]
            return [
                TableDefinition(
                    database=table_input["DatabaseName"],
                    name=table_input["Name"],
                    columns=[
                        Column(name=column["Name"], type=column["Type"])
                        for column in columns
                    ],
                    partitions=[
                        Column(name=partition["Name"], type=partition["Type"])
                        for partition in partitions
                    ],
                    location=table_input["StorageDescriptor"]["Location"],
                    file_format=table_input["Parameters"]["classification"],
                )
            ]
        else:
            return None
