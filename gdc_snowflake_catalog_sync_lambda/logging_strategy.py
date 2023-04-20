# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from typing import List
import logging
from table_definition import TableDefinition
from target_strategy import TargetStrategy


class GCDLogging(TargetStrategy):
    @classmethod
    def build(cls):
        logging.info("Logging :: build")
        return GCDLogging()

    def synchronize(self, table_definitions: List[TableDefinition]) -> None:
        logging.info("Logging :: synchronize")
        logging.info(f"Table Definition={table_definitions}")
