# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from abc import ABC, abstractmethod
from typing import List

from table_definition import TableDefinition


class TargetStrategy(ABC):
    @classmethod
    @abstractmethod
    def build(cls):
        pass

    @abstractmethod
    def synchronize(self, table_definitions: List[TableDefinition]) -> None:
        pass


