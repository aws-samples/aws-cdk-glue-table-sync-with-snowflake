# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

""" AWS Glue Service resource definition """
import boto3
from botocore.config import Config

class Glue:
    def __init__(self):
        """
        Defines Glue Service resource
        """
        this_config = Config(
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            }
        )
        self.client = boto3.client("glue", config=this_config)

    def get_table_definitions(self, catalog: str, database: str, table: str):
        """
        Gets Glue Table definition
        """
        return self.client.get_table(
            CatalogId=catalog, DatabaseName=database, Name=table
        )
