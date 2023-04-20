# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import json
import logging
import os
from datetime import timedelta
from typing import List

import boto3
import requests
from attrs import define
from jinja2 import Template
from jwt_generator import JWTGenerator
from table_definition import TableDefinition
from target_strategy import TargetStrategy

# Jinja Templates for Snowflake external table definition
column_template = "{{name}} {{type}} as (value:{{name}}::{{type}})"
partition_column_template = "{{column_name}} {{column_type}} as {{function}}"
create_template = (
    "CREATE OR REPLACE EXTERNAL TABLE "
    "{{ database_name }}.{{ table_name }}"
    "({{columns}}) "
    "{% if partitions|length > 0 %}"
    "PARTITION BY ({{ partitions }})"
    "{% endif %}"
    " LOCATION=@{{ table_path }} AUTO_REFRESH = {{ auto_refresh }} FILE_FORMAT = (TYPE = {{ file_format }});"
)


@define
class Snowflake(TargetStrategy):
    """
    Defines snowflake target
    """

    def __init__(
        self,
        url: str,
        accountidentifier: str,
        warehouse: str,
        role: str,
        username: str,
        password: str,
        stages: dict,
        allowed_values: dict
    ):
        """
        Defines snowflake instance
        """

        self.url = url
        self.warehouse = warehouse
        self.role = role
        self.username = username
        self.password = password
        self.accountidentifier = accountidentifier
        self.template: Template = Template(create_template)
        self.column_template: Template = Template(column_template)
        self.partition_template: Template = Template(partition_column_template)
        self.stages: dict = stages
        self.allowed_values: dict = allowed_values

    @classmethod
    def build(cls):
        """
        Build the snowflake configuration from secrets
        """

        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=os.getenv("SECRET_ARN"))
        snowflakesecrets = json.loads(response["SecretString"])
        url = snowflakesecrets["url"]
        warehouse = snowflakesecrets["warehouse"]
        role = snowflakesecrets["role"]
        username = snowflakesecrets["username"]
        password = snowflakesecrets["private_key"]
        accountidentifier = snowflakesecrets["accountidentifier"]
        stages = snowflakesecrets["stages"]["s3"]
        allowed_values = snowflakesecrets["allowedvalues"]
        print(f"Stages available in snowflake secrets are: {stages}")
        return Snowflake(
            url=url,
            accountidentifier=accountidentifier,
            warehouse=warehouse,
            role=role,
            username=username,
            password=password,
            stages=stages,
            allowed_values=allowed_values
        )

    @staticmethod
    def find_stage(location: str, stages: dict):
        """
        Parses the Snowflake integration stages and Glue table storage to build the table location
        """

        path = []
        splits = location.split("/")
        for i in reversed(range(len(splits))):
            for k, v in stages.items():
                if v.rstrip("/") == "/".join(splits[: i + 1]):
                    path.reverse()
                    return "/".join([k] + path)
            path.append(splits[i])
        return None

    def invoke_target(self, statement: str, statement_count: int):
        """
        Calls the snowflake SQL API for external table creation
        """

        logging.info(f"Invoking Target {self.url}")
        headers = {
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Authorization": "Bearer " + self.token,
        }
        payload = {
            "statement": statement,
            "parameters": {"MULTI_STATEMENT_COUNT": statement_count},
            "role": self.role,
        }
        resp = requests.post(
            url=self.url, data=json.dumps(payload), headers=headers
        )
        if resp.status_code == 200:
            print("Table Operation Successful")
        else:
            print("Table Operation Failed")
            print(f" Response={resp}")
            print(f" Response={resp.text}")

    def generate_token(self):
        """
        creates JWT token
        """

        self.token = JWTGenerator(
            self.accountidentifier,
            self.username,
            self.password,
            timedelta(minutes=60),
            timedelta(minutes=60),
        ).get_token()

    def auto_generate_token(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logging.info("Auto generating token")
                self.generate_token()
                return func(self, *args, **kwargs)

        return wrapper

    @auto_generate_token
    def synchronize(self, table_definitions: list[TableDefinition]) -> None:
        """
        Parses the Glue table definition and builds Snowflake external table definition
        Invokes Snowflake SQL API to create/update external table definition
        """

        statement = ""
        allowed_file_formats: List = self.allowed_values["fileformats"]
        for table_definition in table_definitions:
            if table_definition.file_format.upper() in allowed_file_formats :
                stage_name = Snowflake.find_stage(table_definition.location, self.stages)
                if stage_name is None:
                    print(f"Could not find stage for {table_definition.location}")
                else:
                    columns: List[str] = [
                        self.column_template.render({"name": column.name, "type": column.type})
                        for column in table_definition.columns
                    ]
                    partition_columns: List[str] = []
                    if len(table_definition.partitions) > 0:
                        path_token_len = len(stage_name.rstrip("/").split("/"))
                    for index, partition in enumerate(table_definition.partitions):
                        partindex = path_token_len + index
                        partition_function = f"DECODE(SPLIT_PART(SPLIT_PART(metadata$filename, '/', {partindex}),'=',2),'',SPLIT_PART(metadata$filename, '/', {partindex}),SPLIT_PART(SPLIT_PART(metadata$filename, '/', {partindex}),'=',2))"
                        partition_columns.append(
                            self.partition_template.render(
                                {
                                    "column_name": partition.name,
                                    "column_type": partition.type,
                                    "function": partition_function,
                                }
                            )
                        )
                    data = {
                        "database_name": table_definition.database.replace("__", "."),
                        "table_name": table_definition.name,
                        "columns": ",".join(columns + partition_columns),
                        "partitions": ",".join(
                            [partition.name for partition in table_definition.partitions]
                        ),
                        "table_path": stage_name,
                        "auto_refresh": "true",
                        "file_format": table_definition.file_format,
                    }
                    sql = self.template.render(data)
                    statement = sql + statement
        print(f"Snowflake Table definition: {statement}")
        if len(table_definitions) > 0 and len(statement) > 0:
            statement_count = len(table_definitions)
            self.invoke_target(statement,statement_count)
        else:
            print(f"Table Sync failed for statement: {statement}")

        pass
