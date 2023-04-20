# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import logging
import os
from typing import List

from context import Context
from enums import TargetType
from glue import Glue
from logging_strategy import GCDLogging
from snowflake_strategy import Snowflake
from table_definition import TableDefinition
from botocore.exceptions import ClientError

# Get environment variable
target_type: TargetType = TargetType(os.environ.get("TARGET_TYPE"))
if target_type is TargetType.SNOWFLAKE:
    target = Snowflake.build()
elif target_type is TargetType.LOGGING:
    target = GCDLogging.build()

# Get the service resource
glue = Glue()


# Helper class to extract table info from the event and get Glue table details
def get_table_detail(event: dict) -> List[TableDefinition]:
    table_name = event["requestParameters"]["tableInput"]["name"]
    database_name = event["requestParameters"]["databaseName"]
    if "catalogId" in event['requestParameters'].keys():
        catalog_id = event["requestParameters"]["catalogId"]
    else:
        catalog_id = event["userIdentity"]["accountId"]
    try:
        get_table_response = glue.get_table_definitions(
            catalog=catalog_id, database=database_name, table=table_name
        )
    except ClientError as err:
        print(f"Get Table Exception.....{err}")

    return TableDefinition.from_get_table(get_table_response)


def handler(event, context):
    print(f"Incoming event: {event}")
    # Sync with target system
    print(f"Syncing table definition with {target_type}")
    event_detail = event["detail"]
    glue_table_definitions = get_table_detail(event_detail)
    if glue_table_definitions is not None:
        Context(strategy=target).synchronize(
            table_definitions=glue_table_definitions
        )
        print(f"Glue Table Sync Attempted with Snowflake: {event}")
    else:
        print(f"Glue Table Extract Failed: {event}")

    return {
        'statusCode': 200
    }
