# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from enum import Enum


class TargetType(Enum):
    """
    Defines Various target types
    """
    SNOWFLAKE = "SNOWFLAKE"
    LOGGING = "LOGGING"
