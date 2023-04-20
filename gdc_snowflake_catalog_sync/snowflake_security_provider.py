# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import json
from abc import abstractmethod
from aws_cdk import aws_secretsmanager as secrets
from gdc_snowflake_catalog_sync.security_provider import SecurityProvider


class SnowflakeSecurityProvider(SecurityProvider):
    """
    Snowflake target security provider
    """

    @classmethod
    @abstractmethod
    def generate_secret_string(cls, config: dict):
        """
        Creates the secret for Snowflake
        """

        return secrets.SecretStringGenerator(
            generate_string_key="password",
            secret_string_template=json.dumps(
                config
            )
        )
