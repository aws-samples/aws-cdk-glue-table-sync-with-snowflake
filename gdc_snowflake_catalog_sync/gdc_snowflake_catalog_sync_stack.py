# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_events as _events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda  # aws_sqs as sqs,
from aws_cdk import aws_secretsmanager as secrets
from constructs import Construct
from gdc_snowflake_catalog_sync.snowflake_security_provider import SnowflakeSecurityProvider
from aws_cdk.aws_iam import Effect


class GdcSnowflakeCatalogSyncStack(Stack):
    """
    Glue - Snowflake Catalog Sync for Tables
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #
        # Target type for catalog sync
        #
        target_type = self.node.try_get_context("TARGET_TYPE")
        target_type_env = self.node.try_get_context("environment")
        target_type_details = self.node.try_get_context(target_type)[target_type_env]
        target_type_str = "_" + target_type

        #
        # Secrets for storing target connection configuration
        #
        secret = secrets.Secret(
            self,
            "GlueDataCatalogSyncTargetSecret" + target_type_str,
            generate_secret_string=GdcSnowflakeCatalogSyncStack.secret(target_type=target_type,
                                                                       target_type_details=target_type_details),
        )

        #
        # Lambda Layer with additional packages
        #
        lmbdalayer = _lambda.LayerVersion(
            self,
            "GlueDataCatalogSyncLayer" + target_type_str,
            removal_policy=RemovalPolicy.RETAIN,
            code=_lambda.Code.from_asset("lambdalibpackages"),
            compatible_architectures=[_lambda.Architecture.X86_64],
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
        )

        #
        # Lambda function with target and secret passed as environment parameters
        #
        lmbda = _lambda.Function(
            self,
            "GlueDataCatalogSyncHandler" + target_type_str,
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("gdc_snowflake_catalog_sync_lambda"),
            handler="gdc_snowflake_catalog_sync.handler",
            layers=[lmbdalayer],
            environment={
                "TARGET_TYPE": target_type,
                "SECRET_ARN": secret.secret_arn,
            },
            timeout=Duration.minutes(5),
        )

        #
        # Lambda role permission to access secrets and glue resources
        #
        secret.grant_read(lmbda.role)
        lmbda.add_to_role_policy(
            iam.PolicyStatement(effect=Effect.ALLOW,
                                actions=[
                                    "glue:getDatabase",
                                    "glue:getTable"
                                ],
                                resources=[
                                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                                    f"arn:aws:glue:{self.region}:{self.account}:database/*",
                                    f"arn:aws:glue:{self.region}:{self.account}:table/*/*",
                                ])
        )

        #
        # Event Bridge rules to trigger Lambda Sync function
        #
        grant = _events.Rule(
            self,
            "GlueDataCatalogSyncRule" + target_type_str,
            event_pattern={
                "detail": {
                    "eventSource": ["glue.amazonaws.com"],
                    "eventName": ["CreateTable", "UpdateTable"],
                }
            },
        )
        grant.add_target(events_targets.LambdaFunction(lmbda))

    @staticmethod
    def secret(target_type, target_type_details):
        """
        Generate the secret based on target type
        """
        secret_str = None
        if target_type == "SNOWFLAKE":
            secret_str = SnowflakeSecurityProvider.generate_secret_string(config=target_type_details)
        return secret_str
