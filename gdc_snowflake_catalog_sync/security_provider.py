# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from abc import ABC, abstractmethod


class SecurityProvider(ABC):
    """
    Security Provider
    """

    @classmethod
    @abstractmethod
    def generate_secret_string(cls, config: dict):
        pass
