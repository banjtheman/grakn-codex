import logging
import json
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient
import redis

from abc import ABC, abstractmethod


logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexQuery(ABC):

    action = "CodexQuery"

    def __init__(self):
        """
        Purpose:
            Init CodexQuery class
        Args:
            N/A
        Returns:
            N/A
        """
        pass


class CodexQueryFind(CodexQuery):

    action = "Find"

    def __init__(
        self,
        concepts: dict,
        query_string: str,
    ) -> None:

        self.concepts = concepts
        self.query_string = query_string

    def __repr__(self):
        return f"{self.query_string}"


class CodexQueryCompute(CodexQuery):

    action = "Compute"

    def __init__(self, queries: dict, query_text_list: list) -> None:

        self.queries = queries
        self.query_text_list = query_text_list

    def __repr__(self):
        return f"{self.query_text_list}"


class CodexQueryCluster(CodexQuery):

    action = "Cluster"

    def __init__(
        self,
        query: dict,
    ) -> None:

        self.query = query

    def __repr__(self):
        return f"{self.query}"


class CodexQueryRule(CodexQuery):

    action = "Rule"

    def __init__(
        self,
        rule: dict,
        rule_string: str,
        rule_string_ans: str,
    ) -> None:

        self.rule = rule
        self.rule_string = rule_string
        self.rule_string_ans = rule_string_ans

    def __repr__(self):
        return f"{self.rule_string}"
