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
        # atrr_type: str,
        # attribute: str,
        # condition_type: str,
        # condition_value: Any,
        query_string: str,
    ) -> None:
        logging.info("Created codex query find")

        self.concepts = concepts
        self.query_string = query_string

    def __repr__(self):
        return f"{self.query_string}"
        # - Codex Query: action: {self.action} | concept: {self.concept} |  concept: {self.concept}

    # spitballin
    # action
    # concept
    # concept_type
    # compare_type
    # attribute
    # condtion_type
    # cond_value


class CodexQueryCompute(CodexQuery):

    action = "Compute"

    def __init__(
        self,
        queries: dict,
    ) -> None:
        logging.info("Created codex query compute")

        self.queries = queries

    def __repr__(self):
        return f"{self.queries}"


class CodexQueryCluster(CodexQuery):

    action = "Cluster"

    def __init__(
        self,
        query: dict,
    ) -> None:
        logging.info("Created codex query cluster")

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
        logging.info("Created codex query rule")

        self.rule = rule
        self.rule_string = rule_string
        self.rule_string_ans = rule_string_ans

    def __repr__(self):
        return f"{self.rule_string}"
