import logging
import json
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient
import redis


logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexQueryFind:
    def __init__(
        self,
        action: str,
        concepts: dict,
        # atrr_type: str,
        # attribute: str,
        # condition_type: str,
        # condition_value: Any,
        query_string: str,
    ) -> None:
        logging.info("Created codex query find")

        self.action = action
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