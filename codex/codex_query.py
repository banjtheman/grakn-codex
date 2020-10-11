import logging
import json
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient
import redis





logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexQuery:

    def __init__(self,):
        logging.info("Created codex query")

    #spitballin
    # action
    # concept
    # concept_type
    # compare_type
    # attribute
    # condtion_type
    # cond_value