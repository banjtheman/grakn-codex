import logging
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient


from .grakn_functions import load_entity_into_grakn

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexKg:
    def __init__(self, uri="localhost:48555"):
        logging.info("created new CodexKg")
        self.uri = uri

    def create_db(self, db_name):
        logging.info("creating new keyspace " + db_name)
        logging.info("Connecting to grakn at " + self.uri)
        # start session create new db
        with GraknClient(uri=self.uri) as client:
            client.session(keyspace=db_name)
            self.keyspace = db_name

    def delete_db(self, db_name):
        logging.info("Deleteing keyspace " + db_name)
        logging.info("Connecting to grakn at " + self.uri)
        # start session create new db
        with GraknClient(uri=self.uri) as client:
            client.keyspaces().delete(self.keyspace)

    def create_entity(self, csv_path, entity_name):
        logging.info("Creating entity from " + csv_path)

        df = pd.read_csv(csv_path)
        with GraknClient(uri=self.uri) as client:
            with client.session(keyspace=self.keyspace) as session:
                load_entity_into_grakn(session,df,entity_name)
