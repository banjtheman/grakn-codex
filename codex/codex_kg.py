import logging
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient


from .grakn_functions import load_entity_into_grakn
from .grakn_functions import add_entities_into_grakn

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexKg:
    def __init__(self, uri="localhost:48555", credentials=None):
        logging.info("created new CodexKg")
        self.uri = uri
        self.creds = credentials
        self.entity_map = {}

    def create_db(self, db_name: str):
        logging.info("creating new keyspace " + db_name)
        logging.info("Connecting to grakn at " + self.uri)
        # start session create new db
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                client.session(keyspace=db_name)
                self.keyspace = db_name
                return 0
        except Exception as error:
            logging.error(error)
            return -1

    def delete_db(self, db_name: str):
        logging.info("Deleteing keyspace " + db_name)
        logging.info("Connecting to grakn at " + self.uri)
        # start session create new db
        try:
            with GraknClient(uri=self.uri) as client:
                client.keyspaces().delete(self.keyspace)
                return 0
        except Exception as error:
            logging.error(error)
            return -1

    def create_entity(self, csv_path: str, entity_name: str, entity_key=None):
        logging.info("Creating entity from " + csv_path)

        try:
            df = pd.read_csv(csv_path)
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:
                    self.entity_map[
                        entity_name
                    ] = {}  # TODO do we want to check if key exisits?
                    self.entity_map[entity_name]["key"] = entity_key
                    self.entity_map[entity_name]["cols"] = load_entity_into_grakn(
                        session, df, entity_name, entity_key
                    )
                    logging.info(self.entity_map)
                    add_entities_into_grakn(session, df, entity_name, self.entity_map)
                    return 0
        except Exception as error:
            logging.error(error)
            return -1

    # TODO
    # get entites/rels
    # create rels
    # create rules
    # add ents/rels
    # query/compute
