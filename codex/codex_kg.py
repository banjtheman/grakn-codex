import logging
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient


from .grakn_functions import load_entity_into_grakn
from .grakn_functions import add_entities_into_grakn
from .grakn_functions import load_relationship_into_grakn, add_relationship_data


logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexKg:
    def __init__(self, uri="localhost:48555", credentials=None):
        logging.info("created new CodexKg")
        self.uri = uri
        self.creds = credentials
        self.entity_map = {}
        self.rel_map = {}

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

    def create_relationship(self, csv_path: str, rel_name: str, rel1: str, rel2: str):
        logging.info("Creating relationship from " + csv_path)
        try:
            df = pd.read_csv(csv_path)
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:
                    self.rel_map[
                        rel_name
                    ] = {}  # TODO do we want to check if key exisits?

                    cols = df.columns
                    attrs = cols[2:]

                    self.rel_map[rel_name]["rel1"] = {}
                    self.rel_map[rel_name]["rel1"]["role"] = cols[0]
                    self.rel_map[rel_name]["rel1"]["entity"] = rel1
                    ent_key = self.entity_map[rel1]["key"]
                    self.rel_map[rel_name]["rel1"]["key"] = ent_key

                    if self.rel_map[rel_name]["rel1"]["key"] is not None:
                        self.rel_map[rel_name]["rel1"]["key_type"] = self.entity_map[
                            rel1
                        ]["cols"][ent_key]["type"]
                    else:
                        self.rel_map[rel_name]["rel1"]["key_type"] = None

                    self.rel_map[rel_name]["rel2"] = {}
                    self.rel_map[rel_name]["rel2"]["role"] = cols[1]
                    self.rel_map[rel_name]["rel2"]["entity"] = rel2
                    ent_key = self.entity_map[rel1]["key"]
                    self.rel_map[rel_name]["rel2"]["key"] = ent_key

                    if self.rel_map[rel_name]["rel2"]["key"] is not None:
                        self.rel_map[rel_name]["rel2"]["key_type"] = self.entity_map[
                            rel2
                        ]["cols"][ent_key]["type"]
                    else:
                        self.rel_map[rel_name]["rel2"]["key_type"] = None

                    self.rel_map[rel_name]["cols"] = load_relationship_into_grakn(
                        session, df, attrs, rel_name, self.rel_map[rel_name]
                    )

                    logging.info(self.rel_map)
                    add_relationship_data(df, self.rel_map[rel_name], rel_name, session)
                    return 0
        except Exception as error:
            logging.error(error)
            return -1

    # TODO
    # load a keyspace
    # get entites/rels
    # create rules
    # query/compute
