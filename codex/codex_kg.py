import logging
import json
from typing import Any, Dict, List

import pandas as pd
from grakn.client import GraknClient
import redis


from .grakn_functions import load_entity_into_grakn
from .grakn_functions import add_entities_into_grakn
from .grakn_functions import (
    load_relationship_into_grakn,
    add_relationship_data,
    get_all_entities,
    query_grakn,
)

from .codex_query import CodexQueryFind

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class CodexKg:
    def __init__(
        self,
        uri="localhost:48555",
        credentials=None,
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        redis_password=None,
    ):
        logging.info("created new CodexKg")
        self.uri = uri
        self.creds = credentials
        self.entity_map = {}
        self.rel_map = {}
        self.rkey = ""
        # TODO add rules map

        # connect to redis
        try:
            self.cache = redis.Redis(
                host=redis_host, port=redis_port, db=redis_db, password=redis_password
            )
        except Exception as error:
            logging.error("Couldnt connect to cache:" + str(error))
            raise

    # TODO can we get this data without having to rely on redis?
    def get_entites_grakn(self):
        logging.info("get all entites")

        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:
                    get_all_entities(session)

                    return 0
        except Exception as error:
            logging.error(error)
            return -1

    def create_db(self, db_name: str):
        logging.info("creating new keyspace " + db_name)
        logging.info("Connecting to grakn at " + self.uri)
        # start session create new db
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                client.session(keyspace=db_name)
                self.keyspace = db_name

                # check if keyspace exists in redis
                key_prefix = "grakn_keyspace_"
                rkey = key_prefix + db_name
                self.rkey = rkey

                if self.cache.exists(rkey):
                    # load data
                    logging.info("Loading data from redis")

                    curr_keyspace = json.loads(self.cache.get(self.rkey))
                    self.entity_map = curr_keyspace["entity_map"]
                    self.rel_map = curr_keyspace["rel_map"]

                else:
                    logging.info("Creating new keypsace in redis")
                    blank_keyspace = {}
                    blank_keyspace["entity_map"] = {}
                    blank_keyspace["rel_map"] = {}
                    # TODO add rules map
                    self.cache.set(rkey, json.dumps(blank_keyspace))

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

                # delete redis key as well
                self.cache.delete(self.rkey)
                self.entity_map = {}
                self.rel_map = {}

                return 0
        except Exception as error:
            logging.error(error)
            return -1

    def create_entity(self, df: pd.DataFrame, entity_name: str, entity_key=None):
        logging.info("Creating entity")

        try:
            # df = pd.read_csv(csv_path)
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:
                    self.entity_map[
                        entity_name
                    ] = {}  # TODO do we want to check if key exisits?
                    self.entity_map[entity_name]["key"] = entity_key
                    self.entity_map[entity_name]["cols"] = load_entity_into_grakn(
                        session, df, entity_name, entity_key
                    )

                    # {Productize} = {plays : produces, with_ent: Company}
                    # create rels array here rels [  {plays: produces, in_rel: Productize. with_ent: Company}
                    self.entity_map[entity_name]["rels"] = {}

                    logging.info(self.entity_map)
                    add_entities_into_grakn(session, df, entity_name, self.entity_map)

                    # add to redis
                    # get current key space
                    curr_keyspace = json.loads(self.cache.get(self.rkey))
                    # update entity map
                    curr_keyspace["entity_map"] = self.entity_map
                    # update redis
                    self.cache.set(self.rkey, json.dumps(curr_keyspace))

                    return 0
        except Exception as error:
            logging.error(error)
            return -1

    def create_relationship(
        self, df: pd.DataFrame, rel_name: str, rel1: str, rel2: str
    ):
        logging.info("Creating relationship")
        try:
            # df = pd.read_csv(csv_path)
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

                    # {Productize} = {plays : produces, with_ent: Company}
                    self.entity_map[rel1]["rels"][rel_name] = {}
                    self.entity_map[rel1]["rels"][rel_name]["plays"] = cols[0]
                    self.entity_map[rel1]["rels"][rel_name]["with_ent"] = rel2

                    # {Productize} = {plays : produces, with_ent: Company}
                    self.entity_map[rel2]["rels"][rel_name] = {}
                    self.entity_map[rel2]["rels"][rel_name]["plays"] = cols[1]
                    self.entity_map[rel2]["rels"][rel_name]["with_ent"] = rel1

                    logging.info(self.rel_map)
                    add_relationship_data(df, self.rel_map[rel_name], rel_name, session)

                    # get current key space
                    curr_keyspace = json.loads(self.cache.get(self.rkey))
                    # update both maps
                    curr_keyspace["rel_map"] = self.rel_map
                    curr_keyspace["entity_map"] = self.entity_map
                    # update redis
                    self.cache.set(self.rkey, json.dumps(curr_keyspace))

                    return 0
        except Exception as error:
            logging.error(error)
            return -1

    def query(self, query_object: CodexQueryFind):

        logging.info(f"{query_object}")
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:
                    return query_grakn(session, query_object)

        except Exception as error:
            logging.error(error)
            return -1

    # TODO
    # streamlit example
    # query/compute
    # create rules
    # show graph?
    # get entites/rels?
