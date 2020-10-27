import logging
import json
import re
from typing import Any, Dict, List, Tuple
import pprint

import pandas as pd
from grakn.client import GraknClient
import redis

from .grakn_functions import load_entity_into_grakn
from .grakn_functions import add_entities_into_grakn
from .grakn_functions import (
    load_relationship_into_grakn,
    add_relationship_data,
    get_all_entities,
    get_all_rels,
    query_grakn,
    run_find_query,
    raw_query_read_grakn,
    raw_query_write_grakn,
)

from .codex_query import CodexQueryFind, CodexQuery, CodexQueryCompute, CodexQueryRule
from .codex_query_builder import (
    find_action,
    compute_action,
    codex_cluster_action,
    make_rule_cond,
    make_rule_string,
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
        """
        Purpose:
            Create new CodexKg
        Args:
            uri: grakn url
            credentials: grakn credentials
            redis_host: redis location
            redis_port: redis port
            redis_db: redis db
            redis_password: redis credentials
        Returns:
            N/A
        """
        logging.info("created new CodexKg")
        self.uri = uri
        self.creds = credentials
        self.entity_map = {}
        self.rel_map = {}
        self.rkey = ""
        self.rules_map = {}

        # new stuff who dis
        # self.lookup = {}
        # self.query_map = {}

        # connect to redis
        try:
            self.cache = redis.Redis(
                host=redis_host, port=redis_port, db=redis_db, password=redis_password
            )
        except Exception as error:
            logging.error("Couldnt connect to cache:" + str(error))
            raise

    def get_concepts_grakn(self):
        """
        Purpose:
            Get all concept in current keyspace
        Args:
            N/A
        Returns:
            entity_map: Entity concepts
            rel_map: Relation concepts
        """
        logging.info("get all concepts")

        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:
                    entity_map = get_all_entities(session)
                    rel_map = get_all_rels(session, entity_map)

                    return entity_map, rel_map
        except Exception as error:
            logging.error(error)
            return -1

    def get_keyspaces(self) -> list:
        """
        Purpose:
            Get all Grakn keyspaces
        Args:
            N/A
        Returns:
            List of keyspaces
        """
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                return client.keyspaces().retrieve()
        except Exception as error:
            logging.error(error)
            return []

    def create_db(self, db_name: str, check_grakn=False) -> int:
        """
        Purpose:
            Connect to Grakn keyspace
        Args:
            db_name: keyspace
        Returns:
            status: 0 if pass, -1 if fail
        """
        logging.info("creating new keyspace " + db_name)
        logging.info("Connecting to grakn at " + self.uri)
        # start session create new db
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                # logging.info(f"making key space...{db_name}")
                client.session(keyspace=str(db_name))
                self.keyspace = db_name

                # logging.info("key space made")

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
                    self.rules_map = curr_keyspace["rules_map"]
                    # self.query_map = {}
                    # self.lookup_map = {}

                    # logging.info(self.rkey)
                    # logging.info(self.rel_map)
                    # self.query_map = curr_keyspace["query_map"]
                    # self.lookup_map = curr_keyspace["lookup_map"]

                else:
                    logging.info("Creating new keypsace in redis")
                    blank_keyspace = {}

                    # check the db if there is data first

                    if check_grakn:
                        logging.info("Checking grakn for concepts")
                        ent_map, rel_map = self.get_concepts_grakn()
                    else:
                        ent_map = {}
                        rel_map = {}

                    blank_keyspace["entity_map"] = ent_map
                    blank_keyspace["rel_map"] = rel_map

                    blank_keyspace["rules_map"] = {}

                    # TODO someother time for nl queries
                    # blank_keyspace["lookup_map"] = {}
                    # blank_keyspace["lookup_map"]["Find"] = {}
                    # blank_keyspace["lookup_map"]["Compute"] = {}
                    # blank_keyspace["lookup_map"]["Cluster"] = {}
                    # blank_keyspace["lookup_map"]["Reason"] = {}
                    # blank_keyspace["lookup_map"]["Center"] = {}
                    # blank_keyspace["query_map"] = {}
                    self.cache.set(rkey, json.dumps(blank_keyspace))

                    self.entity_map = ent_map
                    self.rel_map = rel_map

                return 0
        except Exception as error:
            logging.error(error)
            return -1

    def delete_db(self, db_name: str) -> int:
        """
        Purpose:
            Delete Grakn keyspace
        Args:
            db_name: keyspace
        Returns:
            status: 0 if pass, -1 if fail
        """
        logging.info("Connecting to grakn at " + self.uri)
        logging.info("Deleteing keyspace " + db_name)

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

    def create_entity(
        self, df: pd.DataFrame, entity_name: str, entity_key=None
    ) -> Tuple[int, str]:
        """
        Purpose:
            Query Grakn
        Args:
            df: Relationship data
            entity_name: name for the entity
            entity_key: key for the entity
        Returns:
            status: 0 if pass, -1 if fail
        """
        logging.info(f"Creating entity {entity_name}")

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

                    # logging.info(self.entity_map)
                    add_entities_into_grakn(session, df, entity_name, self.entity_map)

                    # add to redis
                    # get current key space
                    curr_keyspace = json.loads(self.cache.get(self.rkey))
                    # update entity map
                    curr_keyspace["entity_map"] = self.entity_map
                    # update redis
                    self.cache.set(self.rkey, json.dumps(curr_keyspace))

                    return 0, "good"
        except Exception as error:
            logging.error(error)
            return -1, str(error)

    def create_relationship(
        self, df: pd.DataFrame, rel_name: str, rel1: str, rel2: str
    ) -> Tuple[int, str]:
        """
        Purpose:
            Query Grakn
        Args:
            df: Relationship data
            rel_name: relationship name
            rel1: first relationship
            rel2: second relationship
        Returns:
            status: 0 if pass, -1 if fail
        """
        logging.info(f"Creating relationship {rel_name}")
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
                    ent_key = self.entity_map[rel2]["key"]
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

                    # logging.info(self.rel_map)
                    add_relationship_data(df, self.rel_map[rel_name], rel_name, session)

                    # get current key space
                    curr_keyspace = json.loads(self.cache.get(self.rkey))
                    # update both maps
                    curr_keyspace["rel_map"] = self.rel_map
                    curr_keyspace["entity_map"] = self.entity_map
                    # update redis
                    self.cache.set(self.rkey, json.dumps(curr_keyspace))

                    return 0, "good"
        except Exception as error:
            logging.error(error)
            return -1, str(error)

    def raw_graql(self, graql_string: str, mode: str) -> dict:
        """
        Purpose:
            Run raw graql queries
        Args:
            query_object: the query object
        Returns:
            answers: answers to the query
        """
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:

                    if mode == "read":
                        return raw_query_read_grakn(session, graql_string)
                    else:
                        return raw_query_write_grakn(session, graql_string)

        except Exception as error:
            logging.error(error)
            return None

    def query(self, query_object: CodexQuery) -> dict:
        """
        Purpose:
            Query Grakn
        Args:
            query_object: the query object
        Returns:
            answers: answers to the query
        """
        logging.info(f"{query_object}")
        try:
            with GraknClient(uri=self.uri, credentials=self.creds) as client:
                with client.session(keyspace=self.keyspace) as session:

                    if query_object.action == "Rule":

                        rule_name = query_object.rule["name"]
                        self.rules_map[rule_name] = query_grakn(session, query_object)

                        # get current key space
                        curr_keyspace = json.loads(self.cache.get(self.rkey))
                        # update rules maps
                        curr_keyspace["rules_map"] = self.rules_map
                        # update redis
                        self.cache.set(self.rkey, json.dumps(curr_keyspace))

                        return self.rules_map[rule_name]

                    else:
                        return query_grakn(session, query_object)

        except Exception as error:
            logging.error(error)
            return None

    def search_rule(self, rule_name: str) -> dict:
        """
        Purpose:
            Search concepts by rule name
        Args:
            rule_name: The name of the rule
        Returns:
            rule_return_obj: rule object response
        """
        query = f"match $x isa {rule_name}; get;"

        rule_ans = self.rules_map[rule_name]["rule_string_ans"]

        rule_resp = []

        rule_return_obj = []

        answers = self.raw_graql(query, "read")
        logging.info(answers)
        answer_counter = 0
        answer = answers[0]
        answer_keys = list(answers[0].keys())

        for key_con in answer_keys:

            explanation = answer[key_con]["explanation"]
            exp_keys = list(explanation.keys())
            rule_new = rule_ans

            for exp_key in exp_keys:

                if exp_key in rule_ans:
                    concept = exp_key.split("_")[0]

                    if concept in list(self.entity_map.keys()):
                        concept_key = self.entity_map[concept]["key"]

                        try:
                            rule_new = rule_new.replace(
                                str(exp_key), str(explanation[exp_key][concept_key])
                            )
                        except:
                            rule_new = rule_new

            answer_counter += 1
            if rule_new in rule_resp:
                continue
            rule_resp.append(rule_new)

            rule_obj = {}

            rule_obj["concept1"] = answer[key_con]["concepts"][0]
            rule_obj["concept2"] = answer[key_con]["concepts"][1]
            rule_obj["explanation"] = rule_new

            rule_return_obj.append(rule_obj)

        return rule_return_obj

    def make_rule(self, rule_cond1: dict, rule_cond2: dict, rule_name: str) -> dict:
        """
        Purpose:
            Create a rule
        Args:
            rule_cond1: Condtion for first rule
            rule_cond2: Condtion for second rule
            rule_name: The name of the rule
        Returns:
            rule_resp: rule object
        """

        rule_obj = {}

        rule_obj["name"] = rule_name
        rule_obj["cond1"] = rule_cond1
        rule_obj["cond2"] = rule_cond2

        rule_string, rule_string_ans = make_rule_string(rule_obj)

        curr_query = CodexQueryRule(
            rule=rule_obj, rule_string=rule_string, rule_string_ans=rule_string_ans
        )

        rule_resp = self.query(curr_query)

        return rule_resp

    def rule_condition(
        self,
        concept: str,
        concept_attrs: list = [],
        concept_conds: list = [],
        concept_values: list = [],
        rel_actions: list = [],
        concept_rels: list = [],
        concept_rel_attrs: list = [],
        concept_rel_conds: list = [],
        concept_rel_values: list = [],
        with_rel_attrs: list = [],
        with_rel_conds: list = [],
        with_rel_values: list = [],
    ):
        """
        Purpose:
            Find data in Knowledge Graph
        Args:
            concept: Concept to find
            concept_attrs: Concept attributes
            concept_conds: condition for attribute
            concept_values: value for condition
            rel_actions: Find releation attrubite
            concept_rels= Relation to search for
            concept_rel_attrs=attributes for relation attribute
            concept_rel_conds=conditions for relation attribute
            concept_rel_values=values for relation attribute,
            with_rel_attrs=attributes of the relationship ,
            with_rel_conds=conditions of the relationship ,
            with_rel_values=values of the relationship ,
        Returns:
            answers: answers to the query
        """
        logging.info(f"Making {concept} rule condition")

        query_obj = make_rule_cond(
            self,
            concept,
            concept_attrs,
            concept_conds,
            concept_values,
            rel_actions,
            concept_rels,
            concept_rel_attrs,
            concept_rel_conds,
            concept_rel_values,
            with_rel_attrs,
            with_rel_conds,
            with_rel_values,
        )

        return query_obj

    def find(
        self,
        concept: str,
        concept_attrs: list = [],
        concept_conds: list = [],
        concept_values: list = [],
        rel_actions: list = [],
        concept_rels: list = [],
        concept_rel_attrs: list = [],
        concept_rel_conds: list = [],
        concept_rel_values: list = [],
        with_rel_attrs: list = [],
        with_rel_conds: list = [],
        with_rel_values: list = [],
    ):
        """
        Purpose:
            Find data in Knowledge Graph
        Args:
            concept: Concept to find
            concept_attrs: Concept attributes
            concept_conds: condition for attribute
            concept_values: value for condition
            rel_actions: Find releation attrubite
            concept_rels= Relation to search for
            concept_rel_attrs=attributes for relation attribute
            concept_rel_conds=conditions for relation attribute
            concept_rel_values=values for relation attribute,
            with_rel_attrs=attributes of the relationship ,
            with_rel_conds=conditions of the relationship ,
            with_rel_values=values of the relationship ,
        Returns:
            answers: answers to the query
        """

        logging.info(f"Finding {concept} data")

        query_obj = find_action(
            self,
            concept,
            concept_attrs,
            concept_conds,
            concept_values,
            rel_actions,
            concept_rels,
            concept_rel_attrs,
            concept_rel_conds,
            concept_rel_values,
            with_rel_attrs,
            with_rel_conds,
            with_rel_values,
        )

        # do query..
        return self.query(query_obj)

    def compute(self, actions: list, concepts: list, concept_attrs: list):
        """
        Purpose:
            Do a compute query
        Args:
            actions - compute actions
            concepts - list of concepts to compute on
            concept_attrs: concepts attributes to compute
        Returns:
            compute_obj: answer to query
        """
        logging.info(f"Computing data")

        query_obj = compute_action(self, actions, concepts, concept_attrs)

        # do query.
        return self.query(query_obj)

    def cluster(
        self,
        cluster_action,
        action: str,
        cluster_type: str = None,
        cluster_concepts: list = None,
        given_type: str = None,
        k_min: int = None,
    ):
        """
        Purpose:
            Do a cluster query
        Args:
            cluster_action - Type of clustering
            action - how to cluster
            cluster_type: "the cluster action"
            cluster_concepts: List of concepts to cluster
            given_type: concept to filter on
            k_min- how many K groups
        Returns:
            rule_resp: rule object
        """

        logging.info("Clustering data")

        query_obj = codex_cluster_action(
            self,
            cluster_action,
            action,
            cluster_type,
            cluster_concepts,
            given_type,
            k_min,
        )

        # do query.
        return self.query(query_obj)