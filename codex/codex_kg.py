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
    # get_all_entities,
    query_grakn,
    run_find_query,
    raw_query_read_grakn,
    raw_query_write_grakn,
)

from .codex_query import CodexQueryFind, CodexQuery, CodexQueryCompute, CodexQueryRule

from .codex_query_builder import find_action

from difflib import SequenceMatcher


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


rules = (
    (lambda word: re.search("[sxz]$", word), lambda word: re.sub("$", "es", word)),
    (
        lambda word: re.search("[^aeioudgkprt]h$", word),
        lambda word: re.sub("$", "es", word),
    ),
    (
        lambda word: re.search("[^aeiou]y$", word),
        lambda word: re.sub("y$", "ies", word),
    ),
    (lambda word: re.search("$", word), lambda word: re.sub("$", "s", word)),
)


def plural(noun):
    for findpattern, rule in rules:
        if findpattern(noun):
            return rule(noun)


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
        self.lookup = {}
        self.query_map = {}

        # connect to redis
        try:
            self.cache = redis.Redis(
                host=redis_host, port=redis_port, db=redis_db, password=redis_password
            )
        except Exception as error:
            logging.error("Couldnt connect to cache:" + str(error))
            raise

    # TODO can we get this data without having to rely on redis?
    # def get_entites_grakn(self):
    #     logging.info("get all entites")

    #     try:
    #         with GraknClient(uri=self.uri, credentials=self.creds) as client:
    #             with client.session(keyspace=self.keyspace) as session:
    #                 get_all_entities(session)

    #                 return 0
    #     except Exception as error:
    #         logging.error(error)
    #         return -1

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

    def create_db(self, db_name: str) -> int:
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
                    self.query_map = {}
                    self.lookup_map = {}


                    # logging.info(self.rkey)
                    # logging.info(self.rel_map)
                    # self.query_map = curr_keyspace["query_map"]
                    # self.lookup_map = curr_keyspace["lookup_map"]

                else:
                    logging.info("Creating new keypsace in redis")
                    blank_keyspace = {}
                    blank_keyspace["entity_map"] = {}
                    blank_keyspace["rel_map"] = {}
                    blank_keyspace["rules_map"] = {}
                    blank_keyspace["lookup_map"] = {}
                    blank_keyspace["lookup_map"]["Find"] = {}
                    blank_keyspace["lookup_map"]["Compute"] = {}
                    blank_keyspace["lookup_map"]["Cluster"] = {}
                    blank_keyspace["lookup_map"]["Reason"] = {}
                    blank_keyspace["lookup_map"]["Center"] = {}
                    blank_keyspace["query_map"] = {}
                    self.cache.set(rkey, json.dumps(blank_keyspace))

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

        # match (competitors_relationship_1: $x, competitors_relationship_2: $y) isa competitors; get;
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


        #check null case

        # if len(concept_attrs) == 0 and len(rel_actions) == 0:

        #     #do a raw get all query
        #     grakn_query = f"match $x isa {concept}; get;"

        #     run_find_query
        #     return self.raw_graql(grakn_query,"read") 



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

    def get_rel_name_from_ents(self, rel1: str, rel2: str) -> str:

        rels = list(self.rel_map.keys())
        rel_name = ""

        for rel in rels:

            check1 = self.rel_map[rel]["rel1"]["entity"]
            check2 = self.rel_map[rel]["rel2"]["entity"]

            if rel1 == check1 or rel1 == check2:
                if rel2 == check1 or rel2 == check2:
                    rel_name = rel

        return rel_name

    def cond_json_maker(self, cond: str, concept: str, attr_name: str) -> dict:

        cond_json = {}

        # cond_value = f"REPLACE_{concept}_{attr_name}"
        cond_value = f"CODEX_REPLACE"
        cond_string = " that " + cond + " " + cond_value

        cond_json["selected_cond"] = cond
        cond_json["cond_value"] = cond_value
        cond_json["cond_string"] = cond_string

        return cond_json

    def cond_setter(
        self, attr_type: str, attr_name: str, concept: str, rule_num: int
    ) -> str:

        cond_list = []

        if attr_type == "string":
            conds = ["Equals", "Contains"]

            for cond in conds:
                cond_json = self.cond_json_maker(cond, concept, attr_name)
                cond_list.append(cond_json)

        elif attr_type == "long" or attr_type == "double":
            conds = ["Equals", "Less Than", "Greater Than"]

            for cond in conds:
                cond_json = self.cond_json_maker(cond, concept, attr_name)
                cond_list.append(cond_json)

        elif attr_type == "bool":
            conds = ["True", "False"]

            for cond in conds:
                cond_json = self.cond_json_maker(cond, concept, attr_name)
                cond_list.append(cond_json)

        else:
            logging.info("error?")

        return cond_list

    def codex_attr_setter(self, concept: str, is_ent: bool, rule_num):

        # Get Values from entites
        plays_map = {}
        if is_ent:
            attrs = self.entity_map[concept]["cols"]
            attr_list = list(attrs.keys())

            rel_names = self.entity_map[concept]["rels"].keys()

            rel_attrs = []
            for rel in rel_names:
                plays = self.entity_map[concept]["rels"][rel]["plays"]
                with_ent = self.entity_map[concept]["rels"][rel]["with_ent"]
                rel_attrs.append(plays)

                if plays in plays_map:
                    plays_map[plays].append(with_ent)
                else:
                    plays_map[plays] = [with_ent]

            attr_list_comp = attr_list + rel_attrs

        # Get values for relationships
        else:
            attrs = self.rel_map[concept]["cols"]
            attr_list = list(attrs.keys())
            attr_list.remove("codex_details")
            attr_list_comp = attr_list

        # this is the attribute for the entity i.e name

        attr_obj_list = []

        for selected_attr in attr_list_comp:

            attr_json = {}

            if selected_attr in attr_list:
                attr_string = " that have a " + selected_attr

                if is_ent:
                    attr_type = self.entity_map[concept]["cols"][selected_attr]["type"]
                else:
                    attr_type = self.rel_map[concept]["cols"][selected_attr]["type"]

                # TODO can we make this a function
                # check condtion type
                cond_json = self.cond_setter(
                    attr_type, selected_attr, concept, rule_num
                )
                attr_json["attr_concept"] = concept

                attr_json["conds"] = cond_json
                attr_json["attr_type"] = attr_type
                attr_json["attribute"] = selected_attr
                attr_json["attr_string"] = attr_string

                attr_obj_list.append(attr_json)

            else:
                attr_string = " that " + selected_attr

                other_ents = plays_map[selected_attr]

                for selected_ent2 in other_ents:

                    attr_json["rel_ent"] = selected_ent2
                    attr_json["rel_attr"] = selected_attr
                    rel_name = self.get_rel_name_from_ents(concept, selected_ent2)
                    attr_json["rel_name"] = rel_name
                    attr_json["rel_other"] = self.entity_map[selected_ent2]["rels"][
                        rel_name
                    ]["plays"]

                    attr_json["attr_concept"] = selected_ent2
                    attr_string += " " + selected_ent2
                    attrs2 = self.entity_map[selected_ent2]["cols"]
                    attr_list2 = list(attrs2.keys())

                    for selected_attr in attr_list2:
                        attr_type = self.entity_map[selected_ent2]["cols"][
                            selected_attr
                        ]["type"]

                        attr_string += " that have a " + selected_attr

                        cond_json = self.cond_setter(
                            attr_type, selected_attr, selected_ent2, rule_num
                        )

                        attr_json["conds"] = cond_json
                        attr_json["attr_type"] = attr_type
                        attr_json["attribute"] = selected_attr
                        attr_json["attr_string"] = attr_string

                        attr_obj_list.append(attr_json)

        return attr_obj_list

    # call this when entity created
    # users shouldnt have to call this.
    def generate_queries(self, entity_name, entity_type, is_ent):

        query_list = []

        # make all possible concept jsons

        concept_json = {}
        # lock never changes
        concept_json["concept"] = entity_name
        concept_json["concept_type"] = entity_type

        attr_posibilites = []

        # attrs = list(self.entity_map[entity_name]["cols"].keys())
        # to do add rels

        attr_obj_list = self.codex_attr_setter(entity_name, is_ent, 1)

        # there are n attributes,
        concept_json["attrs"] = attr_obj_list

        concept_json["query_strings"] = []

        print(concept_json)

        for attr_obj in attr_obj_list:

            concept_json["query_strings"].append(
                self.query_string_find_maker(entity_name, attr_obj)
            )

        # Start with all find queries

        print("####################")
        print(concept_json)

        query_list = concept_json["query_strings"]
        codex_query_list = []

        codex_query_lookup = {}

        codex_query_lookup = {}
        # codex_query_lookup[entity_name]["concept_type"] = entity_type

        query_counter = 0
        for attr in concept_json["attrs"]:

            attribute = attr["attribute"]

            codex_query_lookup[attribute] = {}

            cond_counter = 0
            for cond in attr["conds"]:

                concept_json = {}
                concept_json["concept"] = entity_name
                concept_json["concept_type"] = entity_type

                attr_json = {}
                attr_json["cond"] = cond
                attr_json["attr_type"] = attr["attr_type"]
                attr_json["attribute"] = attr["attribute"]
                attr_json["attr_string"] = attr["attr_string"]
                attr_json["attr_concept"] = entity_name

                codex_query_lookup[attribute][cond["selected_cond"]] = attr_json
                # codex_query_lookup[entity_name][attribute][cond["selected_cond"]] = attr_json

                concept_json["attrs"] = []
                concept_json["attrs"].append(attr_json)
                concept_json["query_string"] = query_list[query_counter][cond_counter]

                cond_counter += 1
                codex_query_list.append(concept_json)
            query_counter += 1

        pprint.pprint(codex_query_list)

        print("#########")

        pprint.pprint(codex_query_lookup)

        # save queries to redis...

        # get current key space
        curr_keyspace = json.loads(self.cache.get(self.rkey))
        # update entity map
        curr_keyspace["query_map"]

        for codex_query in codex_query_list:
            curr_keyspace["query_map"][codex_query["query_string"]] = codex_query

        # hard code find?
        curr_keyspace["lookup_map"]["Find"][entity_name] = codex_query_lookup
        # update redis
        self.cache.set(self.rkey, json.dumps(curr_keyspace))

    def query_string_find_maker(self, concept: str, attr_obj: dict) -> str:

        print("####################")
        print(attr_obj)

        # attr_len = len(attr_obj_list)
        # attr_counter = 1

        query_list = []

        for cond in attr_obj["conds"]:

            query_string = f"Find {plural(concept)}"
            query_string += f"{attr_obj['attr_string']}{cond['cond_string']}"

            # if not attr_counter == attr_len:
            #     query_string += " and "

            # attr_counter += 1

            query_string += "."
            query_list.append(query_string)

        return query_list

    def list_queries(self):
        codex_queries = list(self.query_map.keys())
        return codex_queries

    def nl_query(self, queries):

        print("will do a nl query")
        # print(self.query_map)

        codex_queries = list(self.query_map.keys())

        # TODO make query if and
        curr_query = queries[0]["query"]

        for codex_query in codex_queries:

            sim_score = similar(codex_query, curr_query)

            print(f"{codex_query}: {sim_score}")

            if curr_query.lower() in codex_query.lower():
                print("boo ya")
                print(codex_query)
                query_obj = self.query_map[codex_query]
                print(query_obj)
                query_obj["attrs"][0]["cond"]["cond_value"] = queries[0]["condition"]

                query_list = [query_obj]

                codex_obj = CodexQueryFind(concepts=query_list, query_string=curr_query)

                answers = self.query(codex_obj)

                print(answers)

    # TODO
    # streamlit example
    # query
    # - find - done
    # - compute - done
    # - cluster - done

    # create rules - done :)
    # re org streamlit app - done
    # show graph? codex_viz - done

    # "real data" - done
    # topics blobls and tweets?
    #  topics
    #  tweets - text, char length, has_link, is_retweet,
    #  user - name, num_followers, following, verified

    # date quieres - check if string matches date format, if not then its a string
    # not quieres?
    # api? - This is the api

    # biz case - shower

    # generate all quries, use natrual language to make requests

    # import from grakn
