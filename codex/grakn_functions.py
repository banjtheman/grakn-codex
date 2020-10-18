import json
import logging
from typing import Tuple

import pandas as pd
from grakn.client import GraknClient, ValueType

from pandas.api.types import is_string_dtype


logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


# # grakn datatypes
# data_type_match = {}
# data_type_match["string"] = DataType.STRING
# data_type_match["long"] = DataType.LONG
# data_type_match["double"] = DataType.DOUBLE
# data_type_match["date"] = DataType.DATE
# data_type_match["boolean"] = DataType.BOOLEAN


def turn_value_type(val: ValueType):
    """
    Purpose:
        Get string value type based on grakn value
    Args:
        val: Grakn Value Type
    Returns:
        string value based on Grakn value
    """
    if val == ValueType.STRING:
        return "string"

    if val == ValueType.DOUBLE:
        return "double"

    if val == ValueType.LONG:
        return "long"

    if val == ValueType.BOOLEAN:
        return "bool"

    return "date"


def rev_value_type(val: str) -> ValueType:
    """
    Purpose:
        Get Grakn value type based on string value
    Args:
        val: string value type
    Returns:
        Grakn Value based on string valye
    """
    if val == "string":
        return ValueType.STRING

    if val == "double":
        return ValueType.DOUBLE

    if val == "long":
        return ValueType.LONG

    if val == "bool":
        return ValueType.BOOLEAN

    return ValueType.DATETIME


def check_types(df: pd.DataFrame, col: str) -> ValueType:
    """
    Purpose:
       Infer dataframe column Grakn type
    Args:
        df: Dataframe
        col: column of dataframe
    Returns:
        Grakn Value Type
    """
    if is_string_dtype(df[col]):
        return ValueType.STRING

    if str(df[col].dtype) == "float64":
        return ValueType.DOUBLE

    if str(df[col].dtype) == "int64":
        return ValueType.LONG

    if str(df[col].dtype) == "bool":
        return ValueType.BOOLEAN

    # TODO figure out how to get dates
    return ValueType.DATETIME


def create_relationship_query(entity_map: dict, rel_name: str, rel_map: dict) -> str:
    """
    Purpose:
       Create relationship definition query
    Args:
        entity_map: Entities map
        rel_name: The name of the relationship
        rel_map:  Relationship Map
    Returns:
        graql_insert_query: The query to run
    """
    graql_insert_query = "define " + rel_name + " sub relation, "

    # relates 1
    graql_insert_query += "relates " + rel_map["rel1"]["role"] + ", "
    # relates2
    graql_insert_query += "relates " + rel_map["rel2"]["role"] + ", "

    # add our custom attr
    # graql_insert_query += 'has codex_details'

    # check if attrs
    attr_length = len(entity_map.keys())
    # if attr_length == 0:
    #     graql_insert_query += ";"
    #     return graql_insert_query

    # #check if blank attr
    # graql_insert_query += ","
    attr_counter = 1

    for attr in entity_map:

        graql_insert_query += "has " + str(attr)

        # check if last
        if attr_counter == attr_length:
            graql_insert_query += ";"
        else:
            graql_insert_query += ", "
        attr_counter += 1

    return graql_insert_query


def find_cond_checker_rule(attr: dict, dif_1: str) -> Tuple[str, list]:
    """
    Purpose:
       Define condtion checker for the graql query
    Args:
        attr: dictionary of attributes
    Returns:
        grakn_query: The query to run
        contain_statements: list of contain queries to run
    """
    query_check_type = attr["attr_type"]
    cond_type = attr["cond"]["selected_cond"]
    cond_value = attr["cond"]["cond_value"]
    curr_attr = attr["attribute"]
    attr_concept = attr["attr_concept"]

    grakn_query = ""

    # contain statements go at end?
    # match $Company isa Company, has name $name , has profit > 300.0;{ $name contains "Two ";};get; offset 0; limit 30;
    contain_statements = []

    if query_check_type:
        # check if string
        if query_check_type == "string":

            # now need to check for each condtion

            # match $Company isa Company, has name "Two"
            if cond_type == "Equals":
                grakn_query += ' "' + cond_value + '"'

            # match $Company isa Company, has name $name; { $name contains "Two";}; get; offset 0; limit 30;
            if cond_type == "Contains":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f'{{ ${attr_concept}_{curr_attr}{dif_1} contains "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

        # if query_check_type == "double" or query_check_type == "long":
        else:
            if cond_type == "Equals":
                grakn_query += f" {cond_value}"
            if cond_type == "Greater Than":
                grakn_query += f" > {cond_value}"
            if cond_type == "Less Than":
                grakn_query += f" < {cond_value}"
    return grakn_query, contain_statements


def find_cond_checker(attr: dict) -> Tuple[str, list]:
    """
    Purpose:
       Define condtion checker for the graql query
    Args:
        attr: dictionary of attributes
    Returns:
        grakn_query: The query to run
        contain_statements: list of contain queries to run
    """
    query_check_type = attr["attr_type"]
    cond_type = attr["cond"]["selected_cond"]
    cond_value = attr["cond"]["cond_value"]
    curr_attr = attr["attribute"]
    attr_concept = attr["attr_concept"]

    grakn_query = ""

    # contain statements go at end?
    # match $Company isa Company, has name $name , has profit > 300.0;{ $name contains "Two ";};get; offset 0; limit 30;
    contain_statements = []

    if query_check_type:
        # TODO add check for date

        # check if string
        if query_check_type == "string":

            # now need to check for each condtion

            # match $Company isa Company, has name "Two"
            if cond_type == "Equals":
                grakn_query += ' "' + cond_value + '"'

            # match $Company isa Company, has name $name; { $name contains "Two";}; get; offset 0; limit 30;
            if cond_type == "Contains":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f'{{ ${attr_concept}_{curr_attr} contains "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

        # if query_check_type == "double" or query_check_type == "long":
        else:
            if cond_type == "Equals":
                grakn_query += f" {cond_value}"
            if cond_type == "Greater Than":
                grakn_query += f" > {cond_value}"
            if cond_type == "Less Than":
                grakn_query += f" < {cond_value}"
    return grakn_query, contain_statements


# Example Queries

# match $x isa Company; $a isa Sample, has name "widget x";
# (produces: $x, produced: $a) isa Productize;

# match $Company isa Company, $Product isa Product has name "widget x";
# (Product: $Product, Company: $Product) isa Productize;get;

# match $Company isa Company, has name "Acme"; $Product isa Product, has name $name;(produces: $Company, produced: $Product) isa Productize;{ $name contains "widget x";};get;


def cluster_query(session, query_object: dict) -> dict:
    """
    Purpose:
       Construct the compute graql query
    Args:
        session: The Grakn session
        query_object: The details of the query
    Returns:
        compute_results: Answers to the queries
    """
    cluster_obj = {}
    logging.info(query_object.query)
    # well just do the query...

    # {'query_type': 'centrality', 'choice': 'subgraph', 'concepts': ['Company', 'Product', 'Productize'], 'query_string': 'compute centrality in [Company, Product, Productize], using degree'}
    if "concepts" in query_object.query:

        concepts = query_object.query["concepts"]
        query = query_object.query["query_string"]

        cluster_obj["answers"] = run_cluster_query(session, query, concepts)

        # if "given_type" in query_object.query:
        #     given_type = query_object.query["given_type"]

        #     cluster_obj["answers"] = centrallity_query(session,query,given_type)
        # else:
        #     cluster_obj["answers"] = run_cluster_query(session,query,concepts)

    return cluster_obj


# ["Count", "Sum", "Maximum", "Minimum", "Mean" ,"Median" , "Standard Deviation"]
compute_action_map = {
    "Sum": "sum",
    "Maximum": "max",
    "Minimum": "min",
    "Mean": "mean",
    "Median": "median",
    "Standard Deviation": "std",
}


def compute_query(session, query_object: dict) -> dict:
    """
    Purpose:
       Construct the compute graql query
    Args:
        session: The Grakn session
        query_object: The details of the query
    Returns:
        compute_results: Answers to the queries
    """
    queries = query_object.queries
    compute_results = {}

    for action in queries:

        compute_results[action] = []

        if action == "Count":

            compute_objs = queries[action]
            for compute_obj in compute_objs:
                results_obj = {}
                concept = compute_obj["concept"]
                if concept == "All Concepts":
                    graql_query = f"compute count;"
                else:
                    graql_query = f"compute count in {concept};"

                results_obj["concept"] = concept
                results_obj["query"] = graql_query
                logging.info(graql_query)

                answer = run_compute_query(session, graql_query)
                results_obj["answer"] = answer
                compute_results[action].append(results_obj)

        else:
            compute_objs = queries[action]
            for compute_obj in compute_objs:
                results_obj = {}
                concept = compute_obj["concept"]
                attr = compute_obj["attr"]

                graql_query = (
                    f"compute {compute_action_map[action]} of {attr}, in {concept};"
                )

                results_obj["concept"] = concept
                results_obj["query"] = graql_query
                logging.info(graql_query)

                answer = run_compute_query(session, graql_query)
                results_obj["answer"] = answer
                compute_results[action].append(results_obj)

    return compute_results


def attr_make_rule_query(concept: str, dif_1: str, dif_2: str) -> str:
    """
    Purpose:
       Construct the rule graql query for attrs
    Args:
        concept: the current concept
        dif_1: The first condition var
        dif_2: The second condition var
    Returns:
        grakn_query: the query string
    """
    grakn_query = ""
    contain_statements = []
    rel_statements = []

    attrs_len = len(concept["attrs"])
    attr_counter = 1

    for attr in concept["attrs"]:

        if "rel_ent" in attr:
            rel_query_string = f"; ${attr['rel_ent']}{dif_2} isa {attr['rel_ent']}"
            rel_query_string += f", has {attr['attribute']}"

            grakn_query_cond, contains_array = find_cond_checker_rule(attr, dif_2)
            rel_query_string += grakn_query_cond
            contain_statements.extend(contains_array)

            rel_query_string += (
                ";(" + attr["rel_attr"] + ": $" + concept["concept"] + f"{dif_1}, "
            )
            rel_query_string += (
                attr["rel_other"]
                + ": $"
                + attr["rel_ent"]
                + f"{dif_2}) isa "
                + attr["rel_name"]
            )
            rel_statements.append(rel_query_string)

        else:
            grakn_query += f", has {attr['attribute']}"

            grakn_query_cond, contains_array = find_cond_checker(attr)

            grakn_query += grakn_query_cond
            contain_statements.extend(contains_array)

        if attr_counter == attrs_len:

            # check rel statements
            for rel in rel_statements:
                grakn_query += f"{rel}"

            # check conditions
            for cond in contain_statements:
                grakn_query += f";{cond}"

            grakn_query += ";"
            logging.info(grakn_query)
        else:
            grakn_query += " "

        attr_counter += 1

    return grakn_query


def rule_query(session, query_object: dict) -> dict:
    """
    Purpose:
       Construct the rule graql query
    Args:
        session: The Grakn session
        query_object: The details of the query
    Returns:
        rules_map: info on the rule
    """
    logging.info(query_object)

    cond1 = query_object.rule["cond1"]
    cond2 = query_object.rule["cond2"]
    rule_name = query_object.rule["name"]

    # define statement
    # WE HAVE TO define the relationship as well!!!
    graql_string = f"define {rule_name} sub relation, relates {rule_name}_relationship_1, relates {rule_name}_relationship_2;"
    graql_string += f"{cond1['concept']} sub entity, plays {rule_name}_relationship_1, plays {rule_name}_relationship_2;"
    graql_string += f"{cond2['concept']} sub entity, plays {rule_name}_relationship_1, plays {rule_name}_relationship_2;"
    graql_string += f"{rule_name}-rule sub rule,"

    # when statement
    graql_string += "when {"

    # cond1
    graql_string += f"${cond1['concept']}_A isa {cond1['concept']}"

    graql_string += attr_make_rule_query(cond1, "_A", "_X")

    logging.info("query after cond1")
    logging.info(graql_string)

    # cond2
    graql_string += f"${cond2['concept']}_B isa {cond2['concept']}"

    graql_string += attr_make_rule_query(cond1, "_B", "_Y")

    logging.info("query after cond2")
    logging.info(graql_string)

    # not equal check

    graql_string += f"${cond1['concept']}_A != ${cond2['concept']}_B;}},"

    # then statement
    graql_string += "then {"

    graql_string += f"({rule_name}_relationship_1: ${cond1['concept']}_A, {rule_name}_relationship_2: ${cond2['concept']}_B) isa {rule_name};}};"

    logging.info(graql_string)

    try:
        raw_query_write_grakn(session, graql_string)
    except Exception as error:
        logging.error(error)

    rules_map = {}
    rules_map["graql_query"] = graql_string
    rules_map["rule_string"] = query_object.rule_string
    rules_map["cond1"] = cond1
    rules_map["cond2"] = cond2

    return rules_map


def find_query(session, query_object: dict) -> dict:
    """
    Purpose:
       Construct the find graql query
    Args:
        session: The Grakn session
        query_object: The details of the query
    Returns:
        answers: Answers to the queries
    """
    # for each concept make a query
    concepts = []

    concept_queries = []
    for concept in query_object.concepts:
        concepts.append(concept["concept"])
        grakn_query = ""
        contain_statements = []
        rel_statements = []

        attrs_len = len(concept["attrs"])
        attr_counter = 1
        grakn_query += f"match ${concept['concept']} isa {concept['concept']}"

        logging.info(grakn_query)

        for attr in concept["attrs"]:

            if "rel_ent" in attr:
                rel_query_string = f"; ${attr['rel_ent']} isa {attr['rel_ent']}"
                rel_query_string += f", has {attr['attribute']}"

                grakn_query_cond, contains_array = find_cond_checker(attr)
                rel_query_string += grakn_query_cond
                contain_statements.extend(contains_array)

                rel_query_string += (
                    ";(" + attr["rel_attr"] + ": $" + concept["concept"] + ", "
                )
                rel_query_string += (
                    attr["rel_other"]
                    + ": $"
                    + attr["rel_ent"]
                    + ") isa "
                    + attr["rel_name"]
                )
                rel_statements.append(rel_query_string)

            else:
                grakn_query += f", has {attr['attribute']}"

                grakn_query_cond, contains_array = find_cond_checker(attr)

                grakn_query += grakn_query_cond
                contain_statements.extend(contains_array)

            if attr_counter == attrs_len:

                # check rel statements
                for rel in rel_statements:
                    grakn_query += f"{rel}"

                # check conditions
                for cond in contain_statements:
                    grakn_query += f";{cond}"

                grakn_query += ";get;"
                concept_queries.append(grakn_query)
                logging.info(grakn_query)
            else:
                grakn_query += " "

            attr_counter += 1

    logging.info("Here is the graql")
    for query in concept_queries:
        logging.info(query)

    answers = run_find_query(session, concept_queries, concepts)
    return answers


# TODO do we need this function?
def get_ent_obj(concept: str) -> dict:
    """
    Purpose:
       Get the entity object?
    Args:
        concept: The string for the entity
    Returns:
        ent_obj: the object for the entity
    """
    logging.info("checking concept")

    ent_obj = {}
    attr_iterator = concept.attributes()

    for attr in attr_iterator:
        logging.info(attr)
        ent_obj[attr.type().label()] = attr.value()

        # TODO any speical case for codex_details?

    return ent_obj


def centrallity_query(session, graql_query: str, concept: str):
    """
    Purpose:
       Excute the graql centrallity query
    Args:
        session: The Grakn session
        graql_query: the query to run
        concept: concept to check
    Returns:
        ent_data: Answers to the query
    """
    ent_data = []
    ent_map = {}

    with session.transaction().read() as read_transaction:
        answer_iterator = read_transaction.query(graql_query)
        for answer in answer_iterator:
            attr_iterator = answer.map().get(concept).attributes()
            ent_obj = {}
            ent_obj["id"] = answer.map().get(concept).id
            rel_iterator = answer.map().get(concept).relations()

            for rel in rel_iterator:
                rel_label = rel.type().label()
                logging.info("checking rel_label")
                print(rel_label)

                # check if label is in ent_map
                if rel_label in ent_map:
                    # print("got inside rel label")
                    rel_attr_iterator = rel.attributes()
                    rel_obj = {}
                    for attr in rel_attr_iterator:
                        # only care about codex_details? so do we need for loop?
                        rel_obj[attr.type().label()] = attr.value()
                    try:
                        codex_details = json.loads(rel_obj["codex_details"])
                        # print(codex_details)
                        rel_obj[codex_details["rel1_name"]] = codex_details[
                            "rel1_value"
                        ]
                        rel_obj[codex_details["rel2_name"]] = codex_details[
                            "rel2_value"
                        ]
                        # print(rel_obj)

                        # find out which one ent is part of
                        if concept == ent_map[rel_label]["rel1"]:
                            # print("hello")
                            ent_rel_key = (
                                codex_details["rel1_name"]
                                + "_"
                                + codex_details["rel2_value"]
                            )
                            ent_obj[ent_rel_key] = 1
                        if concept == ent_map[rel_label]["rel2"]:
                            # print("hello")
                            ent_rel_key = (
                                codex_details["rel2_name"]
                                + "_"
                                + codex_details["rel1_value"]
                            )
                            ent_obj[ent_rel_key] = 1
                    except:
                        continue

            for attr in attr_iterator:
                ent_obj[attr.type().label()] = attr.value()
            ent_data.append(ent_obj)

    return ent_data


def run_cluster_query(session, graql_query: str, concepts: dict) -> dict:
    """
    Purpose:
       Excute the graql centrallity query
    Args:
        session: The Grakn session
        graql_query: the query to run
        concepts: concepts to check
    Returns:
        cluster_obj: Answers to the query
    """
    connected_map = {}
    ent_map = {}
    clusters = []
    cluster_obj = {}
    cluster_obj["clusters"] = []
    cluster_obj["ent_map"] = {}

    for concept in concepts:
        ent_map[concept] = {}
        ent_map[concept]["data"] = {}

    with session.transaction().read() as read_transaction:
        answer_iterator = read_transaction.query(graql_query)
        curr_measurement = 0
        for answer in answer_iterator:
            try:
                curr_measurement = answer.measurement()
            except:
                curr_measurement = curr_measurement + 1

            for concept in concepts:
                ent_map[concept]["data"][curr_measurement] = []

            connected_map[curr_measurement] = {}
            connected_map[curr_measurement]["Attrs"] = []
            connected_map[curr_measurement]["Ents"] = []
            connected_map[curr_measurement]["Rels"] = []
            clusters.append(curr_measurement)

            for node_id in answer.set():
                node = read_transaction.get_concept(node_id)
                # check what type it is and do action
                node_type = node.type()

                if node_type.is_entity_type():
                    # print("ent")
                    attr_iterator = node.attributes()
                    concept = node_type.label()
                    ent_obj = {}
                    for attr in attr_iterator:
                        ent_obj[attr.type().label()] = attr.value()
                    connected_map[curr_measurement]["Ents"].append(ent_obj)
                    ent_map[concept]["data"][curr_measurement].append(ent_obj)
                if node_type.is_relation_type():
                    logging.info("AT a rel type")
                    attr_iterator = node.attributes()
                    concept = node_type.label()
                    rel_obj = {}
                    for attr in attr_iterator:
                        rel_obj[attr.type().label()] = attr.value()
                    codex_details = json.loads(rel_obj["codex_details"])

                    logging.info("Codex Details:")
                    logging.info(codex_details)

                    rel_obj[codex_details["rel1_role"]] = codex_details["rel1_value"]
                    rel_obj[codex_details["rel1_role"]] = codex_details["rel2_value"]

                    connected_map[curr_measurement]["Rels"].append(rel_obj)
                    ent_map[concept]["data"][curr_measurement].append(rel_obj)

                if node_type.is_attribute_type():
                    attr_obj = {}
                    # print("attr")
                    node_val = node.value()
                    node_label = node.type().label()
                    attr_obj["key"] = node_label
                    attr_obj["value"] = node_val
                    connected_map[curr_measurement]["Attrs"].append(attr_obj)

                print("end of loop")
                print(ent_map)
                print(clusters)

    cluster_obj["ent_map"] = ent_map
    cluster_obj["clusters"] = clusters
    logging.info("here is cluster obj")
    logging.info(cluster_obj)

    return cluster_obj


def raw_query_write_grakn(session, graql_query: str) -> None:
    """
    Purpose:
       Excute the graql query
    Args:
        session: The Grakn session
        graql_query: the query to run
    Returns:
        ent_map: Answers to the queries by entity
    """
    # run rule insert here
    with session.transaction().write() as transaction:
        logging.info("Executing Graql Query: " + graql_query)
        transaction.query(graql_query)
        transaction.commit()


def raw_query_read_grakn(session, graql_query: str) -> None:
    """
    Purpose:
       Excute the graql query
    Args:
        session: The Grakn session
        graql_query: the query to run
    Returns:
        ent_map: Answers to the queries by entity
    """
    ent_map = {}

    with session.transaction().read() as read_transaction:
        answer_iterator = read_transaction.query(graql_query, explain=True)
        for answer in answer_iterator:
            try:

                answer_concepts = list(answer.map().keys())

                logging.info(answer_concepts)

                for key in answer_concepts:

                    ent_map[key] = {}
                    ent_map[key]["concepts"] = []

                    ent_obj = {}
                    curr_ent = answer.map().get(key)
                    logging.info(key)

                    cur_val = read_transaction.get_concept(curr_ent.id)

                    # then its a rule
                    if cur_val.is_inferred():
                        rels = cur_val.role_players()
                        for rel in rels:
                            ent_obj = {}
                            attr_iterator = rel.attributes()

                            for attr in attr_iterator:
                                logging.info(attr.value())
                                ent_obj[attr.type().label()] = attr.value()

                            ent_map[key]["concepts"].append(ent_obj)

                    if answer.has_explanation():
                        explanation = answer.explanation()
                        explanation_map = {}
                        logging.info(explanation.get_answers())

                        for exp_concept in explanation.get_answers():
                            logging.info(exp_concept.map())

                            concept_keys = list(exp_concept.map().keys())

                            for concept_key in concept_keys:
                                logging.info(concept_key)

                                curr_concept = exp_concept.map().get(concept_key)
                                cur_val = read_transaction.get_concept(curr_concept.id)

                                # check if attr
                                explanation_map[concept_key] = {}

                                if cur_val.is_attribute():
                                    explanation_map[concept_key][
                                        cur_val.type().label()
                                    ] = cur_val.value()

                                else:
                                    attr_iterator = cur_val.attributes()
                                    for attr in attr_iterator:
                                        logging.info(attr.value())
                                        explanation_map[concept_key][
                                            attr.type().label()
                                        ] = attr.value()

                        ent_map[key]["explanation"] = explanation_map

                    else:

                        attr_iterator = cur_val.attributes()

                        for attr in attr_iterator:
                            ent_obj[attr.type().label()] = attr.value()

                        ent_map[key].append(ent_obj)

            except Exception as error:
                logging.error(error)

    logging.info(ent_map)
    return ent_map


def run_compute_query(session, graql_query: str) -> dict:
    """
    Purpose:
       Excute the graql query
    Args:
        session: The Grakn session
        graql_query: the query to run
    Returns:
        ent_map: Answers to the queries by entity
    """

    with session.transaction().read() as read_transaction:
        answer_iterator = read_transaction.query(graql_query)
        for answer in answer_iterator:
            print(str(answer.number()))
            num = answer.number()
            return num


def run_find_query(session, queries: list, concepts: list) -> dict:
    """
    Purpose:
       Excute the graql query
    Args:
        session: The Grakn session
        queries: the queriesto run
        concepts: The concepts we are looking for
    Returns:
        ent_map: Answers to the queries by entity
    """
    ent_map = {}
    concept_counter = 0
    for query in queries:

        curr_concept = concepts[concept_counter]

        ent_map[curr_concept] = []

        with session.transaction().read() as read_transaction:
            answer_iterator = read_transaction.query(query)
            for answer in answer_iterator:
                try:
                    # logging.info(answer.map())

                    answer_concepts = list(answer.map().keys())

                    # logging.info(answer_concepts)

                    for key in answer_concepts:

                        if key == curr_concept:

                            ent_obj = {}
                            curr_ent = answer.map().get(curr_concept)
                            # logging.info(curr_ent.id)

                            cur_val = read_transaction.get_concept(curr_ent.id)
                            # logging.info(cur_val)

                            attr_iterator = cur_val.attributes()

                            for attr in attr_iterator:
                                ent_obj[attr.type().label()] = attr.value()

                            ent_map[curr_concept].append(ent_obj)

                except Exception as error:
                    logging.error(error)

        concept_counter += 1

    logging.info(ent_map)
    return ent_map


def turn_to_df(answers: list) -> pd.DataFrame:
    """
    Purpose:
       Transform the answers to a dataframe
    Args:
        answers: list of answers
    Returns:
        df: Answers as a dataframe
    """
    if len(answers) == 0:
        return None

    cols = answers[0].keys()

    df_map = {}
    for col in cols:
        df_map[col] = []

    for answer in answers:
        for col in answer:
            df_map[col].append(answer[col])

    df = pd.DataFrame.from_dict(df_map)
    return df


def query_grakn(session, query_object) -> dict:
    """
    Purpose:
       Gateway to the grakn queries
    Args:
        session: The Grakn session
        query_object: the query_object
    Returns:
        answers_df_map: Answers to the queries by entity
    """
    logging.info(f"{query_object}")
    answers = {}

    if query_object.action == "Find":
        answers = find_query(session, query_object)
        answers_df_map = {}

        for answer in answers:
            answers_df_map[answer] = turn_to_df(answers[answer])
        return answers_df_map

    elif query_object.action == "Compute":
        answers = compute_query(session, query_object)
        return answers

    elif query_object.action == "Cluster":
        answers = cluster_query(session, query_object)
        return answers

    elif query_object.action == "Rule":
        answers = rule_query(session, query_object)
        return answers

    else:
        return answers


# rel1: "Company"
# rel1_name: "produces"
# rel2: "Sample"
# rel2_name: "produced"
# rel_name: "Productize"
# [{"rel1_name":"produces","rel1":"Company","rel1_value":"Company A"}]


def commit_relationship(row: pd.Series, session, rel_name: str, rel_map: dict) -> None:
    """
    Purpose:
       Insert statement for relationships
    Args:
        row: The current row of the dataframe
        session: The Grakn session
        rel_name: the relationship name
        rel_map: the relationship map
    Returns:
        N/A
    """
    rel1_role = rel_map["rel1"]["role"]
    rel2_role = rel_map["rel2"]["role"]

    graql_insert_query = (
        "match $"
        + str(rel_map["rel1"]["role"])
        + " isa "
        + str(rel_map["rel1"]["entity"])
    )

    # check key type
    if rel_map["rel1"]["key_type"] == "string":
        graql_insert_query += (
            ", has " + str(rel_map["rel1"]["key"]) + ' "' + str(row[rel1_role]) + '";'
        )

    # TODO what is query if not a string?

    # rel 2
    graql_insert_query += (
        "$" + str(rel_map["rel2"]["role"]) + " isa " + str(rel_map["rel2"]["entity"])
    )

    # check key type
    if rel_map["rel2"]["key_type"] == "string":
        graql_insert_query += (
            ", has " + str(rel_map["rel2"]["key"]) + ' "' + str(row[rel2_role]) + '";'
        )

    # the insert statement
    graql_insert_query += (
        " insert $"
        + str(rel_name)
        + "("
        + str(rel_map["rel1"]["role"])
        + ": $"
        + str(rel_map["rel1"]["role"])
        + ", "
        + str(rel_map["rel2"]["role"])
        + ": $"
        + str(rel_map["rel2"]["role"])
        + ") isa "
        + str(rel_name)
        + "; "
    )

    # create codex details
    codex_obj = {}
    codex_obj["rel1_key"] = rel_map["rel1"]["key"]
    codex_obj["rel1_value"] = row[rel1_role]
    codex_obj["rel1_role"] = rel_map["rel1"]["role"]
    codex_obj["rel2_key"] = rel_map["rel2"]["key"]
    codex_obj["rel2_value"] = row[rel2_role]
    codex_obj["rel2_role"] = rel_map["rel2"]["role"]

    codex_string = json.dumps(codex_obj)
    graql_insert_query += (
        "$" + str(rel_name) + " has codex_details '" + codex_string + "'"
    )

    row_attrs = list(rel_map["cols"].keys())
    logging.info(row_attrs)
    row_attrs.remove("codex_details")
    logging.info(row_attrs)

    attr_len = len(row_attrs)
    attr_counter = 1

    if attr_len > 0:
        graql_insert_query += ", "

        for attr in row_attrs:

            if rel_map["cols"][attr]["type"] == "string":
                graql_insert_query += (
                    "has " + str(attr) + ' "' + str(sanitize_text(row[attr])) + '"'
                )
            else:
                graql_insert_query += "has " + str(attr) + " " + str(row[attr])

            # check if last
            if attr_counter == attr_len:
                graql_insert_query += ";"
            else:
                graql_insert_query += ", "
            attr_counter += 1
    else:
        graql_insert_query += ";"

    # do insert here
    with session.transaction().write() as transaction:
        logging.info("Executing Graql Query: " + graql_insert_query)
        transaction.query(graql_insert_query)
        transaction.commit()


def add_relationship_data(
    df: pd.DataFrame, rel_map: dict, rel_name: str, session
) -> None:
    """
    Purpose:
       add relationship data to Grakn
    Args:
        df: The data to add
        rel_map: the relationship map
        rel_name: the relationship name
        session: The Grakn session
    Returns:
        N/A
    """
    logging.info("Starting add relationships")

    # for each row in csv, add relationship
    df.apply(lambda row: commit_relationship(row, session, rel_name, rel_map), axis=1)


def add_relationship_to_entities(rel_map):
    """
    Purpose:
       define entity relationship
    Args:
        rel_map: the relationship map
    Returns:
        graql_insert_query - graql query
    """
    graql_insert_query = (
        "define "
        + rel_map["rel1"]["entity"]
        + " plays "
        + rel_map["rel1"]["role"]
        + "; "
    )
    graql_insert_query += (
        rel_map["rel2"]["entity"] + " plays " + rel_map["rel2"]["role"] + ";"
    )
    return graql_insert_query


def create_entity_query(df: pd.DataFrame, entity_name: str, entity_key=None) -> str:
    """
    Purpose:
       define entity graql query
    Args:
        df: Entity data
        entity_name: Name of entity
        entity_key: the key for the entity
    Returns:
        graql_insert_query - graql query
    """
    graql_insert_query = "define " + entity_name + " sub entity"

    # check if attrs
    attr_length = len(df.columns)
    if attr_length == 0:
        graql_insert_query += ";"
        return graql_insert_query

    graql_insert_query += ","
    attr_counter = 1

    added_key = False

    for attr in df.columns:

        # check if attr is key
        if entity_key is not None and not added_key:
            if str(attr) == entity_key:
                graql_insert_query += "key " + str(attr)
                added_key = True
        else:
            graql_insert_query += "has " + str(attr)

        # check if last
        if attr_counter == attr_length:
            graql_insert_query += ";"
        else:
            graql_insert_query += ", "
        attr_counter += 1

    return graql_insert_query


def sanitize_text(text: str) -> str:
    """
    Purpose:
       clean text for grakn insert
    Args:
        text: text to cleam
    Returns:
        text - cleaned text
    """
    # will add more when issues come
    text = str(text).replace("/", "_").replace(".", "_dot_")
    return str(text)


def commit_entity(row: pd.Series, session, entity_name: str, entity_map: dict) -> None:
    """
    Purpose:
       Insert statement for entites
    Args:
        row: The current row of the dataframe
        session: The Grakn session
        entity_name: the entity name
        entity_map: the entity map
    Returns:
        N/A
    """
    current_ent = entity_map[entity_name]

    entity_len = len(current_ent["cols"].keys())
    entity_counter = 1
    graql_insert_query = "insert $c isa " + entity_name + ", "

    for col in current_ent["cols"].keys():

        if current_ent["cols"][col]["type"] == "string":
            graql_insert_query += (
                "has " + str(col) + ' "' + str(sanitize_text(row[col])) + '"'
            )
        else:
            graql_insert_query += "has " + str(col) + " " + str(row[col])

        # check if last
        if entity_counter == entity_len:
            graql_insert_query += ";"
        else:
            graql_insert_query += ", "
        entity_counter += 1

    try:
        with session.transaction().write() as transaction:
            logging.info("Executing Graql Query: " + graql_insert_query)
            transaction.query(graql_insert_query)
            transaction.commit()
    except Exception as e:
        logging.info("Query failed: " + graql_insert_query)
        logging.info(str(e))


def add_entities_into_grakn(
    session, df: pd.DataFrame, entity_name: str, entity_map: dict
) -> None:
    """
    Purpose:
       add entites data to Grakn
    Args:
        session: The Grakn session
        df: The data to add
        entity_name: the entity name
        entity_map: the entity map
    Returns:
        N/A
    """
    logging.info("adding entities")
    # for each row in csv, add an entity
    df.apply(lambda row: commit_entity(row, session, entity_name, entity_map), axis=1)


# TODO do we need this function?
# def get_all_entities(session):

#     with session.transaction().write() as transaction:
#         entity_type = transaction.get_schema_concept("entity")
#         all_entities_iter = entity_type.instances()
#         someEntity = next(all_entities_iter)

#         for ent in all_entities_iter:
#             print(ent.type().label())


def load_entity_into_grakn(
    session, df: pd.DataFrame, entity_name: str, entity_key=None
) -> dict:
    """
    Purpose:
       load entites into Grakn
    Args:
        session: The Grakn session
        df: The data to add
        entity_name: the entity name
        entity_key: the entity key
    Returns:
        entity_map: entity data
    """
    # sample entity
    entity_map = {}

    # make attrs
    for col in df.columns:
        with session.transaction().write() as transaction:
            logging.info(col)
            entity_map[col] = {}
            current_type = check_types(df, col)
            entity_map[col]["type"] = turn_value_type(current_type)

            transaction.put_attribute_type(col, current_type)
            transaction.commit()

    # make entity
    with session.transaction().write() as transaction:
        graql_insert_query = create_entity_query(df, entity_name, entity_key)
        print("Executing Graql Query: " + graql_insert_query)
        transaction.query(graql_insert_query)
        transaction.commit()

    return entity_map


def load_relationship_into_grakn(
    session, df: pd.DataFrame, cols: list, rel_name: str, rel_map: dict
) -> dict:
    """
    Purpose:
       load relationships into Grakn
    Args:
        session: The Grakn session
        df: The data to add
        rel_name: the relationship name
        rel_map: the relationship map
    Returns:
        entity_map: relationship data
    """
    # sample entity
    entity_map = {}

    # our hardcoded attribute
    entity_map["codex_details"] = {}
    entity_map["codex_details"]["type"] = "string"

    with session.transaction().write() as transaction:
        transaction.put_attribute_type("codex_details", ValueType.STRING)
        transaction.commit()

    # make attrs
    for col in cols:
        with session.transaction().write() as transaction:
            logging.info(col)
            entity_map[col] = {}
            current_type = check_types(df, col)
            entity_map[col]["type"] = turn_value_type(current_type)

            transaction.put_attribute_type(col, current_type)
            transaction.commit()

    # make relationships
    with session.transaction().write() as transaction:
        graql_insert_query = create_relationship_query(entity_map, rel_name, rel_map)
        print("Executing Graql Query: " + graql_insert_query)
        transaction.query(graql_insert_query)
        transaction.commit()

    # update entities with relationship
    with session.transaction().write() as transaction:
        graql_insert_query = add_relationship_to_entities(rel_map)
        print("Executing Graql Query: " + graql_insert_query)
        transaction.query(graql_insert_query)
        transaction.commit()

    return entity_map
