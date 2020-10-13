import json
import logging

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


def turn_value_type(val):

    if val == ValueType.STRING:
        return "string"

    if val == ValueType.DOUBLE:
        return "double"

    if val == ValueType.LONG:
        return "long"

    if val == ValueType.BOOLEAN:
        return "bool"

    return "date"


def rev_value_type(val):

    if val == "string":
        return ValueType.STRING

    if val == "double":
        return ValueType.DOUBLE

    if val == "long":
        return ValueType.LONG

    if val == "bool":
        return ValueType.BOOLEAN

    return ValueType.DATETIME


def check_types(df: pd.DataFrame, col: str):

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


def create_relationship_query(session, entity_map: dict, rel_name: str, rel_map: dict):

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


def find_cond_checker(attr: dict) -> str:

    query_check_type = attr["attr_type"]
    cond_type = attr["cond"]["selected_cond"]
    cond_value = attr["cond"]["cond_value"]
    curr_attr = attr["attribute"]

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
                grakn_query += f" ${curr_attr}"
                contain_string = f'{{ ${curr_attr} contains "{cond_value}";}}'
                contain_statements.append(contain_string)

        if query_check_type == "double" or query_check_type == "long":
            if cond_type == "Equals":
                grakn_query += f" {cond_value}"
            if cond_type == "Greater Than":
                grakn_query += f" > {cond_value}"
            if cond_type == "Less Than":
                grakn_query += f" < {cond_value}"

    return grakn_query, contain_statements


def find_query(session, query_object: dict) -> str:
    print("ok")

    # for each concept make a query
    grakn_query = ""

    concepts_len = len(query_object.concepts)
    concept_counter = 1
    contain_statements = []
    for concept in query_object.concepts:

        grakn_query += f"match ${concept['concept']} isa {concept['concept']}"

        for attr in concept["attrs"]:

            if "rel_ent" in attr:
                pass
            else:
                grakn_query += f", has {attr['attribute']}"

            grakn_query_cond, contains_array = find_cond_checker(attr)

            grakn_query += grakn_query_cond
            contain_statements.extend(contains_array)

        if concept_counter == concepts_len:

            # check contain statements

            for cond in contain_statements:
                grakn_query += f";{cond}"

            grakn_query += ";get;"
        else:
            grakn_query += " "

        concept_counter += 1

    logging.info("Here is the graql")
    logging.info(grakn_query)

    return grakn_query

    # match $Company isa Company, has name "Two Six Labs";get;
    # match $Company isa Company,has name "Two Six Labs"; get; offset 0; limit 30;
    # match $x isa Company,has name "Two Six Labs"; get; offset 0; limit 30;

    # match $Company isa Company, has name $name; { $name contains "Two";}; get; offset 0; limit 30;
    # match $Company isa Company, has name $name; { $name contains "Two";}, has profit > 300.0;get;


def query_grakn(session, query_object):

    logging.info(f"{query_object}")

    if query_object.action == "Find":
        find_query(session, query_object)


# rel1: "Company"
# rel1_name: "produces"
# rel2: "Sample"
# rel2_name: "produced"
# rel_name: "Productize"
# [{"rel1_name":"produces","rel1":"Company","rel1_value":"Company A"}]


def commit_relationship(row: pd.Series, session, rel_name: str, rel_map: dict):

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


def add_relationship_data(df: pd.DataFrame, rel_map: dict, rel_name: str, session):
    logging.info("Starting add relationships")

    # for each row in csv, add relationship
    df.apply(lambda row: commit_relationship(row, session, rel_name, rel_map), axis=1)


def add_relationship_to_entities(rel_map):
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


def create_entity_query(df: pd.DataFrame, entity_name: str, entity_key=None):
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


def sanitize_text(text: str):
    # will add more when issues come
    text = str(text).replace("/", "_").replace(".", "_dot_")
    return str(text)


def commit_entity(row: pd.Series, session, entity_name: str, entity_map: dict):

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
):
    logging.info("adding entities")
    # for each row in csv, add an entity
    df.apply(lambda row: commit_entity(row, session, entity_name, entity_map), axis=1)


def get_all_entities(session):

    with session.transaction().write() as transaction:
        entity_type = transaction.get_schema_concept("entity")
        all_entities_iter = entity_type.instances()
        someEntity = next(all_entities_iter)

        for ent in all_entities_iter:
            print(ent.type().label())


def load_entity_into_grakn(
    session, df: pd.DataFrame, entity_name: str, entity_key=None
):
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
):
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
        graql_insert_query = create_relationship_query(
            session, entity_map, rel_name, rel_map
        )
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
