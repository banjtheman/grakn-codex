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


def entity_query(df: pd.DataFrame, entity_name: str, entity_key=None):
    graql_insert_query = "define " + entity_name + " sub entity"

    # check if attrs
    attr_length = len(df.columns)
    if attr_length == 0:
        graql_insert_query += ";"
        return graql_insert_query

    graql_insert_query += ","
    attr_counter = 1

    for attr in df.columns:

        # check if attr is key
        if entity_key is not None:
            if str(attr) == entity_key:
                graql_insert_query += "key " + str(attr)
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

        if current_ent["cols"][col]["type"] == ValueType.STRING:
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
            entity_map[col]["type"] = current_type

            transaction.put_attribute_type(col, current_type)
            transaction.commit()

    # make entity
    with session.transaction().write() as transaction:
        graql_insert_query = entity_query(df, entity_name, entity_key)
        print("Executing Graql Query: " + graql_insert_query)
        transaction.query(graql_insert_query)
        transaction.commit()

    return entity_map
