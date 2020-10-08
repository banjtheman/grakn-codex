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


def check_types(df, col):

    if is_string_dtype(df[col]):
        return ValueType.STRING

    if str(df[col].dtype) == "float64":
        return ValueType.FLOAT

    if str(df[col].dtype) == "int64":
        return ValueType.LONG

    if str(df[col].dtype) == "bool":
        return ValueType.BOOLEAN

    # TODO figure out how to get dates
    return ValueType.DATETIME


def entity_query(df, entity_name, entity_key = None):
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


def load_entity_into_grakn(session, df, entity_name, entity_key = None):
    # sample entity
    # {'proj_name': 'codex_test', 'ent_name': 'test', 'desc': 'test entity', 'attrs': [{'attr_name': 'name', 'attr_type': 'string'}]}

    # make attrs
    for col in df.columns:
        with session.transaction().write() as transaction:
            logging.info(col)
            current_type = check_types(df, col)

            transaction.put_attribute_type(col, current_type)
            transaction.commit()

    # make entity
    with session.transaction().write() as transaction:
        graql_insert_query = entity_query(df, entity_name, entity_key)
        print("Executing Graql Query: " + graql_insert_query)
        transaction.query(graql_insert_query)
        transaction.commit()
