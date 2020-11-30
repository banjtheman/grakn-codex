import json
import logging
from typing import Tuple
from dateutil.parser import parse

import pandas as pd
from grakn.client import GraknClient, ValueType

from pandas.api.types import is_string_dtype


logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


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

    elif val == ValueType.DOUBLE:
        return "double"

    elif val == ValueType.LONG:
        return "long"

    elif val == ValueType.BOOLEAN:
        return "bool"

    elif val == ValueType.DATETIME:
        return "date"

    else:
        raise TypeError(f"Invalid type {val}")


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

    elif val == "double":
        return ValueType.DOUBLE

    elif val == "long":
        return ValueType.LONG

    elif val == "bool":
        return ValueType.BOOLEAN

    elif val == "date":
        return ValueType.DATETIME

    else:
        raise TypeError(f"Invalid value {val}")


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

    if col == "desc":
        raise ValueError("desc is a reserved word sorry, rename your column")

    if col == "entity":
        raise ValueError("entity is a reserved word sorry, rename your column")

    if is_string_dtype(df[col]):

        # check if string is a date
        try:
            parse(df[col][0])
            logging.info(f"This is a date good {df[col][0]}")
            return ValueType.DATETIME
        except:
            return ValueType.STRING

    elif str(df[col].dtype) == "float64":
        return ValueType.DOUBLE

    elif str(df[col].dtype) == "int64":
        return ValueType.LONG

    elif str(df[col].dtype) == "bool":
        return ValueType.BOOLEAN

    else:
        raise TypeError(f"Could not figure out type for {col}")


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


# Example Rule
# define same_loc_not_nan sub relation, relates same_loc_not_nan_relationship_1, relates same_loc_not_nan_relationship_2;Tweet sub entity, plays same_loc_not_nan_relationship_1, plays same_loc_not_nan_relationship_2;Tweet sub entity, plays same_loc_not_nan_relationship_1, plays same_loc_not_nan_relationship_2;same_loc_not_nan-rule sub rule,when {$Tweet_A isa Tweet, has location $Tweet_location_A;{$Tweet_location_A != "nan";};{$Tweet_location_A != "";};$Tweet_B isa Tweet, has location $Tweet_location_B;{$Tweet_location_B != "nan";};{$Tweet_location_B != "";};$Tweet_A != $Tweet_B;},then {(same_loc_not_nan_relationship_1: $Tweet_A, same_loc_not_nan_relationship_2: $Tweet_B) isa same_loc_not_nan;};


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
            if cond_type == "equals":
                grakn_query += ' "' + cond_value + '"'

            # match $Company isa Company, has name $name; { $name contains "Two";}; get; offset 0; limit 30;
            if cond_type == "contains":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f'{{ ${attr_concept}_{curr_attr}{dif_1} contains "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

            if cond_type == "not equals":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f'not {{ ${attr_concept}_{curr_attr}{dif_1} == "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

            if cond_type == "not contains":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = f'not {{ ${attr_concept}_{curr_attr}{dif_1} contains "{cond_value}";}}'
                contain_statements.append(contain_string)

            if cond_type == "congruent":
                # {Company_name_X == Company_name_Y};
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                if dif_1 == "_B":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_A == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_Y":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_X == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_A":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_B == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_X":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_Y == ${attr_concept}_{curr_attr}{dif_1};}}"

                contain_statements.append(attr_string)
                # TODO: would we specify?
                # nan blocker
                if cond_value == "True":
                    logging.info("blocking nans")

                    nan_string1 = (
                        f'not {{ ${attr_concept}_{curr_attr}{dif_1} == "nan";}}'
                    )
                    # nan_string2 = f'not {{ ${attr_concept}_{curr_attr}{dif_1} == "";}}'

                    contain_statements.append(nan_string1)
                    # contain_statements.append(nan_string2)

        # if query_check_type == "double" or query_check_type == "long":
        elif query_check_type == "long" or query_check_type == "double":
            if cond_type == "equals":
                grakn_query += f" {cond_value}"
            if cond_type == "not equals":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f'not {{ ${attr_concept}_{curr_attr}{dif_1} == "{cond_value}";}}'
                )
                contain_statements.append(contain_string)
            if cond_type == "greater than":
                grakn_query += f" > {cond_value}"
            if cond_type == "less than":
                grakn_query += f" < {cond_value}"

            if cond_type == "congruent":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                if dif_1 == "_B":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_A == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_Y":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_X == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_A":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_B == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_X":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_Y == ${attr_concept}_{curr_attr}{dif_1};}}"

                contain_statements.append(attr_string)

        elif query_check_type == "bool":
            grakn_query += ' "' + cond_value + '"'

        elif query_check_type == "date":

            if cond_type == "between" or cond_type == " not between":
                conds = cond_value.split(" ")

                cond_value = make_dt_string(conds[0])
                cond_value2 = make_dt_string(conds[1])
            elif cond_type == "congruent":
                cond_value = ""
            else:
                cond_value = make_dt_string(cond_value)

            if cond_type == "on":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f"{{ ${attr_concept}_{curr_attr}{dif_1} == {cond_value};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "not on":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f"not {{ ${attr_concept}_{curr_attr}{dif_1} == {cond_value};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "after":

                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f"{{ ${attr_concept}_{curr_attr}{dif_1} > {cond_value};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "before":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f"{{ ${attr_concept}_{curr_attr}{dif_1} < {cond_value};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "between":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f"{{ ${attr_concept}_{curr_attr}{dif_1} > {cond_value};}}"
                )
                contain_statements.append(contain_string)
                contain_string = (
                    f"{{ ${attr_concept}_{curr_attr}{dif_1} < {cond_value2};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "not between":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                contain_string = (
                    f"not {{ ${attr_concept}_{curr_attr}{dif_1} > {cond_value};}}"
                )
                contain_statements.append(contain_string)
                contain_string = (
                    f"not {{ ${attr_concept}_{curr_attr}{dif_1} < {cond_value2};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "congruent":
                grakn_query += f" ${attr_concept}_{curr_attr}{dif_1}"
                if dif_1 == "_B":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_A == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_Y":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_X == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_A":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_B == ${attr_concept}_{curr_attr}{dif_1};}}"

                if dif_1 == "_X":
                    attr_string = f"{{ ${attr_concept}_{curr_attr}_Y == ${attr_concept}_{curr_attr}{dif_1};}}"

                contain_statements.append(attr_string)

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

    # logging.info("here comes...")
    # logging.info(attr)

    if not "selected_cond" in attr["cond"]:
        logging.info("empty cond check")
        return "", []
    cond_type = attr["cond"]["selected_cond"]
    cond_value = attr["cond"]["cond_value"]
    curr_attr = attr["attribute"]
    attr_concept = attr["attr_concept"]
    # logging.info("loaded all")

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
            if cond_type == "equals":
                grakn_query += ' "' + cond_value + '"'

            # match $Company isa Company, has name $name; { $name contains "Two";}; get; offset 0; limit 30;
            if cond_type == "contains":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f'{{ ${attr_concept}_{curr_attr} contains "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

            if cond_type == "not equals":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f'not {{ ${attr_concept}_{curr_attr} == "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

            if cond_type == "not contains":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f'not {{ ${attr_concept}_{curr_attr} contains "{cond_value}";}}'
                )
                contain_statements.append(contain_string)

            if cond_type == "congruent":
                raise ValueError(f"congruent is not supported for find")

        # if query_check_type == "double" or query_check_type == "long":
        elif query_check_type == "double" or query_check_type == "long":
            if cond_type == "equals":
                grakn_query += f" {cond_value}"
            if cond_type == "not equals":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f'not {{ ${attr_concept}_{curr_attr} == "{cond_value}";}}'
                )
                contain_statements.append(contain_string)
            if cond_type == "greater than":
                grakn_query += f" > {cond_value}"
            if cond_type == "less than":
                grakn_query += f" < {cond_value}"

            if cond_type == "congruent":
                raise ValueError(f"congruent is not supported for find")

        elif query_check_type == "bool":
            grakn_query += ' "' + cond_value + '"'

        elif query_check_type == "date":

            if cond_type == "between" or cond_type == "not between":
                conds = cond_value.split(" ")

                cond_value = make_dt_string(conds[0])
                cond_value2 = make_dt_string(conds[1])
            elif cond_type == "congruent":
                raise ValueError(f"congruent is not supported for find")
            else:
                cond_value = make_dt_string(cond_value)

            if cond_type == "on":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = f"{{ ${attr_concept}_{curr_attr} == {cond_value};}}"
                contain_statements.append(contain_string)

            if cond_type == "after":

                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = f"{{ ${attr_concept}_{curr_attr} > {cond_value};}}"
                contain_statements.append(contain_string)

            if cond_type == "before":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = f"{{ ${attr_concept}_{curr_attr} < {cond_value};}}"
                contain_statements.append(contain_string)

            if cond_type == "between":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = f"{{ ${attr_concept}_{curr_attr} > {cond_value};}}"
                contain_statements.append(contain_string)
                contain_string = f"{{ ${attr_concept}_{curr_attr} < {cond_value2};}}"
                contain_statements.append(contain_string)

            if cond_type == "not on":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = (
                    f"not {{ ${attr_concept}_{curr_attr} == {cond_value};}}"
                )
                contain_statements.append(contain_string)

            if cond_type == "not between":
                grakn_query += f" ${attr_concept}_{curr_attr}"
                contain_string = f"not {{ ${attr_concept}_{curr_attr} > {cond_value};}}"
                contain_statements.append(contain_string)
                contain_string = (
                    f"not {{ ${attr_concept}_{curr_attr} < {cond_value2};}}"
                )
                contain_statements.append(contain_string)

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
    compute_results["graql_queries"] = []

    for action in queries:

        if action not in compute_results:
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
                compute_results["graql_queries"].append(graql_query)

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
                compute_results["graql_queries"].append(graql_query)

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

        logging.info(attr)

        if "rel_ent" in attr:
            rel_query_string = f"; ${attr['rel_ent']}{dif_2} isa {attr['rel_ent']}"
            # rel_query_string += f", has {attr['attribute']}"

            # grakn_query_cond, contains_array = find_cond_checker_rule(attr, dif_2)
            # rel_query_string += grakn_query_cond
            # contain_statements.extend(contains_array)

            attr_loop_counter = 0
            attr_obj = None
            for attr_type in attr["attr_type"]:

                # hmm only if attr_type is not null
                if not attr_type is None:
                    rel_query_string += f", has {attr['attribute'][attr_loop_counter]}"

                # make a new attr object

                attr_obj = {}

                # logging.info(attr["cond"])
                attr_obj["attr_type"] = attr_type
                attr_obj["cond"] = attr["cond"][attr_loop_counter]
                attr_obj["attribute"] = attr["attribute"][attr_loop_counter]
                attr_obj["attr_concept"] = attr["rel_ent"]
                grakn_query_cond, contains_array = find_cond_checker_rule(
                    attr_obj, dif_2
                )

                rel_query_string += grakn_query_cond
                contain_statements.extend(contains_array)

                attr_loop_counter += 1

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

            # should be another loop?
            if "rel_conds" in attr:

                # logging.info("got here")
                # logging.info(attr["rel_conds"])

                if attr_obj is None:
                    attr_obj = {}

                for rel_cond in attr["rel_conds"]:

                    rel_query_string += f", has {rel_cond['attribute']}"
                    attr_obj["attr_type"] = rel_cond["attr_type"]
                    attr_obj["cond"] = {}
                    attr_obj["attribute"] = rel_cond["attribute"]
                    attr_obj["attr_concept"] = rel_cond["concept"]
                    attr_obj["cond"]["selected_cond"] = rel_cond["selected_cond"]
                    attr_obj["cond"]["cond_value"] = rel_cond["cond_value"]

                    grakn_query_cond, contains_array = find_cond_checker(attr_obj)
                    rel_query_string += grakn_query_cond
                    contain_statements.extend(contains_array)

            rel_statements.append(rel_query_string)

        else:
            grakn_query += f", has {attr['attribute']}"

            # grakn_query_cond, contains_array = find_cond_checker(attr)
            grakn_query_cond, contains_array = find_cond_checker_rule(attr, dif_1)

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

    graql_string += attr_make_rule_query(cond2, "_B", "_Y")

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
    rules_map["rule_string_ans"] = query_object.rule_string_ans

    return rules_map


def find_query(session, query_object: dict) -> Tuple[dict, list]:
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

        # logging.info(grakn_query)

        for attr in concept["attrs"]:

            if "rel_ent" in attr:
                rel_query_string = f"; ${attr['rel_ent']} isa {attr['rel_ent']}"

                # add rel ent to concepts
                if attr["rel_ent"] not in concepts:
                    concepts.append(attr["rel_ent"])

                if attr["rel_name"] not in concepts:
                    concepts.append(attr["rel_name"])

                attr_loop_counter = 0
                for attr_type in attr["attr_type"]:

                    # hmm only if attr_type is not null
                    if not attr_type is None:
                        rel_query_string += (
                            f", has {attr['attribute'][attr_loop_counter]}"
                        )

                    # make a new attr object

                    attr_obj = {}

                    # logging.info(attr["cond"])
                    attr_obj["attr_type"] = attr_type
                    attr_obj["cond"] = attr["cond"][attr_loop_counter]
                    attr_obj["attribute"] = attr["attribute"][attr_loop_counter]
                    attr_obj["attr_concept"] = attr["rel_ent"]
                    grakn_query_cond, contains_array = find_cond_checker(attr_obj)

                    rel_query_string += grakn_query_cond
                    contain_statements.extend(contains_array)

                    attr_loop_counter += 1

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

                # should be another loop?
                if "rel_conds" in attr:

                    # logging.info("got here")
                    # logging.info(attr["rel_conds"])

                    for rel_cond in attr["rel_conds"]:

                        rel_query_string += f", has {rel_cond['attribute']}"
                        attr_obj["attr_type"] = rel_cond["attr_type"]
                        attr_obj["cond"] = {}
                        attr_obj["attribute"] = rel_cond["attribute"]
                        attr_obj["attr_concept"] = rel_cond["concept"]
                        attr_obj["cond"]["selected_cond"] = rel_cond["selected_cond"]
                        attr_obj["cond"]["cond_value"] = rel_cond["cond_value"]

                        concepts.append(f"{attr['rel_name']}_{rel_cond['attribute']}")

                        grakn_query_cond, contains_array = find_cond_checker(attr_obj)
                        rel_query_string += grakn_query_cond
                        contain_statements.extend(contains_array)

                    # logging.info("passed here")

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
                # logging.info(grakn_query)
            else:
                grakn_query += " "

            attr_counter += 1

    logging.info("Here is the graql")
    for query in concept_queries:
        logging.info(query)

    # check for a do all query....
    if len(concept_queries) == 0:
        grakn_query = f"match ${concepts[0]} isa {concepts[0]}; get;"
        concept_queries.append(grakn_query)

    answers = run_find_query(session, concept_queries, concepts)
    return answers, concept_queries


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
    # logging.info("checking concept")

    ent_obj = {}
    attr_iterator = concept.attributes()

    for attr in attr_iterator:
        # logging.info(attr)
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
                # logging.info("checking rel_label")
                # print(rel_label)

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
                    # logging.info("AT a rel type")
                    attr_iterator = node.attributes()
                    concept = node_type.label()
                    rel_obj = {}
                    for attr in attr_iterator:
                        rel_obj[attr.type().label()] = attr.value()
                    codex_details = json.loads(rel_obj["codex_details"])

                    # logging.info("Codex Details:")
                    # logging.info(codex_details)

                    rel_obj[codex_details["rel1_role"]] = codex_details["rel1_value"]
                    rel_obj[codex_details["rel2_role"]] = codex_details["rel2_value"]

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

                # print("end of loop")
                # print(ent_map)
                # print(clusters)

    cluster_obj["ent_map"] = ent_map
    cluster_obj["clusters"] = clusters
    # logging.info("here is cluster obj")
    # logging.info(cluster_obj)

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

    with session.transaction().read() as read_transaction:
        answer_iterator = read_transaction.query(graql_query, explain=True)
        ent_map = {}
        answer_list = []
        answer_counter = 0

        for answer in answer_iterator:
            try:

                answer_concepts = list(answer.map().keys())

                # logging.info(answer_concepts)

                for key_con in answer_concepts:

                    key = f"{key_con}_{answer_counter}"

                    ent_map[key] = {}
                    ent_map[key]["concepts"] = []

                    ent_obj = {}
                    curr_ent = answer.map().get(key_con)
                    # logging.info(key)

                    cur_val = read_transaction.get_concept(curr_ent.id)

                    # then its a rule
                    if cur_val.is_inferred():
                        rels = cur_val.role_players()
                        for rel in rels:
                            ent_obj = {}
                            attr_iterator = rel.attributes()

                            for attr in attr_iterator:
                                # logging.info(attr.value())
                                ent_obj[attr.type().label()] = attr.value()

                            ent_map[key]["concepts"].append(ent_obj)

                        if answer.has_explanation():
                            explanation = answer.explanation()
                            explanation_map = {}
                            # logging.info(explanation.get_answers())

                            for exp_concept in explanation.get_answers():
                                # logging.info(exp_concept.map())

                                concept_keys = list(exp_concept.map().keys())

                                for concept_key in concept_keys:
                                    # logging.info(concept_key)

                                    curr_concept = exp_concept.map().get(concept_key)
                                    cur_val = read_transaction.get_concept(
                                        curr_concept.id
                                    )

                                    # check if attr
                                    explanation_map[concept_key] = {}

                                    if cur_val.is_attribute():
                                        explanation_map[concept_key][
                                            cur_val.type().label()
                                        ] = cur_val.value()

                                    else:
                                        attr_iterator = cur_val.attributes()
                                        for attr in attr_iterator:
                                            # logging.info(attr.value())
                                            explanation_map[concept_key][
                                                attr.type().label()
                                            ] = attr.value()

                            ent_map[key]["explanation"] = explanation_map
                            # logging.info("Here is explnation")
                            # logging.info(ent_map)
                            # logging.info("")
                            # answer_list.append(ent_map)

                            # logging.info("Here is list before")
                            # logging.info(answer_list)
                            # logging.info("")
                            if ent_map not in answer_list:
                                answer_list.append(ent_map)

                            # logging.info("Here is list after")
                            # logging.info(answer_list)
                            # logging.info("")

                            answer_counter += 1

                    else:

                        attr_iterator = cur_val.attributes()
                        concept_keys = list(answer.map().keys())
                        # logging.info(concept_keys)

                        for attr in attr_iterator:
                            ent_obj[attr.type().label()] = attr.value()

                        ent_map[key]["concepts"].append(ent_obj)

                        logging.error("Tdo we even come here?")

                        answer_list.append(ent_map)

                        answer_counter += 1

            except Exception as error:
                # logging.error("TAz dingo")
                logging.error(error)

    # logging.info("Here is everything")
    # logging.info(answer_list)
    return answer_list


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

    for concept in concepts:
        ent_map[concept] = []

    concept_counter = 0
    for query in queries:

        with session.transaction().read() as read_transaction:
            answer_iterator = read_transaction.query(query)
            for answer in answer_iterator:
                try:

                    answer_concepts = list(answer.map().keys())

                    for key in answer_concepts:

                        if key in concepts:

                            ent_obj = {}
                            curr_ent = answer.map().get(key)
                            # logging.info(curr_ent.id)

                            cur_val = read_transaction.get_concept(curr_ent.id)
                            # logging.info(cur_val)

                            attr_iterator = cur_val.attributes()
                            # come back here

                            if cur_val.is_attribute():
                                logging.info("Inside")
                                ent_obj = {}
                                node_val = cur_val.value()
                                node_label = cur_val.type().label()
                                ent_obj["key"] = node_label
                                ent_obj["value"] = node_val
                                logging.info(ent_obj)

                            else:

                                for attr in attr_iterator:
                                    ent_obj[attr.type().label()] = attr.value()

                            ent_map[key].append(ent_obj)

                except Exception as error:
                    logging.error(error)

        concept_counter += 1

    # logging.info(ent_map)
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
    answers = {}

    if query_object.action == "Find":
        answers, concept_queries = find_query(session, query_object)
        answers_df_map = {}

        for answer in answers:
            answers_df_map[answer] = turn_to_df(answers[answer])
        answers_df_map["graql_queries"] = concept_queries
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


def make_dt_string(val: str) -> str:
    """
    Purpose:
       Make a datetime string
    Args:
        val: string to make a datetime
    Returns:
        dt_string - date time string
    """
    try:
        dt = parse(val)
        # convert string to grakn format
        dt_string = f'{dt.strftime("%Y-%m-%dT%H:%M:%S")}.{str(dt.microsecond)[:3]}'
    except Exception as error:
        logging.error(error)
        raise ValueError(f"could not turn {val} to a date string")

    return dt_string


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
    if rel_map["rel1"]["key_type"] == "string" or rel_map["rel1"]["key_type"] == "bool":
        graql_insert_query += (
            ", has " + str(rel_map["rel1"]["key"]) + ' "' + str(row[rel1_role]) + '";'
        )

    if rel_map["rel1"]["key_type"] == "long" or rel_map["rel1"]["key_type"] == "double":
        graql_insert_query += (
            ", has " + str(rel_map["rel1"]["key"]) + " " + str(row[rel1_role]) + ";"
        )

    # TODO what is query key is a date?

    # rel 2
    graql_insert_query += (
        "$" + str(rel_map["rel2"]["role"]) + " isa " + str(rel_map["rel2"]["entity"])
    )

    # check key type
    if rel_map["rel2"]["key_type"] == "string" or rel_map["rel2"]["key_type"] == "bool":
        graql_insert_query += (
            ", has " + str(rel_map["rel2"]["key"]) + ' "' + str(row[rel2_role]) + '";'
        )

    if rel_map["rel2"]["key_type"] == "long" or rel_map["rel2"]["key_type"] == "double":
        graql_insert_query += (
            ", has " + str(rel_map["rel2"]["key"]) + " " + str(row[rel2_role]) + ";"
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
    # logging.info(row_attrs)
    row_attrs.remove("codex_details")
    # logging.info(row_attrs)

    attr_len = len(row_attrs)
    attr_counter = 1

    if attr_len > 0:
        graql_insert_query += ", "

        for attr in row_attrs:

            if (
                rel_map["cols"][attr]["type"] == "string"
                or rel_map["cols"][attr]["type"] == "bool"
            ):
                graql_insert_query += (
                    "has " + str(attr) + ' "' + str(sanitize_text(row[attr])) + '"'
                )
            elif (
                rel_map["cols"][attr]["type"] == "long"
                or rel_map["cols"][attr]["type"] == "double"
            ):
                graql_insert_query += "has " + str(attr) + " " + str(row[attr])

            # This is a date
            else:
                dt = parse(row[attr])
                # convert string to grakn format
                dt_string = (
                    f'{dt.strftime("%Y-%m-%dT%H:%M:%S")}.{str(dt.microsecond)[:3]}'
                )
                # dt_string = dt.strftime("%Y-%m-%dT%H:%M:%S")
                graql_insert_query += "has " + str(attr) + " " + dt_string

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
    # logging.info("Starting add relationships")

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

        else:
            graql_insert_query += "has " + str(attr)

        # logging.info(graql_insert_query)

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
    # text = str(text).replace("/", "_").replace(".", "_dot_")
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

        if (
            current_ent["cols"][col]["type"] == "string"
            or current_ent["cols"][col]["type"] == "bool"
        ):
            graql_insert_query += (
                "has " + str(col) + ' "' + str(sanitize_text(row[col])) + '"'
            )
        elif (
            current_ent["cols"][col]["type"] == "long"
            or current_ent["cols"][col]["type"] == "double"
        ):
            graql_insert_query += "has " + str(col) + " " + str(row[col])

        # This is a date
        else:
            dt = parse(row[col])
            # convert string to grakn format
            dt_string = f'{dt.strftime("%Y-%m-%dT%H:%M:%S")}.{str(dt.microsecond)[:3]}'
            # dt_string = dt.strftime("%Y-%m-%dT%H:%M:%S")
            # logging.info(f"adding dt{dt_string}")
            graql_insert_query += "has " + str(col) + " " + dt_string

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
    # logging.info("adding entities")
    # for each row in csv, add an entity
    df.apply(lambda row: commit_entity(row, session, entity_name, entity_map), axis=1)


def get_all_rels(session, entity_map):
    """
    Purpose:
       Get relationship map
    Args:
        session - Current grakn session
        entity_map - the entity map
    Returns:
        rel_map - the rel map
    """
    # Example rel map
    # {'Productize': {'rel1': {'role': 'produced', 'entity': 'Product', 'key': 'name', 'key_type': 'string'}, 'rel2': {'role': 'producer', 'entity': 'Company', 'key': 'name', 'key_type': 'string'}, 'cols': {'codex_details': {'type': 'string'}, 'note': {'type': 'string'}}}}

    with session.transaction().write() as transaction:
        entity_type = transaction.get_schema_concept("relation")
        subs = list(entity_type.subs())
        # logging.info(subs)
        subs_labels = [sub.label() for sub in subs]
        logging.info(subs_labels)
        subs_labels.pop(0)
        rel_map = {}

        for sub_label in subs_labels:
            # setup ent map
            logging.info(f"##################{sub_label}##################")
            rel_map[sub_label] = {}
            rel_map[sub_label]["cols"] = {}
            rel_map[sub_label]["rel1"] = {}
            rel_map[sub_label]["rel2"] = {}

            curr_sub = transaction.get_schema_concept(sub_label)

            current_attrs = curr_sub.attributes()

            labels = []
            for attr in current_attrs:

                label_obj = {}
                label_obj["type"] = turn_value_type(attr.value_type())

                label_obj["label"] = attr.label()
                labels.append(label_obj)

            for label in labels:
                rel_map[sub_label]["cols"][label["label"]] = {}
                rel_map[sub_label]["cols"][label["label"]]["type"] = label["type"]

            roles = curr_sub.roles()

            counter = 0
            for role in roles:

                players = role.players()

                with_ent = ""
                for player in players:
                    with_ent = player.label()

                # {'role': 'produced', 'entity': 'Product', 'key': 'name', 'key_type': 'string'}
                ent_key = entity_map[with_ent]["key"]
                ent_key_type = entity_map[with_ent]["cols"][ent_key]["type"]

                if counter == 0:
                    rel_map[sub_label]["rel1"]["role"] = role.label()
                    rel_map[sub_label]["rel1"]["entity"] = with_ent
                    rel_map[sub_label]["rel1"]["key"] = ent_key
                    rel_map[sub_label]["rel1"]["key_type"] = ent_key_type
                else:
                    rel_map[sub_label]["rel2"]["role"] = role.label()
                    rel_map[sub_label]["rel2"]["entity"] = with_ent
                    rel_map[sub_label]["rel2"]["key"] = ent_key
                    rel_map[sub_label]["rel2"]["key_type"] = ent_key_type

                counter += 1

        logging.info(rel_map)
        return rel_map


# TODO do we need this function?
def get_all_entities(session):

    with session.transaction().write() as transaction:
        entity_type = transaction.get_schema_concept("entity")
        subs = list(entity_type.subs())
        subs_labels = [sub.label() for sub in subs]
        logging.info(subs_labels)
        subs_labels.pop(0)

        # Example ent map
        # : {'Company': {'key': 'name', 'cols': {'name': {'type': 'string'}, 'budget': {'type': 'double'}}, 'rels': {'Productize': {'plays': 'producer', 'with_ent': 'Product'}}}, 'Product': {'key': 'name', 'cols': {'name': {'type': 'string'}, 'product_type': {'type': 'string'}}, 'rels': {'Productize': {'plays': 'produced', 'with_ent': 'Company'}}}}

        # Making ent_map right here
        ent_map = {}
        for sub_label in subs_labels:
            # setup ent map
            logging.info(f"##################{sub_label}##################")
            ent_map[sub_label] = {}
            ent_map[sub_label]["cols"] = {}
            ent_map[sub_label]["rels"] = {}

            curr_sub = transaction.get_schema_concept(sub_label)
            current_attrs = curr_sub.attributes()

            labels = []
            for attr in current_attrs:

                label_obj = {}
                label_obj["type"] = turn_value_type(attr.value_type())

                label_obj["label"] = attr.label()
                labels.append(label_obj)

            keys = [key.label() for key in curr_sub.keys()]

            # Only one key?
            ent_map[sub_label]["key"] = keys[0]

            # logging.info(keys)
            # labels = [attr.label() for attr in current_attrs]
            # logging.info(labels)

            for label in labels:
                ent_map[sub_label]["cols"][label["label"]] = {}
                ent_map[sub_label]["cols"][label["label"]]["type"] = label["type"]

            roles = curr_sub.playing()
            for role in roles:

                rels = role.relations()
                players = role.players()

                with_ent = ""

                for player in players:

                    if player.label() != sub_label:
                        with_ent = player.label()

                for rel in rels:

                    ent_map[sub_label]["rels"][rel.label()] = {}
                    ent_map[sub_label]["rels"][rel.label()]["plays"] = role.label()
                    ent_map[sub_label]["rels"][rel.label()]["with_ent"] = with_ent

        logging.info(ent_map)
        return ent_map


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
            # logging.info(col)
            entity_map[col] = {}
            current_type = check_types(df, col)
            entity_map[col]["type"] = turn_value_type(current_type)

            transaction.put_attribute_type(col, current_type)
            transaction.commit()

    # make entity
    with session.transaction().write() as transaction:
        graql_insert_query = create_entity_query(df, entity_name, entity_key)
        logging.info("Executing Graql Query: " + graql_insert_query)
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
            # logging.info(col)
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
