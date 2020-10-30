import json
import re
import uuid
import logging

# import inflect
import pandas as pd

# from .codex_kg import CodexKg
from .codex_query import (
    CodexQueryFind,
    CodexQueryCompute,
    CodexQueryCluster,
    CodexQueryRule,
)

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
    """
    Purpose:
        Get plural form of a string
    Args:
        noun - string to get plural for
    Returns:
        noun: plural form of noun
    """
    for findpattern, rule in rules:
        if findpattern(noun):
            return rule(noun)


def get_rel_name_from_ents(codexkg, rel1: str, rel2: str) -> str:
    """
    Purpose:
        Get the rel name from entity
    Args:
        codexkg - Codex KG object
        rel1: first relationship
        rel2: second relationship
    Returns:
        rel_name: Relation name
    """
    rels = list(codexkg.rel_map.keys())
    rel_name = ""

    for rel in rels:

        check1 = codexkg.rel_map[rel]["rel1"]["entity"]
        check2 = codexkg.rel_map[rel]["rel2"]["entity"]

        if rel1 == check1 or rel1 == check2:
            if rel2 == check1 or rel2 == check2:
                rel_name = rel

    return rel_name


def cond_json_maker(concept_cond: str, concept_value) -> dict:
    """
    Purpose:
        Get the condtions json object
    Args:
        concept_cond - concept condition
        concept_value - concept value

    Returns:
        cond_json: condition json object
    """
    cond_json = {}

    # cond_value = f"REPLACE_{concept}_{attr_name}"
    cond_string = f" that {concept_cond} {concept_value}"

    cond_json["selected_cond"] = concept_cond
    cond_json["cond_value"] = concept_value
    cond_json["cond_string"] = cond_string

    return cond_json


def cond_setter(
    attr_type: str, attr_name: str, concept: str, concept_cond, concept_value
) -> list:
    """
    Purpose:
        Get the condtions object
    Args:
        attr_type: attribute type
        attr_name: attribute name
        concept: concept
        concept_cond - concept condition
        concept_value - concept value

    Returns:
        cond_array: condition query object
    """
    cond_json = {}

    if attr_type == "string":

        conds = ["equals", "contains", "not equals", "not contains", "congruent"]

        if concept_cond in conds:

            cond_json = cond_json_maker(concept_cond, concept_value)

        else:
            raise ValueError(
                f"For string values must be equals or contains not {concept_cond}"
            )

    elif attr_type == "long" or attr_type == "double":
        conds = ["equals", "less than", "greater than", "not equals", "congruent"]

        if concept_cond in conds:
            cond_json = cond_json_maker(concept_cond, concept_value)
        else:
            raise ValueError(f"For int values {concept_cond} is not in {conds}")

    elif attr_type == "bool":
        conds = ["True", "False"]

        if concept_cond in conds:
            cond_json = cond_json_maker(concept_cond, concept_value)
        else:
            raise ValueError(f"For bool values {concept_cond} is not in {conds}")

    elif attr_type == "date":
        conds = [
            "on",
            "after",
            "before",
            "between",
            "not on",
            "not between",
            "congruent",
        ]

        if concept_cond in conds:
            cond_json = cond_json_maker(concept_cond, concept_value)
        else:
            raise ValueError(f"For bool values {concept_cond} is not in {conds}")

    else:
        logging.error(f"error invalid type attr_type: {attr_type}")
        raise TypeError("Undefined Type")

    return cond_json


def cond_array_maker(
    codexkg, concept, is_ent, rel_attrs, rel_conds, rel_values, attr_string
):
    """
    Purpose:
        Get the condtions array
    Args:
        concept: Concept to find
        is_ent - Is entity or not
        rel_attrs - relation attributes
        rel_conds - relation conditions
        rel_values - values of the conditions
        attr_string - attribute string

    Returns:
        cond_array: condition query object
    """
    cond_array = []
    selected_attr = []
    attr_type = []
    rel_attr_counter = 0

    for rel_attr in rel_attrs:

        selected_attr.append(rel_attr)

        concept_cond = rel_conds[rel_attr_counter]
        concept_value = rel_values[rel_attr_counter]

        # logging.info(concept)
        # logging.info(rel_attr)

        if is_ent:
            curr_type = codexkg.entity_map[concept]["cols"][rel_attr]["type"]
        else:
            curr_type = codexkg.rel_map[concept]["cols"][rel_attr]["type"]

        attr_type.append(curr_type)

        attr_string += f" that have a {rel_attr}"

        cond_json = cond_setter(
            curr_type,
            rel_attr,
            concept,
            concept_cond,
            concept_value,
        )

        cond_json["attr_string"] = attr_string
        cond_json["attr_type"] = curr_type
        cond_json["attribute"] = rel_attr
        cond_json["concept"] = concept

        cond_array.append(cond_json)

        rel_attr_counter += 1

    # logging.info("Here is cond array")
    # logging.info(cond_array)

    return cond_array


def attr_setter(
    codexkg,
    concept: str,
    is_ent: bool,
    rule_num: int,
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
) -> list:
    """
    Purpose:
        Get the attributes query object
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
        attr_obj_list: attributes query object
    """
    # Get Values from entites

    rel_action_counter = 0
    concept_attr_counter = 0
    plays_map = {}
    if is_ent:
        attrs = codexkg.entity_map[concept]["cols"]
        attr_list = list(attrs.keys())

        rel_names = codexkg.entity_map[concept]["rels"].keys()

        rel_attrs = []
        for rel in rel_names:
            plays = codexkg.entity_map[concept]["rels"][rel]["plays"]
            with_ent = codexkg.entity_map[concept]["rels"][rel]["with_ent"]
            rel_attrs.append(plays)

            if plays in plays_map:
                plays_map[plays].append(with_ent)
            else:
                plays_map[plays] = [with_ent]

    # Get values for relationships
    else:
        attrs = codexkg.rel_map[concept]["cols"]
        attr_list = list(attrs.keys())
        attr_list.remove("codex_details")

    # this is the attribute for the entity i.e name
    attr_obj_list = []

    selected_attrs = concept_attrs + rel_actions

    for selected_attr in selected_attrs:

        attr_json = {}

        if selected_attr in attr_list:
            attr_string = " that have a " + selected_attr

            if is_ent:
                attr_type = codexkg.entity_map[concept]["cols"][selected_attr]["type"]
            else:
                attr_type = codexkg.rel_map[concept]["cols"][selected_attr]["type"]

            # need value and condition
            concept_cond = concept_conds[concept_attr_counter]
            concept_value = concept_values[concept_attr_counter]

            cond_json = cond_setter(
                attr_type, selected_attr, concept, concept_cond, concept_value
            )
            attr_json["attr_concept"] = concept

            concept_attr_counter += 1

        else:
            attr_string = " that " + selected_attr

            if len(rel_actions) > 0:

                selected_ent2 = concept_rels[rel_action_counter]

                rel_attrs = concept_rel_attrs[rel_action_counter]
                rel_conds = concept_rel_conds[rel_action_counter]
                rel_values = concept_rel_values[rel_action_counter]

                # logging.info(rel_conds)
                # logging.info(rel_values)
                # logging.info(selected_ent2)

                attr_json["rel_ent"] = selected_ent2
                attr_json["rel_attr"] = selected_attr

                rel_name = get_rel_name_from_ents(codexkg, concept, selected_ent2)
                attr_json["rel_name"] = rel_name

                # logging.info(rel_name)

                attr_json["rel_other"] = codexkg.entity_map[selected_ent2]["rels"][
                    rel_name
                ]["plays"]

                attr_json["attr_concept"] = selected_ent2
                attr_string += " " + selected_ent2

                # basicaly do a loop for all rel_conds and rel_values

                # TODO Come fix
                # selected_attr = st.selectbox(f"Select Attribute {rule_num}", attr_list2)

                rel_attr_counter = 0
                cond_json = []

                selected_attr = []
                attr_type = []

                # TODO why cant this be a function?

                for rel_attr in rel_attrs:

                    selected_attr.append(rel_attr)

                    concept_cond = rel_conds[rel_attr_counter]
                    concept_value = rel_values[rel_attr_counter]

                    curr_type = codexkg.entity_map[selected_ent2]["cols"][rel_attr][
                        "type"
                    ]

                    attr_type.append(curr_type)

                    attr_string += " that have a " + rel_attr

                    cond_json.append(
                        cond_setter(
                            curr_type,
                            rel_attr,
                            selected_ent2,
                            concept_cond,
                            concept_value,
                        )
                    )

                    rel_attr_counter += 1

            else:
                attr_json["rel_ent"] = selected_ent2
                attr_json["rel_attr"] = selected_attr
                rel_name = get_rel_name_from_ents(codexkg, concept, selected_ent2)
                attr_json["rel_name"] = rel_name
                attr_json["rel_other"] = codexkg.entity_map[selected_ent2]["rels"][
                    rel_name
                ]["plays"]

                attr_json["attr_concept"] = selected_ent2
                attr_string += " " + selected_ent2
                # attrs2 = codexkg.entity_map[selected_ent2]["cols"]
                attr_string + " that have a " + selected_ent2 + " relationship that "
                cond_json = {}

                attr_type = None

            if len(with_rel_attrs) > 0:

                attr_json["rel_conds"] = cond_array_maker(
                    codexkg,
                    rel_name,
                    False,
                    with_rel_attrs[rel_action_counter],
                    with_rel_conds[rel_action_counter],
                    with_rel_values[rel_action_counter],
                    attr_string,
                )

            rel_action_counter += 1

        attr_json["cond"] = cond_json
        attr_json["attr_type"] = attr_type
        attr_json["attribute"] = selected_attr
        attr_json["attr_string"] = attr_string

        attr_obj_list.append(attr_json)

    # logging.info("Final attrs")
    # print(attr_obj_list)

    return attr_obj_list


def query_string_find_maker(concept: str, attr_obj_list: dict) -> str:
    """
    Purpose:
        Create a query string
    Args:
        concept: the concept
        attr_obj_list: concept attributes
    Returns:
        query_string: The query string
    """
    query_string = f"Find {plural(concept)}"

    attr_len = len(attr_obj_list)
    attr_counter = 1

    for attr in attr_obj_list:

        try:

            query_string += f"{attr['attr_string']}{attr['cond']['cond_string']}"
            logging.info(query_string)
        except:
            try:
                query_string += f"{attr['attr_string']}{attr['cond'][0]['cond_string']}"
            except:
                query_string += f"{attr['attr_string']}"

        if "rel_conds" in attr:

            try:
                query_string += (
                    f" and have a {attr['rel_conds'][0]['concept']} relation with "
                )
            except:
                query_string += ""

            cond_counter = 0
            cond_len = len(attr["rel_conds"])

            for cond in attr["rel_conds"]:
                logging.info(cond)

                query_string += f" {cond['attribute']} {cond['cond_string']}"
                cond_counter += 1

                if not cond_counter == cond_len:
                    query_string += " and "

        if not attr_counter == attr_len:
            query_string += " and "

        attr_counter += 1

    query_string += ". "

    return query_string


def make_rule_string(rule_obj):
    """
    Purpose:
        Create Rule string
    Args:
        rule_obj: Rule object
    Returns:
        rule_string: The rule string
        rule_string_ans: The rule string as an answer
    """
    rule_string = ""
    rule_string_ans = ""

    # check cond1
    rule_name = rule_obj["name"]

    cond1 = rule_obj["cond1"]
    cond2 = rule_obj["cond2"]

    rule_string += f"If {cond1['concept']} A "
    rule_string_ans += f"{cond1['concept']}_A "

    attr_len = len(cond1["attrs"])
    attr_counter = 1
    for attr in cond1["attrs"]:

        if "rel_attr" in attr:
            logging.info(attr)
            rule_string += f"{attr['rel_attr']} {attr['rel_ent']} Y "

            rule_string_ans += (
                f"{attr['rel_attr']} {attr['rel_ent']}_{attr['attribute']}_Y "
            )

            attrib_counter = 0
            num_attrs = len(attr["attribute"])
            for attrib in attr["attribute"]:

                rule_string += (
                    f"that has a {attrib} {attr['cond'][attrib_counter]['cond_string']}"
                )

                rule_string_ans += (
                    f"that has a {attrib} {attr['cond'][attrib_counter]['cond_string']}"
                )

                attrib_counter += 1

                if attrib_counter != num_attrs:
                    rule_string += " and "

            if "rel_conds" in attr:

                try:
                    rule_string += (
                        f" and have a {attr['rel_conds'][0]['concept']} relation with "
                    )

                    rule_string_ans += (
                        f" and have a {attr['rel_conds'][0]['concept']} relation with"
                    )
                except:
                    rule_string += ""

                cond_counter = 0
                cond_len = len(attr["rel_conds"])

                for cond in attr["rel_conds"]:
                    logging.info(cond)

                    rule_string += f" {cond['attribute']} {cond['cond_string']}"
                    rule_string_ans += f" {cond['attribute']} {cond['cond_string']}"
                    cond_counter += 1

                    if not cond_counter == cond_len:
                        rule_string += " and "

        else:
            rule_string += f" has a {attr['attribute']} {attr['cond']['cond_string']}"
            rule_string_ans += (
                f" has a {attr['attribute']} {attr['cond']['cond_string']}"
            )

        if attr_counter == attr_len:
            rule_string += "."
            rule_string_ans += "."

        else:
            rule_string += " and "
            rule_string_ans += " and "

        attr_counter += 1

    # check cond2
    attr_len = len(cond2["attrs"])
    attr_counter = 1
    rule_string += f" If {cond2['concept']} B "
    rule_string_ans += f"{cond2['concept']}_B "
    for attr in cond2["attrs"]:

        if "rel_attr" in attr:
            logging.info(attr)
            rule_string += f"{attr['rel_attr']} {attr['rel_ent']} Y "

            rule_string_ans += (
                f"{attr['rel_attr']} {attr['rel_ent']}_{attr['attribute']}_Y "
            )

            attrib_counter = 0
            num_attrs = len(attr["attribute"])
            for attrib in attr["attribute"]:

                rule_string += (
                    f"that has a {attrib} {attr['cond'][attrib_counter]['cond_string']}"
                )

                rule_string_ans += (
                    f"that has a {attrib} {attr['cond'][attrib_counter]['cond_string']}"
                )

                attrib_counter += 1

                if attrib_counter != num_attrs:
                    rule_string += " and "

            if "rel_conds" in attr:

                try:
                    rule_string += (
                        f" and have a {attr['rel_conds'][0]['concept']} relation with "
                    )

                    rule_string_ans += (
                        f" and have a {attr['rel_conds'][0]['concept']} relation with"
                    )
                except:
                    rule_string += ""

                cond_counter = 0
                cond_len = len(attr["rel_conds"])

                for cond in attr["rel_conds"]:
                    logging.info(cond)

                    rule_string += f" {cond['attribute']} {cond['cond_string']}"
                    rule_string_ans += f" {cond['attribute']} {cond['cond_string']}"
                    cond_counter += 1

                    if not cond_counter == cond_len:
                        rule_string += " and "

        else:
            rule_string += f" has a {attr['attribute']} {attr['cond']['cond_string']}"
            rule_string_ans += (
                f" has a {attr['attribute']} {attr['cond']['cond_string']}"
            )

        if attr_counter == attr_len:
            rule_string += ". "
            rule_string_ans += ". "
        else:
            rule_string += " and "
            rule_string_ans += " and "

        attr_counter += 1

    rule_string += (
        f"Then  {cond1['concept']} A and {cond2['concept']} B are {rule_name}"
    )

    return rule_string, rule_string_ans


# TODO make this stand alone?
def make_rule_cond(
    codexkg,
    concept: str,
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
):
    """
    Purpose:
        Create Rule Condition
    Args:
        concept: Concept to find
        concept_attrs: Concept attributes
        concept_conds: condition for attribute
        concept_values: value for condition
        rel_actions: Find releation attrubite
        concept_rels: Relation to search for
        concept_rel_attrs: attributes for relation attribute
        concept_rel_conds: conditions for relation attribute
        concept_rel_values: values for relation attribute,
        with_rel_attrs: attributes of the relationship ,
        with_rel_conds: conditions of the relationship ,
        with_rel_values: values of the relationship ,
    Returns:
        answers: find rule query object
    """
    ents = list(codexkg.entity_map.keys())

    if concept in ents:
        is_ent = True
        concept_type = "Entity"
    else:
        is_ent = False
        concept_type = "Relationship"

    attr_obj_list = attr_setter(
        codexkg,
        concept,
        is_ent,
        1,
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

    concept_json = {}
    concept_json["concept"] = concept
    concept_json["concept_type"] = concept_type
    concept_json["attrs"] = attr_obj_list
    concept_json["query_string"] = query_string_find_maker(concept, attr_obj_list)

    return concept_json


def find_action(
    codexkg,
    concept: str,
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
        concept_rels: Relation to search for
        concept_rel_attrs: attributes for relation attribute
        concept_rel_conds: conditions for relation attribute
        concept_rel_values: values for relation attribute,
        with_rel_attrs: attributes of the relationship ,
        with_rel_conds: conditions of the relationship ,
        with_rel_values: values of the relationship ,
    Returns:
        answers: find query object
    """
    ents = list(codexkg.entity_map.keys())
    # rels = list(codexkg.rel_map.keys())

    if concept in ents:
        is_ent = True
        concept_type = "Entity"
    else:
        is_ent = False
        concept_type = "Relationship"

    codex_query_list = []

    attr_obj_list = attr_setter(
        codexkg,
        concept,
        is_ent,
        1,
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

    concept_json = {}
    concept_json["concept"] = concept
    concept_json["concept_type"] = concept_type
    concept_json["attrs"] = attr_obj_list
    concept_json["query_string"] = query_string_find_maker(concept, attr_obj_list)
    codex_query_list.append(concept_json)

    try:
        query_text = ""
        for concept_obj in codex_query_list:
            query_text += concept_obj["query_string"]

    except:
        query_text = "Enter Query:"

    # make a codex_query object here
    curr_query = CodexQueryFind(concepts=codex_query_list, query_string=query_text)

    return curr_query


def compute_action(
    codexkg,
    actions: list,
    concepts: list,
    concept_attrs: list,
):
    """
    Purpose:
        Do a compute query
    Args:
        actions - compute actions
        concepts - list of concepts to compute on
        concept_attrs: concepts attributes to compute
    Returns:
        compute_obj: compute query object
    """
    compute_obj = {}

    actions_list = [
        "Count",
        "Sum",
        "Maximum",
        "Minimum",
        "Mean",
        "Median",
        "Standard Deviation",
    ]

    # check if valid actions..

    for action in actions:
        if action not in actions_list:
            raise ValueError(
                f"{action} is not a valid action. Choose from {actions_list}"
            )

    query_text_list = []

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())

    ents_rels = ents + rels

    for counter, action in enumerate(actions):

        if action not in compute_obj:
            compute_obj[action] = []

        concept = concepts[counter]
        concept_attr = concept_attrs[counter]

        if concept not in ents_rels:
            if concept == "All Concepts":
                pass
            else:
                raise ValueError(
                    f"Invalid concept: {concept}, must be from {ents_rels}"
                )

        if action == "Count":
            query_text = f"Compute {action} for {concept}"
            query_text_list.append(query_text)

            count_obj = {}
            count_obj["concept"] = concept
            count_obj["query_text"] = query_text
            compute_obj[action].append(count_obj)

        else:

            if concept in ents:
                attrs = codexkg.entity_map[concept]["cols"]
                attr_list = list(codexkg.entity_map[concept]["cols"])
            else:
                attrs = codexkg.rel_map[concept]["cols"]
                attr_list = list(codexkg.rel_map[concept]["cols"])

            # check if attr is valid
            if not concept_attr in attr_list:
                raise ValueError(
                    f"{concept_attr} is not a valid attrubite for {concept}, select from {attr_list}"
                )

            # check if attr type is valid
            if not (
                attrs[concept_attr]["type"] == "double"
                or attrs[concept_attr]["type"] == "long"
            ):
                raise TypeError(
                    f"{concept_attr} is not a double or a long, can not compute..."
                )

            query_text = f"Compute {action} for {concept_attr} in {concept}"

            action_obj = {}
            action_obj["concept"] = concept
            action_obj["attr"] = concept_attr
            action_obj["query_text"] = query_text

            compute_obj[action].append(action_obj)
            query_text_list.append(query_text)

    curr_query = CodexQueryCompute(queries=compute_obj, query_text_list=query_text_list)
    logging.info(curr_query)

    return curr_query


def concept_string(concepts: list) -> str:
    """
    Purpose:
        make grakn query string
    Args:
        concepts: concepts for the graql string
    Returns:
        concept_string - graql string
    """
    concept_string = "["
    concept_len = len(concepts)
    concept_counter = 1
    for concept in concepts:

        if concept_counter == concept_len:
            concept_string += concept + "]"
        else:
            concept_string += concept + ", "
        concept_counter += 1

    return concept_string


def compute_centrality(
    codexkg,
    action: str,
    choice: str,
    cluster_concepts: list,
    given_type: str,
    k_min: int,
):
    """
    Purpose:
        computer centrality in grakn
    Args:
        codexkg: codex kg
        action: str: cluster specific action
        cluster_type: type of cluster,
        cluster_concepts: list of concepts,
        given_type: concept to hone in on,
        k_min: how many k groups,
    Returns:
        codex_query - Cluster object
    """
    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())
    ents_rels = ents + rels

    cluster_obj = {}

    if action == "degree":

        if choice == "All":
            query_string = "compute centrality using degree;"

            cluster_obj["query_string"] = query_string
            cluster_obj["query_type"] = "centrality"
            cluster_obj["choice"] = "All Concepts"
            cluster_obj["concepts"] = ents_rels

        elif choice == "Subgraph":

            cluster_obj["query_type"] = "centrality"
            cluster_obj["choice"] = "subgraph"
            cluster_obj["concepts"] = cluster_concepts

            query_string = f"compute centrality in {concept_string(cluster_concepts)}, using degree;"

            cluster_obj["query_string"] = query_string

        elif choice == "Given type":

            cluster_obj["query_type"] = "centrality"
            cluster_obj["choice"] = "subgraph"
            cluster_obj["concepts"] = cluster_concepts
            cluster_obj["given_type"] = given_type

            query_string = f"compute centrality of {given_type}, in {concept_string(cluster_concepts)}, using degree;"
            cluster_obj["query_string"] = query_string
        else:
            raise TypeError(
                f"Unknown choice{choice} must be All,Subgraph, or Given type"
            )

    if action == "k-core":

        cluster_obj["query_type"] = "centrality"
        cluster_obj["choice"] = "k-core"
        cluster_obj["concepts"] = ents_rels
        query_string = f"compute centrality using k-core;"
        cluster_concepts = ents_rels

        if k_min is not None:

            if k_min < 2:
                raise ValueError(f"k_min: {k_min} is less than 2")

            query_string = f"compute centrality using k-core, where min-k={k_min};"

        cluster_obj["query_string"] = query_string

    curr_query = CodexQueryCluster(query=cluster_obj)

    return curr_query


def compute_cluster(
    codexkg,
    action: str,
    choice: str,
    cluster_concepts: list,
    given_type: str,
    k_min: int,
):
    """
    Purpose:
        computer cluser in grakn
    Args:
        codexkg: codex kg
        action: str: cluster specific action
        cluster_type: type of cluster,
        cluster_concepts: list of concepts,
        given_type: concept to hone in on,
        k_min: how many k groups,
    Returns:
        codex_query - Cluster object
    """
    cluster_obj = {}

    if action == "connected":

        cluster_obj["query_type"] = "cluster"
        cluster_obj["choice"] = "cluster-in"
        cluster_obj["concepts"] = cluster_concepts

        query_string = f"compute cluster in {concept_string(cluster_concepts)}, using connected-component;"

        cluster_obj["query_string"] = query_string

    elif action == "k-core":

        cluster_obj["query_type"] = "cluster"
        cluster_obj["choice"] = "k-core"
        cluster_obj["concepts"] = cluster_concepts
        query_string = (
            f"compute cluster in {concept_string(cluster_concepts)}, using k-core;"
        )

        if k_min is not None:

            if k_min < 2:
                raise ValueError(f"k_min: {k_min} is less than 2")

            query_string = f"compute cluster in {concept_string(cluster_concepts)}, using k-core,where k={k_min};"

        cluster_obj["query_string"] = query_string

    else:
        raise TypeError(f"Unknown choice{choice} must be All,Subgraph, or Given type")

    curr_query = CodexQueryCluster(query=cluster_obj)

    return curr_query


def codex_cluster_action(
    codexkg,
    cluster_action,
    action: str,
    cluster_type: str,
    cluster_concepts: list,
    given_type: str,
    k_min: int,
):
    """
    Purpose:
        Cluster Query on Grakn
    Args:
        codexkg: codex kg
        cluster_action: what action to cluster with
        action: str: cluster specific action
        cluster_type: type of cluster,
        cluster_concepts: list of concepts,
        given_type: concept to hone in on,
        k_min: how many k groups,
    Returns:
        codex_query - Cluster object
    """

    if cluster_action == "centrality":
        return compute_centrality(
            codexkg, action, cluster_type, cluster_concepts, given_type, k_min
        )
    elif cluster_action == "cluster":
        return compute_cluster(
            codexkg, action, cluster_type, cluster_concepts, given_type, k_min
        )
    else:
        raise ValueError(
            f"cluster_action: {cluster_action} is not defined, muse be either centrality or cluster"
        )
