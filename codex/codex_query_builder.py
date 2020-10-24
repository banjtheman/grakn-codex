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


# from .codex_query import CodexQueryFind, CodexQuery, CodexQueryCompute, CodexQueryRule


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


def get_rel_name_from_ents(codexkg, rel1: str, rel2: str) -> str:

    rels = list(codexkg.rel_map.keys())
    rel_name = ""

    # logging.info(" do we have rels?")
    # logging.info(rels)
    # logging.info(codexkg.rel_map)

    for rel in rels:

        check1 = codexkg.rel_map[rel]["rel1"]["entity"]
        check2 = codexkg.rel_map[rel]["rel2"]["entity"]

        # print("the rel name checks")
        # print(check1)
        # print(check2)

        if rel1 == check1 or rel1 == check2:
            if rel2 == check1 or rel2 == check2:
                rel_name = rel

    return rel_name


def cond_json_maker(concept_cond: str, concept_value) -> dict:

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

    cond_json = {}

    # logging.info("the type:")
    # logging.info(attr_type)

    if attr_type == "string":

        conds = ["equals", "contains"]

        if concept_cond in conds:

            cond_json = cond_json_maker(concept_cond, concept_value)

        else:
            raise ValueError(
                f"For string values must be equals or contains not {concept_cond}"
            )

    elif attr_type == "long" or attr_type == "double":
        conds = ["equals", "less than", "greater than"]

        if concept_cond in conds:
            cond_json = cond_json_maker(concept_cond, concept_value)
        else:
            raise ValueError(f"For int values {concept_cond} is not in {conds}")

    elif attr_type == "bool":
        conds = ["true", "false"]

        if concept_cond in conds:
            cond_json = cond_json_maker(concept_cond, concept_value)
        else:
            raise ValueError(f"For bool values {concept_cond} is not in {conds}")

    else:
        logging.info("error?")
        raise TypeError("Undefined Type")

    return cond_json



def cond_array_maker(
    codexkg, concept, is_ent, rel_attrs, rel_conds, rel_values, attr_string
):

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

        cond_array.append(
            cond_json

        )

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
):

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
                attrs2 = codexkg.entity_map[selected_ent2]["cols"]
                # attr_list2 = list(attrs2.keys())

                # basicaly do a loop for all rel_conds and rel_values

                # TODO Come fix
                # selected_attr = st.selectbox(f"Select Attribute {rule_num}", attr_list2)

                rel_attr_counter = 0
                cond_json = []

                selected_attr = []
                attr_type = []

                # cond_json_new = cond_array_maker(
                #     codexkg,
                #     selected_ent2,
                #     True,
                #     rel_attrs,
                #     rel_conds,
                #     rel_values,
                #     attr_string,
                # )

                #TODO why cant this be a function?

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
                attrs2 = codexkg.entity_map[selected_ent2]["cols"]
                # attr_list2 = list(attrs2.keys())
                # selected_attr = st.selectbox(f"Select Attribute {rule_num}", attr_list2)
                # attr_type = codexkg.entity_map[selected_ent2]["cols"][selected_attr]["type"]
                attr_string + " that have a " + selected_ent2 + " relationship that "

                cond_json = {}

                attr_type = None

            # with_rel_cond = st.checkbox(
            #     "Relationship condition?", key=f"rel cond {rule_num}"
            # )

            if len(with_rel_attrs) > 0:
                # rel_cond_list = attr_setter(codexkg, rel_name, False, rule_num + 1)
                # logging.info("Check rel values")
                # logging.info(with_rel_attrs)
                attr_json["rel_conds"] = cond_array_maker(
                    codexkg,
                    rel_name,
                    False,
                    with_rel_attrs[rel_action_counter],
                    with_rel_conds[rel_action_counter],
                    with_rel_values[rel_action_counter],
                    attr_string,
                )

                # logging.info("Here is the rel con json")

                # logging.info(attr_json["rel_conds"])
                # st.write(rel_cond_list)
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

    query_string = f"Find {plural(concept)}"

    attr_len = len(attr_obj_list)
    attr_counter = 1

    for attr in attr_obj_list:

        try:

            # logging.info("here comes")
            # logging.info(attr)
            query_string += f"{attr['attr_string']}{attr['cond']['cond_string']}"

            # logging.info(query_string)
        except:
            query_string += f"{attr['attr_string']}"

        if "rel_conds" in attr:
            query_string += " and "

            query_string += query_string_find_maker(attr["rel_name"], attr["rel_conds"])

        if not attr_counter == attr_len:
            query_string += " and "

        attr_counter += 1

    query_string += ". "

    return query_string


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

    # st.write(attr_obj_list)

    # TODO make a codex_query object
    # TODO add mulipte queries
    try:
        query_text = ""
        for concept_obj in codex_query_list:
            query_text += concept_obj["query_string"]

    except:
        query_text = "Enter Query:"

    # make a codex_query object here
    curr_query = CodexQueryFind(concepts=codex_query_list, query_string=query_text)

    # st.write(str(curr_query))

    return curr_query


    # st.write(codex_query_list)

    # st.header(query_text)
    # if st.button("Query"):

    #     with st.spinner("Doing query..."):
    #         answers = codexkg.query(curr_query)

    #     for key in answers.keys():
    #         st.subheader(key)
    #         if answers[key] is None:
    #             st.error("No Matches for Query")
    #         else:
    #             st.write(answers[key])
