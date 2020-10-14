import json
import re
import uuid

# import inflect
import pandas as pd
import streamlit as st


from codex import CodexKg
from codex import CodexQueryFind

# p = inflect.engine()

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


@st.cache(allow_output_mutation=True)
def cache_df(entity_csv):
    df = pd.read_csv(entity_csv)
    return df, df.columns


def codex_entities(codexkg):

    # show current entities

    st.header("Current Entities")

    for key in codexkg.entity_map.keys():
        curr_entity = codexkg.entity_map[key]

        expander = st.beta_expander(key)
        expander.header("Key")
        expander.text("Key:" + curr_entity["key"])
        expander.header("Attributes")
        for col in curr_entity["cols"]:

            expander.text(col + ": " + str(curr_entity["cols"][col]))

    # create new entity

    st.header("Create new Entity")

    new_entity = st.beta_expander("Create new Entity")

    entity_name = new_entity.text_input("Entity Name")
    entity_csv = new_entity.file_uploader("Entity CSV", type="csv")

    if entity_csv is not None:
        df, cols = cache_df(entity_csv)
        new_entity.write(df)

        entity_key = new_entity.selectbox("Enter key", cols)

        if new_entity.button("Create entity"):
            codexkg.create_entity(df, entity_name, entity_key)


def codex_rels(codexkg):

    # show current entities

    st.header("Current Relationships")

    # {'Proudctize': {'rel1': {'role': 'produces', 'entity': 'Company', 'key': 'name', 'key_type': 'string'}, 'rel2': {'role': 'produced', 'entity': 'Product', 'key': 'name', 'key_type': 'string'}, 'cols': {'codex_details': 'string', 'note': {'type': 'string'}}}}

    for key in codexkg.rel_map.keys():
        curr_rel = codexkg.rel_map[key]

        expander = st.beta_expander(key)
        expander.header("Relationship 1")
        expander.text(curr_rel["rel1"])

        expander.header("Relationship 2")
        expander.text(curr_rel["rel2"])

        expander.header("Attributes")
        for col in curr_rel["cols"]:
            expander.text(col + ": " + str(curr_rel["cols"][col]))

    # create new entity

    st.header("Create new Relationship")

    new_rel = st.beta_expander("Create new Relationship")

    rel_name = new_rel.text_input("Relationship Name")
    rel_csv = new_rel.file_uploader("Relationship CSV", type="csv")

    ents = list(codexkg.entity_map.keys())

    if rel_csv is not None:
        df, cols = cache_df(rel_csv)
        new_rel.write(df)

        rel1 = new_rel.selectbox("Enter Relationship 1", ents)

        st.write(rel1 + " " + cols[0])

        rel2 = new_rel.selectbox("Enter Relationship 2", ents)

        st.write(rel2 + " " + cols[1])

        if new_rel.button("Create relationship"):
            codexkg.create_relationship(df, rel_name, rel1, rel2)

    # codexkg.create_relationship(
    #     "sample_data/company_sample.csv", "Proudctize", "Company", "Product"
    # )


def main_menu(codexkg, keyspace):

    st.sidebar.header("These are the codex actions")

    st.sidebar.header("Refresh")
    if st.sidebar.button("Refresh"):
        print("ok")

    st.sidebar.header("Delete keyspace")
    if st.sidebar.button("Delete Keyspace"):
        codexkg.create_db(keyspace)
        codexkg.delete_db(keyspace)


def cond_setter(attr_type: str, attr_name: str, concept: str, seed: str) -> str:

    cond_json = {}

    st.subheader(f"{concept}-{attr_name}")

    if attr_type == "string":
        conds = ["Equals", "Contains"]
        selected_cond = st.selectbox(
            "Select Condition", conds, key=f"{concept}-{attr_name} {seed} cond checker"
        )
        cond_value = st.text_input(
            "Condition Value", key=f"{concept}-{attr_name} {seed} cond value"
        )
        cond_string = " that " + selected_cond + " " + cond_value

    if attr_type == "long" or attr_type == "double":
        conds = ["Equals", "Less Than", "Greater Than"]
        selected_cond = st.selectbox(
            "Select Condition", conds, key=f"{concept}-{attr_name} {seed} cond checker"
        )
        cond_value = st.number_input(
            "Condition Value", key=f"{concept}-{attr_name} {seed} cond value"
        )
        cond_string = f" that {selected_cond} {cond_value}"

    cond_json["selected_cond"] = selected_cond
    cond_json["cond_value"] = cond_value
    cond_json["cond_string"] = cond_string

    return cond_json


def attr_setter(codexkg: CodexKg, concept: str, is_ent: bool):

    # Get Values from entites
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

        attr_list_comp = attr_list + rel_attrs

    # Get values for relationships
    else:
        attrs = codexkg.rel_map[concept]["cols"]
        attr_list = list(attrs.keys())
        attr_list.remove("codex_details")
        attr_list_comp = attr_list

    # this is the attribute for the entity i.e name
    selected_attrs = st.multiselect("Select Attributes", attr_list_comp)

    attr_obj_list = []

    for selected_attr in selected_attrs:

        attr_json = {}

        if selected_attr in attr_list:
            attr_string = " that have a " + selected_attr

            if is_ent:
                attr_type = codexkg.entity_map[concept]["cols"][selected_attr]["type"]
            else:
                attr_type = codexkg.rel_map[concept]["cols"][selected_attr]["type"]

            # TODO can we make this a function
            # check condtion type
            cond_json = cond_setter(attr_type, selected_attr, concept, "seed1")
            attr_json["attr_concept"] = concept

        else:
            attr_string = " that " + selected_attr
            st.subheader(f"Concepts for {selected_attr}")
            selected_ent2 = st.selectbox("Select Entity", plays_map[selected_attr])

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
            attr_list2 = list(attrs2.keys())
            selected_attr = st.selectbox("Select Attribute", attr_list2)
            attr_type = codexkg.entity_map[selected_ent2]["cols"][selected_attr]["type"]

            attr_string += " that have a " + selected_attr

            cond_json = cond_setter(attr_type, selected_attr, selected_ent2, "seed2")

        attr_json["cond"] = cond_json
        attr_json["attr_type"] = attr_type
        attr_json["attribute"] = selected_attr
        attr_json["attr_string"] = attr_string

        attr_obj_list.append(attr_json)

    return attr_obj_list


def query_string_find_maker(concept: str, attr_obj_list: dict) -> str:

    query_string = f"Find {plural(concept)}"

    attr_len = len(attr_obj_list)
    attr_counter = 1

    for attr in attr_obj_list:

        query_string += f"{attr['attr_string']}{attr['cond']['cond_string']}"

        if not attr_counter == attr_len:
            query_string += " and "

        attr_counter += 1

    query_string += ". "

    return query_string


def get_rel_name_from_ents(codexkg, rel1: str, rel2: str) -> str:

    rels = list(codexkg.rel_map.keys())
    rel_name = ""

    for rel in rels:

        check1 = codexkg.rel_map[rel]["rel1"]["entity"]
        check2 = codexkg.rel_map[rel]["rel2"]["entity"]

        if rel1 == check1 or rel1 == check2:
            if rel2 == check1 or rel2 == check2:
                rel_name = rel

    return rel_name


def codex_reasoner(codexkg):

    st.header("Reasoner")
    st.subheader("Ask and you shall receive")

    # st.write(codexkg.entity_map)
    # st.write(codexkg.rel_map)

    # The actions supported
    actions = ["Find", "Compute", "Cluster"]

    action = st.selectbox("Select Action", actions)

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())

    ents_rels = ents + rels

    concepts = st.multiselect("Select Concepts", ents_rels)

    # concept_type = ""
    codex_query_list = []
    for concept in concepts:

        if concept in ents:
            is_ent = True
            concept_type = "Entity"
        else:
            is_ent = False
            concept_type = "Relationship"

        st.header(f"{concept} Query Builder")

        attr_obj_list = attr_setter(codexkg, concept, is_ent)

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

    st.write(codex_query_list)

    st.header(query_text)
    if st.button("Query"):
        st.success("Doing query")
        answers = codexkg.query(curr_query)
        st.write(answers)


def main():

    st.title("Codex")
    st.header("Codex allows you to gain insights from your data")

    keyspace = st.text_input("Enter your project name")

    if keyspace is not "":
        codexkg = CodexKg()
        codexkg.create_db(keyspace)
        main_menu(codexkg, keyspace)

        # show entities
        codex_entities(codexkg)

        # show rels
        codex_rels(codexkg)

        # show reasoner
        codex_reasoner(codexkg)


if __name__ == "__main__":
    main()
