import json
import re
import uuid

import pandas as pd
import streamlit as st

import codex_viz as viz
from codex import CodexKg

from codex import CodexQueryFind, CodexQueryCompute, CodexQueryCluster, CodexQueryRule

from codex.codex_query_builder import (
    find_action,
    compute_action,
    codex_cluster_action,
    make_rule_cond,
    make_rule_string,
)


@st.cache(allow_output_mutation=True)
def cache_df(entity_csv: str) -> pd.DataFrame:
    """
    Purpose:
        Cache any dataframe
    Args:
        entity_csv: path to csv
    Returns:
        df: Pandas dataframe
    """
    df = pd.read_csv(entity_csv, index_col=False)
    return df


@st.cache(allow_output_mutation=True)
def save_cols(cols: list) -> list:
    """
    Purpose:
        cache df columns
    Args:
        cols: cache of df cols
    Returns:
        cols: list of columns
    """
    return cols


def codex_entities(codexkg):
    """
    Purpose:
        Logic for codex entities
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    # show current entities

    st.header("Entites")

    st.markdown(
        "An entity is a thing with a distinct existence in the domain. For example, `organization`, `location` and `person`. The existence of each of these entities is independent of any other concept in the domain."
    )

    st.subheader("Current Entities")

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
        # make sure it reads from 0
        entity_csv.seek(0)
        df = cache_df(entity_csv)
        cols = save_cols(df.columns)
        new_entity.write(df)

        entity_key = new_entity.selectbox("Enter key", cols)

        if new_entity.button("Create entity"):
            with st.spinner("Creating entity"):

                try:
                    status, message = codexkg.create_entity(df, entity_name, entity_key)

                    if status == 0:
                        st.balloons()
                        st.success(f"Entity {entity_name} created")
                    else:
                        st.error(message)
                        st.error("Failed to create entity")

                except Exception as error:
                    st.error(error)
                    st.error("Failed to create entity")


def codex_rels(codexkg) -> None:
    """
    Purpose:
        Logic for codex relationships
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    st.header("Relationships")

    st.markdown(
        "A relation describes how two or more things are in some way connected to each other. For example, `friendship` and `employment`. Each of these relations must relate to roles that are played by something else in the domain. In other words, relations are dependent on the existence of at least two other things."
    )

    st.subheader("Current Relationships")

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
        df = cache_df(rel_csv)
        cols = save_cols(df.columns)
        new_rel.write(df)

        rel1 = new_rel.selectbox("Enter Relationship 1", ents)

        st.write(rel1 + " " + cols[0])

        rel2 = new_rel.selectbox("Enter Relationship 2", ents)

        st.write(rel2 + " " + cols[1])

        if new_rel.button("Create relationship"):

            with st.spinner("Creating relationship..."):
                try:
                    status, message = codexkg.create_relationship(
                        df, rel_name, rel1, rel2
                    )

                    if status == 0:
                        st.balloons()
                        st.success(f"Relationship {rel_name} created")
                    else:
                        st.error(message)
                        st.error("Failed to create entity")
                except Exception as error:
                    st.error(error)
                    st.error("Failed to create Relationship")


def main_menu(codexkg, keyspace):
    """
    Purpose:
        Create sidebar menu
    Args:
        codexkg: Codexkg Object
        keyspace: current keyspace
    Returns:
        N/A
    """
    st.sidebar.header("These are the codex actions")

    st.sidebar.header("Refresh")
    if st.sidebar.button("Refresh"):
        print("ok")

    st.sidebar.header("Delete keyspace")
    if st.sidebar.button("Delete Keyspace"):
        codexkg.create_db(keyspace)
        codexkg.delete_db(keyspace)


def ontology_maker_app(codexkg) -> None:
    """
    Purpose:
        Logic for codex ontology maker app
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    st.header("Ontology Maker")
    st.subheader("Define your schema")

    st.markdown("The Ontology Maker allows you to define how your data is structured")

    options = ["Entites", "Relationships"]
    selected_option = st.selectbox("Select concept", options)

    if selected_option == "Entites":
        # show entities
        codex_entities(codexkg)

    elif selected_option == "Relationships":
        # show rels
        codex_rels(codexkg)


def graph_codex_ont(codexkg):
    """
    Purpose:
        Logic to dispaly keyspace ontology
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    ents = codexkg.entity_map
    rels = codexkg.rel_map
    keyspace = codexkg.keyspace

    viz.ent_rel_graph(ents, rels, keyspace)


def select_cond(attr, concept, concept_map, key):
    """
    Purpose:
        Logic to select condition
    Args:
        attr: current attribute
        concept: current concept
        concept_map: details fro concept
        key: key tracking for streamlit
    Returns:
        selected_cond : user selected condition
        selected_val: user selecetd value
    """
    attr_type = concept_map["cols"][attr]["type"]

    st.subheader(f"Select Conditions for {concept} {attr}")

    if attr_type == "string":

        conds = ["equals", "contains", "not equals", "not contains", "congruent"]

        selected_cond = st.selectbox(
            "Select Condition", conds, key=f"select cond for {attr} {key}"
        )
        selected_value = st.text_input("Enter Value", key=f"select val for {attr}{key}")

    if attr_type == "long" or attr_type == "double":

        conds = ["equals", "less than", "greater than", "not equals", "congruent"]

        selected_cond = st.selectbox(
            "Select Condition", conds, key=f"select cond for {attr}{key}"
        )
        selected_value = st.number_input(
            "Enter Value", key=f"select val for {attr}{key}"
        )

    if attr_type == "bool":

        conds = ["True", "False"]

        selected_cond = "equals"
        selected_value = st.selectbox(
            "Select Value", conds, key=f"select cond for {attr}{key}"
        )

    if attr_type == "date":

        conds = [
            "on",
            "after",
            "before",
            "between",
            "not on",
            "not between",
            "congruent",
        ]

        selected_cond = st.selectbox(
            "Select Condition", conds, key=f"select cond for {attr}{key}"
        )

        if "between" in selected_cond:
            st.warning("Add a space between the two dates i.e 10/01/2020 10/30/2020")

        selected_value = st.text_input("Enter Value", key=f"select val for {attr}{key}")

    return selected_cond, selected_value


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


def make_find_action_obj(codexkg, rule_num):
    """
    Purpose:
        Logic for codex entities
    Args:
        codexkg: Codexkg Object
        rule_num: key for streamlit
    Returns:
        query_object : Query object to return
    """
    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())

    ents_rels = ents + rels

    concept = st.selectbox(
        "Select Concept", ents_rels, key=f"Select concept {rule_num}"
    )

    # st.write(codexkg.entity_map)

    attr_list = []
    concept_map = {}

    if concept in ents:
        is_ent = True

        attr_list = list(codexkg.entity_map[concept]["cols"].keys())
        concept_map = codexkg.entity_map[concept]

    else:
        is_ent = False
        attr_list = list(codexkg.rel_map[concept]["cols"].keys())
        concept_map = codexkg.rel_map[concept]

    st.header(f"{concept} Query Builder")

    concept_attrs = st.multiselect(
        "Select Concept attrs", attr_list, key=f"Select attrs {rule_num}"
    )

    # for each concept select conds
    concept_conds = []
    concept_values = []
    for attr in concept_attrs:

        curr_cond, curr_val = select_cond(attr, concept, concept_map, 1 + rule_num)
        concept_conds.append(curr_cond)
        concept_values.append(curr_val)

    # only entities have rel actions
    if is_ent:

        concept_rels = list(concept_map["rels"].keys())
        rel_list = []
        concept_rel_map = {}

        for concept_rel in concept_rels:
            rel_action = concept_map["rels"][concept_rel]["plays"]
            rel_list.append(rel_action)
            if rel_action not in concept_rel_map:
                concept_rel_map[rel_action] = []

            other_ent = concept_map["rels"][concept_rel]["with_ent"]
            concept_rel_map[rel_action].append(other_ent)

        # now have a select box for the rel list

        rel_actions = st.multiselect(
            "Select Relation action", rel_list, key=f"Select rel actions {rule_num}"
        )

        concept_rels = []
        concept_rel_attrs = []
        concept_rel_conds = []
        concept_rel_values = []
        with_rel_attrs = []
        with_rel_conds = []
        with_rel_values = []

        for rel_act in rel_actions:
            other_concepts = concept_rel_map[rel_act]
            concept_rel = st.selectbox(
                "Select other concept",
                other_concepts,
                key=f"Select other concept {rule_num}",
            )

            concept_rels.append(concept_rel)
            # for each concept select conds

            other_attr_list = list(codexkg.entity_map[concept_rel]["cols"].keys())

            other_concept_attrs = st.multiselect(
                "Select Concept attrs",
                other_attr_list,
                key=f"Select other concept attrs {rule_num}",
            )
            other_concept_conds = []
            other_concept_values = []

            other_concept_map = codexkg.entity_map[concept_rel]
            for attr in other_concept_attrs:

                curr_cond, curr_val = select_cond(
                    attr, concept_rel, other_concept_map, 2 + rule_num
                )
                other_concept_conds.append(curr_cond)
                other_concept_values.append(curr_val)

            concept_rel_attrs.append(other_concept_attrs)
            concept_rel_conds.append(other_concept_conds)
            concept_rel_values.append(other_concept_values)

            have_rel_conds = st.checkbox(
                "Add relationship conditions?", key=f"Select rel conds {rule_num}"
            )

            if have_rel_conds:

                curr_rel = get_rel_name_from_ents(codexkg, concept, concept_rel)
                rel_concept_map = codexkg.rel_map[curr_rel]
                rel_attr_list = list(codexkg.rel_map[curr_rel]["cols"].keys())
                rel_concept_attrs = st.multiselect(
                    "Select Relationship Concept attrs",
                    rel_attr_list,
                    key=f"Select rel concept attrs {rule_num}",
                )

                rel_concept_conds = []
                rel_concept_values = []

                for attr in rel_concept_attrs:

                    curr_cond, curr_val = select_cond(
                        attr, concept_rel, rel_concept_map, 3 + rule_num
                    )
                    rel_concept_conds.append(curr_cond)
                    rel_concept_values.append(curr_val)

                with_rel_attrs.append(rel_concept_attrs)
                with_rel_conds.append(rel_concept_conds)
                with_rel_values.append(rel_concept_values)

    if rule_num == 1:
        query_obj = find_action(
            codexkg,
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

        st.subheader(query_obj)
    else:

        query_obj = make_rule_cond(
            codexkg,
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

        # st.subheader(query_obj)

    return query_obj


def find_action_codex(codexkg):
    """
    Purpose:
        Page for find action
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    query_obj = make_find_action_obj(codexkg, 1)

    if st.button("Query"):

        with st.spinner("Doing query..."):
            answers = codexkg.query(query_obj)

        for key in answers.keys():
            st.subheader(key)
            if answers[key] is None:
                st.error("No Matches for Query")
            else:
                st.write(answers[key])


def compute_cluster_action(codexkg):
    """
    Purpose:
        Page for cluster action
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    actions = ["connected-component", "k-core"]
    action = st.selectbox("Select Actions", actions)

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())
    ents_rels = ents + rels

    cluster_obj = {}

    if action == "connected-component":
        concepts = st.multiselect(
            "Select Concepts", ents_rels, key=f"{action} concept select"
        )

        cluster_obj["query_type"] = "cluster"
        cluster_obj["choice"] = "cluster-in"
        cluster_obj["concepts"] = concepts

        query_string = (
            f"compute cluster in {concept_string(concepts)}, using connected-component;"
        )

        cluster_obj["query_string"] = query_string

    elif action == "k-core":
        concepts = st.multiselect(
            "Select Concepts", ents_rels, key=f"{action} concept select"
        )

        cluster_obj["query_type"] = "cluster"
        cluster_obj["choice"] = "k-core"
        cluster_obj["concepts"] = concepts
        query_string = f"compute cluster in {concept_string(concepts)}, using k-core;"

        if st.checkbox("specify k?"):
            k_num = st.number_input("Select K", min_value=2, value=2, step=1)
            query_string = f"compute cluster in {concept_string(concepts)}, using k-core,where k={k_num};"

        cluster_obj["query_string"] = query_string

    else:
        st.error("Unknown type")

    # st.write(cluster_obj)
    st.header(query_string)
    curr_query = CodexQueryCluster(query=cluster_obj)

    if st.button("Query"):
        # st.success("Doing query")

        with st.spinner("Doing query..."):
            answers = codexkg.query(curr_query)
        # st.write(answers)

        viz.cluster_graph(answers, ents, rels, codexkg)


def compute_action_codex(codexkg):
    """
    Purpose:
        Page for compute action
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    compute_obj = {}

    actions = [
        "Count",
        "Sum",
        "Maximum",
        "Minimum",
        "Mean",
        "Median",
        "Standard Deviation",
    ]
    action_list = st.multiselect("Select Actions", actions)

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())
    ents_rels = ents + rels

    concepts = []
    concept_attrs = []

    for action in action_list:

        compute_obj[action] = []
        st.header(f"{action} Query Builder")

        if action == "Count":
            ents_rels.append("All Concepts")
            concept = st.selectbox(
                "Select Concept", ents_rels, key=f"{action} concept select"
            )

        else:
            concept = st.selectbox(
                "Select Concept", ents_rels, key=f"{action} concept select"
            )

        if concept in ents:
            concept_map = codexkg.entity_map[concept]
        elif concept in rels:
            concept_map = codexkg.rel_map[concept]

        if action == "Count":
            concept_attr = ""

        else:
            st.subheader(f"{concept} Query Builder")
            attr_list = list(concept_map["cols"].keys())

            int_attrs = []

            for attr in attr_list:
                if (
                    concept_map["cols"][attr]["type"] == "double"
                    or concept_map["cols"][attr]["type"] == "long"
                ):
                    int_attrs.append(attr)

            concept_attr = st.selectbox(
                "Select attr", int_attrs, key=f"{action}_{concept}"
            )

        concept_attrs.append(concept_attr)
        concepts.append(concept)

    query_obj = compute_action(codexkg, action_list, concepts, concept_attrs)
    st.subheader(query_obj)

    if st.button("Query"):

        with st.spinner("Doing query..."):
            answers = codexkg.query(query_obj)
        # st.write(answers)

        for key in answers.keys():

            answer_map = answers[key]

            for answer in answer_map:
                st.subheader(f"{key}: {answer['answer']}")


def compute_centrality_codex(codexkg):
    """
    Purpose:
        Page for centrality action
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    actions = ["degree", "k-core"]
    action = st.selectbox("Select Actions", actions)

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())
    ents_rels = ents + rels

    cluster_obj = {}

    if action == "degree":

        # TODO look into "All Concepts" not working
        choices = ["All Concepts", "Subgraph", "Given type"]
        choice = st.selectbox("Select Chocie", choices, key="centrality select actions")

        if choice == "All Concepts":
            query_string = "compute centrality using degree;"

            cluster_obj["query_string"] = query_string
            cluster_obj["query_type"] = "centrality"
            cluster_obj["choice"] = "All Concepts"
            cluster_obj["concepts"] = ents_rels

        elif choice == "Subgraph":

            concepts = st.multiselect(
                "Select Concepts", ents_rels, key=f"{action} concept select"
            )

            cluster_obj["query_type"] = "centrality"
            cluster_obj["choice"] = "subgraph"
            cluster_obj["concepts"] = concepts

            query_string = (
                f"compute centrality in {concept_string(concepts)}, using degree;"
            )

            cluster_obj["query_string"] = query_string

        elif choice == "Given type":

            given_type = st.selectbox(
                "Select Concept", ents_rels, key=f"{action} concept select given"
            )
            concepts = st.multiselect(
                "Select Concepts", ents_rels, key=f"{action} concept select"
            )

            cluster_obj["query_type"] = "centrality"
            cluster_obj["choice"] = "subgraph"
            cluster_obj["concepts"] = concepts
            cluster_obj["given_type"] = given_type

            query_string = f"compute centrality of {given_type}, in {concept_string(concepts)}, using degree;"
            cluster_obj["query_string"] = query_string
        else:
            st.error("Unknown type")

    if action == "k-core":
        # concepts = st.multiselect("Select Concepts", ents_rels,key=f"{action} concept select")

        cluster_obj["query_type"] = "centrality"
        cluster_obj["choice"] = "k-core"
        cluster_obj["concepts"] = ents_rels
        query_string = f"compute centrality using k-core;"
        concepts = ents_rels

        if st.checkbox("specify k?"):
            k_num = st.number_input("Select K", min_value=2, value=2, step=1)
            query_string = f"compute centrality using k-core, where min-k={k_num};"

        cluster_obj["query_string"] = query_string

    # st.write(cluster_obj)
    st.header(query_string)
    curr_query = CodexQueryCluster(query=cluster_obj)

    if st.button("Query"):
        # st.success("Doing query")
        with st.spinner("Doing query..."):
            answers = codexkg.query(curr_query)
        # st.write(answers)

        viz.cluster_graph(answers, ents, rels, codexkg)


def codex_reasoner(codexkg):
    """
    Purpose:
        Page for the reasoner
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    st.header("Reasoner")
    st.subheader("Ask and you shall receive")

    st.markdown("The Reasoner allows you to make queries to find data")

    # st.write(codexkg.entity_map)
    # st.write(codexkg.rel_map)

    # The actions supported
    actions = ["Find", "Compute", "Centrality", "Cluster", "Reason"]

    action = st.selectbox("Select Action", actions)

    if action == "Find":
        st.markdown("This query will find concepts that match your input condtions")
        find_action_codex(codexkg)

    if action == "Reason":
        st.markdown("This query will find relationships based on your rules")
        handle_rule_query(codexkg)

    if action == "Compute":
        st.markdown("This query will calculate statistical values over your data")
        compute_action_codex(codexkg)

    if action == "Centrality":
        st.markdown(
            "This query will find the most important instances in your data or a subset."
        )
        compute_centrality_codex(codexkg)

    if action == "Cluster":
        st.markdown(
            "This query will identify clusters of interconnected instances or those that are tightly linked within a network."
        )
        compute_cluster_action(codexkg)


def handle_rule_query(codexkg):
    """
    Purpose:
        Page for rule search
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    # get list of rules

    rules = list(codexkg.rules_map.keys())
    concept = st.selectbox("Select relationship", rules)
    # st.write(codexkg.rules_map[concept])

    rules_string = codexkg.rules_map[concept]["rule_string"]
    st.subheader(rules_string)

    query = f"match $x isa {concept}; get;"

    rule_ans = codexkg.rules_map[concept]["rule_string_ans"]
    # st.subheader(rule_ans)

    # try:
    #     rule_ans = codexkg.rules_map[concept]["rule_string_ans"]
    # except:
    #     rule_ans = "Company_A produces Product_name_X that has a name that Contains widget. Company_B produces Product_name_Y that has a name that Contains widget."

    rule_resp = []

    if st.button(f"Find {concept} relationships"):
        answers = codexkg.raw_graql(query, "read")
        # st.write(answers)
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

                    if concept in list(codexkg.entity_map.keys()):
                        concept_key = codexkg.entity_map[concept]["key"]

                        try:
                            rule_new = rule_new.replace(
                                str(exp_key), str(explanation[exp_key][concept_key])
                            )
                        except:
                            rule_new = rule_new

                        # st.write(f"{exp_key}: {explanation[exp_key][concept_key]}")
                        # st.subheader(rule_ans)

            answer_counter += 1
            if rule_new in rule_resp:
                continue
            rule_resp.append(rule_new)
            col1, col2 = st.beta_columns(2)

            col1.write(answer[key_con]["concepts"][0])
            col2.write(answer[key_con]["concepts"][1])
            st.subheader(rule_new)


def raw_query(codexkg):
    """
    Purpose:
        Page for raw queries
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    st.header("Raw graql queries")

    st.markdown("This for entering raw graql queries")

    query_types = ["read", "write"]
    mode = st.selectbox("Select query type", query_types)

    query = st.text_input("Enter Query")

    if st.button("Do query"):
        with st.spinner("Doing query..."):
            answers = codexkg.raw_graql(query, mode)
        st.write(answers)


def rule_maker(codexkg):
    """
    Purpose:
        Page to make rules
    Args:
        codexkg: Codexkg Object
    Returns:
        N/A
    """
    st.header("Rule Maker")
    st.subheader("lets make some rules")
    st.markdown(
        "Rules look for a given pattern in the dataset and when found, create the given queryable relation"
    )

    rule_name = st.text_input("Enter rule_name")

    # replace spaces
    rule_name = rule_name.replace(" ", "_")

    # cond 1
    st.markdown("Enter condtions for the first concept")
    rule_cond1 = make_find_action_obj(codexkg, 5)

    # cond 2
    st.markdown("Enter condtions for the second concept")
    rule_cond2 = make_find_action_obj(codexkg, 10)

    # query =f"define {rule_name}  sub rule,"

    # st.header("Rule Object")

    rule_obj = {}

    rule_obj["name"] = rule_name
    rule_obj["cond1"] = rule_cond1
    rule_obj["cond2"] = rule_cond2

    rule_string, rule_string_ans = make_rule_string(rule_obj)

    st.header(rule_string)
    # st.header(rule_string_ans)

    curr_query = CodexQueryRule(
        rule=rule_obj, rule_string=rule_string, rule_string_ans=rule_string_ans
    )

    if st.button("Create rule"):

        with st.spinner("Creating rule..."):
            try:
                _answers = codexkg.query(curr_query)
                # st.write(answers)
                st.success("Rule created")
            except Exception as error:
                st.error(error)
                st.error("Failed to create rule")


def concept_string(concepts: list) -> str:
    """
    Purpose:
        Turn list of concepts to a graql string
    Args:
        concept: list of concepts
    Returns:
        concept_string: concepts as a string
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


# user opens up codex
# auto connect to localhost, have expander setup options
def get_codex_keyspaces():
    """
    Purpose:
        Setup for codex
    Args:
        N/A
    Returns:
        N/A
    """
    st.sidebar.subheader("Here are options to configure the Grakn and Redis Databases")
    expander = st.sidebar.beta_expander("Database Options")

    uri = expander.text_input("Grakn uri", value="localhost:48555")
    credentials = expander.text_input("Grakn Password")
    redis_host = expander.text_input("Redis Host", value="localhost")
    redis_port = expander.number_input("Redis Port", min_value=1, value=6379, step=1)
    redis_db = expander.number_input("Redis db", min_value=0, value=0, step=1)
    redis_password = expander.text_input("Redis Password")

    if redis_password == "":
        redis_password = None

    if credentials == "":
        credentials = None

    codexkg = CodexKg(
        uri=uri,
        credentials=credentials,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
    )

    status_container = st.empty()

    # then get a list of dbs
    select_proj = st.empty()
    keyspaces = codexkg.get_keyspaces()
    keyspace = select_proj.selectbox("Select Project", keyspaces)

    # connect to keyspace
    codexkg.create_db(keyspace)

    graph_codex_ont(codexkg)

    # show codex_grapj

    # TODO show graph of key space
    apps = ["Ontology Maker", "Reasoner", "Rules", "Graql"]

    st.sidebar.header(f"Codex Apps")
    app = st.sidebar.selectbox("Select App", apps)

    if app == "Ontology Maker":
        ontology_maker_app(codexkg)

    if app == "Reasoner":
        codex_reasoner(codexkg)

    if app == "Rules":
        rule_maker(codexkg)

    if app == "Graql":
        raw_query(codexkg)

    # create new project
    st.sidebar.subheader("Create a new Project")

    new_project = st.sidebar.beta_expander("New Project")
    new_project_name = new_project.text_input("Project Name")

    if new_project.button("Create Project"):

        if new_project_name in keyspaces:
            st.warning(f"Project {new_project_name} already exists")
            return

        try:
            codexkg.create_db(str(new_project_name))
            keyspaces = codexkg.get_keyspaces()
            select_proj.selectbox("Select Project", keyspaces)
            st.balloons()
            status_container.success(f"{new_project_name} created")
        except Exception as error:
            st.error(error)
            st.error(f"Could not create project {new_project_name}")

    st.sidebar.subheader(f"Delete Project {keyspace}")
    keyspace_delete_confirm = st.sidebar.text_input("Type project name to confirm")
    if keyspace_delete_confirm == keyspace:
        if st.sidebar.button("Delete Keyspace"):
            codexkg.delete_db(keyspace)

            keyspaces = codexkg.get_keyspaces()
            select_proj.selectbox("Select Project", keyspaces)
            st.balloons()
            status_container.success(f"{keyspace} deleted")


def main():
    """
    Purpose:
        Start codex_st
    Args:
        N/A
    Returns:
        N/A
    """
    st.title("Codex")
    st.header("Codex allows you to gain insights from your data")

    get_codex_keyspaces()


if __name__ == "__main__":
    main()