import json
import re
import uuid

# import inflect
import pandas as pd
import streamlit as st


from codex import CodexKg
from codex import CodexQueryFind, CodexQueryCompute, CodexQueryCluster, CodexQueryRule

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


def cond_setter(
    attr_type: str, attr_name: str, concept: str, seed: str, rule_num: int
) -> str:

    cond_json = {}

    st.subheader(f"{concept}-{attr_name}")

    if attr_type == "string":
        conds = ["Equals", "Contains"]
        selected_cond = st.selectbox(
            "Select Condition",
            conds,
            key=f"{concept}-{attr_name} {seed} {rule_num} cond checker",
        )
        cond_value = st.text_input(
            "Condition Value",
            key=f"{concept}-{attr_name} {seed} {rule_num}  cond value",
        )
        cond_string = " that " + selected_cond + " " + cond_value

    if attr_type == "long" or attr_type == "double":
        conds = ["Equals", "Less Than", "Greater Than"]
        selected_cond = st.selectbox(
            "Select Condition",
            conds,
            key=f"{concept}-{attr_name} {seed}  {rule_num} cond checker",
        )
        cond_value = st.number_input(
            "Condition Value",
            key=f"{concept}-{attr_name} {seed}  {rule_num} cond value",
        )
        cond_string = f" that {selected_cond} {cond_value}"

    cond_json["selected_cond"] = selected_cond
    cond_json["cond_value"] = cond_value
    cond_json["cond_string"] = cond_string

    return cond_json


def attr_setter(codexkg: CodexKg, concept: str, is_ent: bool, rule_num):

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
    selected_attrs = st.multiselect(f"Select Attributes {rule_num}", attr_list_comp)

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
            cond_json = cond_setter(
                attr_type, selected_attr, concept, "seed1", rule_num
            )
            attr_json["attr_concept"] = concept

        else:
            attr_string = " that " + selected_attr
            st.subheader(f"Concepts for {selected_attr}")
            selected_ent2 = st.selectbox(
                f"Select Entity {rule_num}", plays_map[selected_attr]
            )

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
            selected_attr = st.selectbox(f"Select Attribute {rule_num}", attr_list2)
            attr_type = codexkg.entity_map[selected_ent2]["cols"][selected_attr]["type"]

            attr_string += " that have a " + selected_attr

            cond_json = cond_setter(
                attr_type, selected_attr, selected_ent2, "seed2", rule_num
            )

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


def find_action(codexkg):

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

        attr_obj_list = attr_setter(codexkg, concept, is_ent, 1)

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

        for key in answers.keys():
            st.subheader(key)
            if answers[key] is None:
                st.error("No Matches for Query")
            else:
                st.write(answers[key])


def attr_setter_compute(
    codexkg: CodexKg, concept: str, is_ent: bool, action: str
) -> list:

    # Get Values from entites
    if is_ent:
        attrs = codexkg.entity_map[concept]["cols"]
        attr_list = list(attrs.keys())
        attr_list_comp = attr_list

    # Get values for relationships
    else:
        attrs = codexkg.rel_map[concept]["cols"]
        attr_list = list(attrs.keys())
        attr_list.remove("codex_details")
        attr_list_comp = attr_list

    # this is the attribute for the entity i.e name

    # check if it is a long/double
    int_attrs = []
    for attr in attr_list_comp:
        if attrs[attr]["type"] == "double" or attrs[attr]["type"] == "long":
            int_attrs.append(attr)

    selected_attrs = st.multiselect(
        "Select Attributes", int_attrs, key=f"{concept}_{action} attr select"
    )

    return selected_attrs


def concept_string(concepts: list) -> str:

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


def compute_cluster(codexkg):

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

    st.write(cluster_obj)
    st.header(query_string)
    curr_query = CodexQueryCluster(query=cluster_obj)

    if st.button("Query"):
        st.success("Doing query")
        answers = codexkg.query(curr_query)
        st.write(answers)


def compute_centrality(codexkg):

    actions = ["degree", "k-core"]
    action = st.selectbox("Select Actions", actions)

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())
    ents_rels = ents + rels

    cluster_obj = {}

    if action == "degree":

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

    st.write(cluster_obj)
    st.header(query_string)
    curr_query = CodexQueryCluster(query=cluster_obj)

    if st.button("Query"):
        st.success("Doing query")
        answers = codexkg.query(curr_query)
        st.write(answers)


def compute_action(codexkg):

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

    for action in action_list:

        compute_obj[action] = []

        if action == "Count":
            ents_rels.append("All Concepts")
            concepts = st.multiselect(
                "Select Concepts", ents_rels, key=f"{action} concept select"
            )

        else:
            concepts = st.multiselect(
                "Select Concepts", ents_rels, key=f"{action} concept select"
            )

        # select concept
        for concept in concepts:
            st.header(f"{action} Query Builder")

            if concept in ents:
                is_ent = True
            else:
                is_ent = False

            if action == "Count":
                query_text = f"Compute {action} for {concept}"
                st.header(query_text)

                count_obj = {}
                count_obj["concept"] = concept
                count_obj["query_text"] = query_text

                compute_obj[action].append(count_obj)

            else:
                st.subheader(f"{concept} Query Builder")
                attr_obj_list = attr_setter_compute(codexkg, concept, is_ent, action)

                for attr in attr_obj_list:
                    query_text = f"Compute {action} for {attr} in {concept}"

                    action_obj = {}
                    action_obj["concept"] = concept
                    action_obj["attr"] = attr
                    action_obj["query_text"] = query_text

                    compute_obj[action].append(action_obj)
                    st.header(query_text)

    st.write(compute_obj)
    curr_query = CodexQueryCompute(queries=compute_obj)

    if st.button("Query"):
        st.success("Doing query")
        answers = codexkg.query(curr_query)
        st.write(answers)

        # for key in answers.keys():
        #     st.subheader(key)
        #     if answers[key] is None:
        #         st.error("No Matches for Query")
        #     else:
        #         st.write(answers[key])

    # select int value


def codex_reasoner(codexkg):

    st.header("Reasoner")
    st.subheader("Ask and you shall receive")

    # st.write(codexkg.entity_map)
    # st.write(codexkg.rel_map)

    # The actions supported
    actions = ["Find", "Compute", "Centrality", "Cluster"]

    action = st.selectbox("Select Action", actions)

    if action == "Find":
        find_action(codexkg)

    if action == "Compute":
        compute_action(codexkg)

    if action == "Centrality":
        compute_centrality(codexkg)

    if action == "Cluster":
        compute_cluster(codexkg)


def rule_action(codexkg, rule_num):

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())

    ents_rels = ents + rels

    concept = st.selectbox(f"Select Rule{rule_num} Concepts", ents_rels)

    # concept_type = ""

    if concept in ents:
        is_ent = True
        concept_type = "Entity"
    else:
        is_ent = False
        concept_type = "Relationship"

    st.header(f"{concept} Query Builder")

    attr_obj_list = attr_setter(codexkg, concept, is_ent, rule_num)

    concept_json = {}
    concept_json["concept"] = concept
    concept_json["concept_type"] = concept_type
    concept_json["attrs"] = attr_obj_list
    concept_json["query_string"] = query_string_find_maker(concept, attr_obj_list)

    # st.write(attr_obj_list)

    # TODO make a codex_query object
    # TODO add mulipte queries
    try:
        query_text = ""
        query_text += concept_json["query_string"]
    except:
        query_text = "Enter Query:"

    # make a codex_query object here
    # curr_query = CodexQueryFind(concepts=codex_query_list, query_string=query_text)

    # st.write(str(curr_query))

    st.write(concept_json)

    st.header(query_text)

    return concept_json
    # if st.button("Query"):
    #     st.success("Doing query")
    #     answers = codexkg.query(curr_query)

    #     for key in answers.keys():
    #         st.subheader(key)
    #         if answers[key] is None:
    #             st.error("No Matches for Query")
    #         else:
    #             st.write(answers[key])


def make_rule_string(rule_obj):

    rule_string = ""

    # check cond1
    rule_name = rule_obj["name"]

    cond1 = rule_obj["cond1"]
    cond2 = rule_obj["cond2"]

    rule_string += f"If {cond1['concept']} A "

    attr_len = len(cond1["attrs"])
    attr_counter = 1
    for attr in cond1["attrs"]:

        if "rel_attr" in attr:
            rule_string += f"{attr['rel_attr']} {attr['rel_ent']} X that has a {attr['attribute']} {attr['cond']['cond_string']}"
        else:
            rule_string += f" has a {attr['attribute']} {attr['cond']['cond_string']}"           

        if attr_counter == attr_len:
            rule_string += "."
        else:
            rule_string += " and "

        attr_counter += 1

    # check cond2
    attr_len = len(cond2["attrs"])
    attr_counter = 1
    rule_string += f"If {cond2['concept']} B "
    for attr in cond2["attrs"]:

        if "rel_attr" in attr:
            rule_string += f"{attr['rel_attr']} {attr['rel_ent']} Y that has a {attr['attribute']} {attr['cond']['cond_string']}"
        else:
            rule_string += f" has a {attr['attribute']} {attr['cond']['cond_string']}"           

        if attr_counter == attr_len:
            rule_string += "."
        else:
            rule_string += " and "

        attr_counter += 1


    rule_string += f"Then  {cond1['concept']} A and {cond2['concept']} B are {rule_name}"
    




    return rule_string


def rule_maker(codexkg):

    st.header("Rule Maker")
    st.subheader("lets make some rules")

    rule_name = st.text_input("Enter rule_name")

    # cond 1
    rule_cond1 = rule_action(codexkg, 1)

    # cond 2
    rule_cond2 = rule_action(codexkg, 2)

    # query =f"define {rule_name}  sub rule,"

    st.header("Rule Object")

    rule_obj = {}

    rule_obj["name"] = rule_name
    rule_obj["cond1"] = rule_cond1
    rule_obj["cond2"] = rule_cond2

    st.write(rule_obj)

    rule_string = make_rule_string(rule_obj)

    st.header(rule_string)


    curr_query = CodexQueryRule(rule=rule_obj, rule_string=rule_string)

    if st.button("Create rule"):
        st.success("Creating rule...")
        answers = codexkg.query(curr_query)
        st.write(answers)



    # st.subheader(query)

    # Given the data, define a rule..
    # A rule defines a relationship between two entities, based on condtions

    # So for our simple example...

    # if company a produce a product with the word widget
    # if compnay b produces a product with the word widget
    # and a != b

    # then a and b are competitors

    # Our graql for this will be...
    # match $Company isa Company; $Product isa Product, has name $Product_name;(produces: $Company, produced: $Product) isa Productize;{ $Product_name contains "widget";};get;
    #

    # define
    # RULE_NAME sub rule,
    # when {
    #  match $Company isa Company; $Product isa Product, has name $Product_name;(produces: $Company, produced: $Product) isa Productize;{ $Product_name contains "widget";};

    #
    #      },
    # then {
    #
    # };

    # example graql

    # define

    # people-with-same-parents-are-siblings sub rule,
    # when {
    #     (mother: $m, $x) isa parentship;
    #     (mother: $m, $y) isa parentship;
    #     (father: $f, $x) isa parentship;
    #     (father: $f, $y) isa parentship;
    #     $x != $y;
    # }, then {
    #     (sibling: $x, sibling: $y) isa siblings;
    # };

    #



def raw_query(codexkg):


    query_types = ["read","write"]
    mode = st.selectbox("Select query type",query_types)

    query = st.text_input("Enter Query")

    if st.button("Do query"):
        answers = codexkg.raw_graql(query,mode)
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

        # show ruler maker
        rule_maker(codexkg)

        #raw query
        raw_query(codexkg)




if __name__ == "__main__":
    main()
