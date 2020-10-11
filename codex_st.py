import json
import re

# import inflect
import pandas as pd
import streamlit as st


from codex import CodexKg


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


def codex_reasoner(codexkg):

    st.header("Reasoner")
    st.subheader("Ask and you shall receive")

    st.write(codexkg.entity_map)
    st.write(codexkg.rel_map)

    # The actions supported
    actions = ["Find", "Compute", "Cluster"]

    action = st.selectbox("Select Action", actions)

    ents = list(codexkg.entity_map.keys())
    rels = list(codexkg.rel_map.keys())

    ents_rels = ents + rels

    concept = st.selectbox("Select Concept", ents_rels)

    if concept in ents:
        is_ent = True
    else:
        is_ent = False

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
    selected_attr = st.selectbox("Select Attribute", attr_list_comp)

    if selected_attr in attr_list:
        attr_string = " that have a " + selected_attr

        if is_ent:
            attr_type = codexkg.entity_map[concept]["cols"][selected_attr]["type"]
        else:
            attr_type = codexkg.rel_map[concept]["cols"][selected_attr]["type"]

        #TODO can we make this a function
        # check condtion type
        if attr_type == "string":
            conds = ["Equals", "Contains"]
            selected_cond = st.selectbox("Select Condition", conds)
            cond_value = st.text_input("Condition Value")
            cond_string = " that " + selected_cond + " " + cond_value

    else:
        attr_string = " that " + selected_attr
        selected_ent2 = st.selectbox("Select Entity", plays_map[selected_attr])
        attr_string += " " + selected_ent2
        attrs2 = codexkg.entity_map[selected_ent2]["cols"]
        attr_list2 = list(attrs2.keys())
        selected_attr = st.selectbox("Select Attribute", attr_list2)
        attr_type2 = codexkg.entity_map[selected_ent2]["cols"][selected_attr]["type"]

        attr_string += " that have a " + selected_attr

        #TODO can we make this a function
        # check condtion type
        if attr_type2 == "string":
            conds = ["Equals", "Contains"]
            selected_cond = st.selectbox("Select Condition", conds)
            cond_value = st.text_input("Condition Value")
            cond_string = " that " + selected_cond + " " + cond_value

        # st.write(plays_map[selected_attr])

    #TODO make a codex_query object
    #TODO add mulipte queries
    query_text = action + " " + plural(concept) + attr_string + cond_string

    st.header(query_text)
    if st.button("Query"):
        st.success("Doing query")


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
