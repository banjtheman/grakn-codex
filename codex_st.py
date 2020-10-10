import json

import pandas as pd
import streamlit as st


from codex import CodexKg


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

        #show rels
        codex_rels(codexkg)


if __name__ == "__main__":
    main()
