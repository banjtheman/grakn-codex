from codex import CodexKg


def main():
    codexkg = CodexKg()
    codexkg.create_db("meowth")
    # #codexkg.get_entites_grakn()
    # company_df = pd.read_csv("sample_data/company.csv")
    # codexkg.create_entity("sample_data/company.csv", "Company", "name")
    # product_df = pd.read_csv("sample_data/sample.csv")
    # codexkg.create_entity(product_df, "Product", "name")

    # company_product_df = pd.read_csv("sample_data/company_sample.csv")
    # codexkg.create_relationship(
    #     company_product_df, "Proudctize", "Company", "Product"
    # )

    print("")
    print(codexkg.entity_map)
    print(codexkg.rel_map)


    # find all companies that have name contains "Two"

    # {'1': {'action': 'find', 'concept_type': 'entity', 'concept': 'Company', 'check_type': 'attribute', 'check_attr': 'name____string', 'check_attr_type': 'string', 'check_attr_name': 'name', 'rel_query': False, 'condtion_type': 'equals', 'condtion_value': 'Acme'}}
    # Got query: Find Companies that have a name that equals Acme.
    # Got query_type: find



    #codexkg.query()

    # codexkg.delete_db("meowth") # Delete keyspace


if __name__ == "__main__":
    main()
