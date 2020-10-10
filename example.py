from codex import CodexKg


def main():
    codexkg = CodexKg()
    codexkg.create_db("meowth")
    # #codexkg.get_entites_grakn()
    # codexkg.create_entity("sample_data/company.csv", "Company", "name")
    # codexkg.create_entity("sample_data/sample.csv", "Product", "name")

    # codexkg.create_relationship(
    #     "sample_data/company_sample.csv", "Proudctize", "Company", "Product"
    # )

    print("")
    print(codexkg.entity_map)
    print(codexkg.rel_map)

    # codexkg.delete_db("meowth") # Delete keyspace


if __name__ == "__main__":
    main()
