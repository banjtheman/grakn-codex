from codex import CodexKg


def main():
    codexkg = CodexKg()
    codexkg.create_db("meowth")
    codexkg.create_entity("sample_data/company.csv", "Company", "name")
    codexkg.create_entity("sample_data/sample.csv", "Product", "name")

    codexkg.create_relationship(
        "sample_data/company_sample.csv", "Proudctize", "Company", "Product"
    )

    # codexkg.delete_db("meowth") # Delete keyspace


if __name__ == "__main__":
    main()
