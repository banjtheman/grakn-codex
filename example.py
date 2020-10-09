from codex import CodexKg


def main():
    codexkg = CodexKg()
    codexkg.create_db("meowth")
    codexkg.create_entity("sample_data/company.csv", "Company")

    #codexkg.delete_db("meowth") # Delete keyspace


if __name__ == "__main__":
    main()
