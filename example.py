from codex import CodexKg



def main():
    #print("hello codex")
    codexkg = CodexKg()
    codexkg.create_db("meowth")
    codexkg.create_entity("sample_data/company.csv","Company")






if __name__ == "__main__":
    main()