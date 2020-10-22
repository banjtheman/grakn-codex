import logging
import pandas as pd
from codex import CodexKg



logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)

def main():

    logging.info("This will highlight how we can use codex to create knowledge graphs")
    codexkg = CodexKg()

    
    codexkg.create_db("tech_example")
    # loading_data(codexkg)
    # gen_qs(codexkg)
    # search_data(codexkg)


    query_list = []
    query_object = {}
    query_object["query"] = "Find Companies that have a name that contains"
    query_object["condition"] = "Google"


    print(codexkg.list_queries())


    query_list.append(query_object)

    codexkg.nl_query(query_list)



def gen_qs(codexkg):

    logging.info("Well here comes the pain")
    #codexkg.generate_queries("Company","Entity",True)
    codexkg.generate_queries("Product","Entity",True)


def search_data(codexkg):


    
    #codexkg.gen_queries() - > create all queries 
    #codexkg.show_queries() - > list of all queries
    #codexkg.nlquery("Find Companies that have a name that contains Google.")


    df = pd.read_csv("sample_data/tech_companies.csv")
    ans = df.loc[df['name'] == "Google"]

    logging.info(ans)







def loading_data(codexkg):




    #load data from csv
    tech_companies = pd.read_csv("sample_data/tech_companies.csv")
    tech_products = pd.read_csv("sample_data/tech_products.csv")



    #create entites
    codexkg.create_entity(tech_companies, "Company", "name")
    codexkg.create_entity(tech_products, "Product", "name")



    # codexkg.delete_db("tech_example") # Delete keyspace



if __name__ == "__main__":
    main()