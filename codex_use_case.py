import logging
import pandas as pd
from codex import CodexKg


import spacy
# nlp = spacy.load("en_core_web_sm")
from spacy import displacy



logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


# good use case can be smart tvs...
# they have lots of different content
# we want to use nlq to find certain content


parser = spacy.load('en_core_web_sm', disable=['ner','textcat'])

from spacy_example import findCodexQuery





#example entity_map


entity_map = {}
entity_map["Company"] = ["company","companies"]
entity_map["Product"] = ["product","products"]

attr_list = ["name","budget"]


codex_actions_map = {}
codex_actions_map["Find"] = ["find","get","show","list"]

codex_action_keys = list(codex_actions_map.keys())


cols = ["name","budget","produces"]


#its messy but got it to work
def use_spacy(codexkg):

    #we need the lookup

    # print(codexkg.lookup_map)

    sentence = "Find companies with a budget that equals 100"
    
    parse = parser(sentence)
    displacy.render(parse, style="dep")
    codex_query = []

    action_found = False
    entity_found = False
    attr_found = False
    get_cond = False

    cur_action = ""
    curr_ent = ""
    prev_word =""
    action_word = ""
    for word in parse:
        print(f'{word.text:{12}} {word.pos_:{10}} {word.tag_:{8}} {spacy.explain(word.tag_)}  {word.head}')

        #check if word is in codex actions

        #Get the action
        if not action_found:
            for key in codex_action_keys:
                if str(word.text).lower() in codex_actions_map[key]:
                    codex_query.append(key)        
                    action_found = True
                    cur_action = key
                    break



        #Get the entity
        if not entity_found:
            ent_keys = list(entity_map.keys())

            for ent_key in ent_keys:
                if str(word.text).lower() in entity_map[ent_key]:
                    codex_query.append(ent_key)
                    entity_found = True
                    cur_ent = ent_key
                    break                    


        #Get the attribute
        if not attr_found:

            curr_word = str(word.text).lower()

            if curr_word in cols:
                codex_query.append(curr_word)
                attr_found = True

                attr_actions = list(codexkg.lookup_map[cur_action][cur_ent][curr_word].keys())


                #print rights
                rights = [tok for tok in word.rights]
                print("ATTR FOUND")
                print(curr_word)

                print(rights)

                for right in rights:

                    for action in attr_actions:
                        if str(right).lower() in action.lower():
                            codex_query.append(action)

                            print(action)

                            attr_found = True
                            #TODO better way?
                            if action == "Equals":
                                prev_word = "equal"
                                action_word = str(right)

                                print("setting prev word")

                            if action == "Contains":
                                prev_word = "contains"
                                action_word = str(right)

                                print("setting prev word")


                            if action == "Greater Than":
                                prev_word = "greater"
                                action_word = str(right)

                                print("setting prev word")



                            if action == "Less Than":
                                prev_word = "less"
                                action_word = str(right)

                                print("setting prev word")


                            # print(attr_actions)
                            # action_rights = [tok for tok in right.rights]

                            # print(action_rights)


                            #get ri
            

            # if str(word)
        


        elif get_cond and prev_word == "less":
            prev_word = "than"
            continue

        if get_cond:
            codex_query.append(str(word))

        elif attr_found and str(word) == "less":
            codex_query.append("Less than")

            get_cond = True
            prev_word = "less"
            continue









        elif prev_word == "equal" and attr_found and str(word) == "to":
            print("SETING TO HER")
            get_cond = True
            prev_word = "to"
            continue



        elif prev_word == "contains" and attr_found and str(word) == "contains":
            print("SETING TO HER")
            get_cond = True
            prev_word = "to"
            continue



        elif prev_word == "greater" and attr_found and str(word) == "than":
            print("SETING THAN HER")
            get_cond = True
            prev_word = "than"
            continue


        elif prev_word == "less" and attr_found and str(word) == "than":
            print("SETING THAN HER")
            get_cond = True
            prev_word = "than"
            continue



        elif action_word == str(word) and action_word != prev_word:

            get_cond = True
            print("got cond")
            print(str(word))
            continue
        

            # if prev_word == "equal" and attr_found and str(word) == "to":
            #     get_cond = True
            #     prev_word = "to"
            #     continue
            # elif prev_word == "equal" and attr_found :
            #     codex_query.append(str(word))

        # else:
        #     #this must be it
        #     codex_query.append(str(word))


        

            # if prev_word == "to" and get_cond:
            #     codex_query.append(str(word))

            



    print(codex_query)

 
    #print(findCodexQuery(parse,entity_map))







def main():


    logging.info("This will highlight how we can use codex to create knowledge graphs")
    codexkg = CodexKg()

    codexkg.create_db("tech_example")


    use_spacy(codexkg)
    return



    

    # loading_data(codexkg)
    # gen_qs(codexkg)
    # search_data(codexkg)

    print(codexkg.lookup_map["Find"]["Company"]["name"])


    query_list = []
    query_object = {}
    query_object["query"] = "find companies that have a name equal to Google"
    query_object["condition"] = "Google"

    #Find Companies that have a name that Equals CODEX_REPLACE.
    #Find Companies named Google.

    #Find Companies with a budget less than 500.
    # doc = nlp("Find companies that have a name equal to Google")


    # find coffee open - failed
    # find coffee shops that are open - worked

    # find companies that have a name equal to Google


    #look for Companies that have a budget equal to 100."
    # look for this in sentence

    # VERB - action we are doing
    # NOUN - entity we want
    # NOUN - attribute we want
    # ADJ/VERB  - Condition
    # NUM - Value of condtion

    for word in doc:
        print(f'{word.text:{12}} {word.pos_:{10}} {word.tag_:{8}} {spacy.explain(word.tag_)}')



    # print(codexkg.list_queries())


    # query_list.append(query_object)

    # codexkg.nl_query(query_list)



def gen_qs(codexkg):

    logging.info("Well here comes the pain")
    codexkg.generate_queries("Company","Entity",True)
    # codexkg.generate_queries("Product","Entity",True)


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