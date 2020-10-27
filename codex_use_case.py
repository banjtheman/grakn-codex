import logging
import pandas as pd
from codex import CodexKg

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


# good use case can be smart tvs...
# they have lots of different content
# we want to use nlq to find certain content


# example entity_map


# entity_map = {}
# entity_map["Company"] = ["company","companies"]
# entity_map["Product"] = ["product","products"]

# attr_list = ["name","budget"]


# codex_actions_map = {}
# codex_actions_map["Find"] = ["find","get","show","list"]

# codex_action_keys = list(codex_actions_map.keys())


# cols = ["name","budget","produces"]


def make_rules(codexkg):

    # Find Company that has a name equal to google
    cond1 = codexkg.rule_condition(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
    )

    # Find Products that has a name that contains google
    cond2 = codexkg.rule_condition(
        concept="Product",
        concept_attrs=["name"],
        concept_conds=["contains"],
        concept_values=["Google"],
    )

    rule_name = "_is_a_good_Google_Product"

    ans = codexkg.make_rule(cond1, cond2, rule_name)
    logging.info(ans)


def search_for_rule(codexkg):

    ans = codexkg.search_rule("_is_a_good_Google_Product")
    logging.info(ans)


def not_query(codexkg):

    # Find Company that has a name equal to google
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["not equals"],
        concept_values=["Google"],
        rel_actions=["producer"],
        concept_rels=["Product"],
        concept_rel_attrs=[["product_type"]],
        concept_rel_conds=[["not equals"]],
        concept_rel_values=[["phone"]],
    )

    logging.info(ans)


def quick_search(codexkg):

    ans = codexkg.cluster(
        cluster_action="centerality", action="degree", cluster_type="All"
    )

    logging.info(ans)

    ans = codexkg.cluster(
        cluster_action="centerality",
        action="degree",
        cluster_type="Subgraph",
        cluster_concepts=["Product", "Company", "Productize"],
    )

    logging.info(ans)

    ans = codexkg.cluster(
        cluster_action="centerality",
        action="degree",
        cluster_type="Subgraph",
        cluster_concepts=["Product", "Company", "Productize"],
        given_type="Company",
    )

    logging.info(ans)

    ans = codexkg.cluster(cluster_action="centerality", action="k-core", k_min=2)

    logging.info(ans)

    ans = codexkg.cluster(
        cluster_action="cluster",
        action="k-core",
        cluster_concepts=["Product", "Company", "Productize"],
        k_min=2,
    )

    logging.info(ans)

    ans = codexkg.cluster(
        cluster_action="cluster",
        action="connected",
        cluster_concepts=["Product", "Company", "Productize"],
    )

    logging.info(ans)

    ans = codexkg.compute(
        actions=["Sum", "Count"],
        concepts=["Company", "Product"],
        concept_attrs=["budget", ""],
    )

    logging.info(ans)

    # Find all companies
    ans = codexkg.find("Company")

    logging.info(ans)

    # Find Company that has a name equal to google
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
    )

    logging.info(ans)

    ans = codexkg.find(
        concept="Company",
        rel_actions=["producer"],
        concept_rels=["Product"],
        concept_rel_attrs=[["name", "product_type"]],
        concept_rel_conds=[["equals", "equals"]],
        concept_rel_values=[["Pixel", "phone"]],
    )

    logging.info(ans)

    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name", "budget"],
        concept_conds=["equals", "greater than"],
        concept_values=["Google", 100],
        rel_actions=["producer"],
        concept_rels=["Product"],
        concept_rel_attrs=[["name", "product_type"]],
        concept_rel_conds=[["equals", "equals"]],
        concept_rel_values=[["Pixel", "phone"]],
        with_rel_attrs=[["note"]],
        with_rel_conds=[["contains"]],
        with_rel_values=[["pixel"]],
    )

    logging.info(ans)

    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name", "budget"],
        concept_conds=["equals", "greater than"],
        concept_values=["Google", 100],
    )

    logging.info(ans)


def main():

    logging.info("This will highlight how we can use codex to create knowledge graphs")
    codexkg = CodexKg()

    codexkg.create_db("tech_example")

    not_query(codexkg)

    # make_rules(codexkg)
    # search_for_rule(codexkg)
    # loading_data(codexkg)
    # quick_search(codexkg)


def search_data(codexkg):

    # codexkg.gen_queries() - > create all queries
    # codexkg.show_queries() - > list of all queries
    # codexkg.nlquery("Find Companies that have a name that contains Google.")

    df = pd.read_csv("sample_data/tech_companies.csv")
    ans = df.loc[df["name"] == "Google"]

    logging.info(ans)


def delete_keyspace(codexkg):
    codexkg.delete_db("tech_example")  # Delete keyspace


def loading_data(codexkg):

    # load data from csv
    tech_companies = pd.read_csv("sample_data/tech_companies.csv")
    tech_products = pd.read_csv("sample_data/tech_products.csv")
    company_products = pd.read_csv("sample_data/tech_products_rel.csv")

    # create entites
    codexkg.create_entity(tech_companies, "Company", "name")
    codexkg.create_entity(tech_products, "Product", "name")

    # create rels
    codexkg.create_relationship(company_products, "Productize", "Product", "Company")


if __name__ == "__main__":
    main()
