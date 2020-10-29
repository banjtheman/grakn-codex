import logging
import pandas as pd
from codex import CodexKg
import pprint

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


def make_tech_rule(codexkg):

    # Find Company that has a name equal to Google
    cond1 = codexkg.rule_condition(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
    )

    # Find Products that has a name that contains Google
    cond2 = codexkg.rule_condition(
        concept="Product",
        concept_attrs=["name"],
        concept_conds=["contains"],
        concept_values=["Google"],
    )

    rule_name = "Google_Product"

    codexkg.make_rule(cond1, cond2, rule_name)

    ans = codexkg.search_rule("Google_Product")
    pprint.pprint(ans)


def not_query(codexkg):

    # Find Company that has a name not equal to Google and that produces products that are not a phone
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["not equals"],
        concept_values=["Google"],
        rel_actions=["produces"],
        concept_rels=["Product"],
        concept_rel_attrs=[["product_type"]],
        concept_rel_conds=[["not equals"]],
        concept_rel_values=[["phone"]],
    )

    pprint.pprint(ans)


def compute_searches(codexkg):

    # Compute the Sum of Company budget and the Count of Products
    ans = codexkg.compute(
        actions=["Sum", "Count"],
        concepts=["Company", "Product"],
        concept_attrs=["budget", ""],
    )

    pprint.pprint(ans)

    # Compute the Mean of Company budget and the Standard Deviation
    ans = codexkg.compute(
        actions=["Mean", "Standard Deviation"],
        concepts=["Company", "Company"],
        concept_attrs=["budget", "budget"],
    )

    pprint.pprint(ans)


def cluster_searches(codexkg):

    # Find centrality cluster by degree
    ans = codexkg.cluster(
        cluster_action="centrality", action="degree", cluster_type="All"
    )

    pprint.pprint(ans)

    # Find centrality cluster by degree using the Product, Company and Productize concepts
    ans = codexkg.cluster(
        cluster_action="centrality",
        action="degree",
        cluster_type="Subgraph",
        cluster_concepts=["Product", "Company", "Productize"],
    )

    pprint.pprint(ans)

    # Find centrality cluster by degree using the Product, Company and Productize concepts with given type of Company
    ans = codexkg.cluster(
        cluster_action="centrality",
        action="degree",
        cluster_type="Subgraph",
        cluster_concepts=["Product", "Company", "Productize"],
        given_type="Company",
    )

    pprint.pprint(ans)

    # Find centrality cluster by k-core with a k-min of 2
    ans = codexkg.cluster(cluster_action="centrality", action="k-core", k_min=2)

    pprint.pprint(ans)

    # Find a cluster by k-core with a k-min of 2 using the Product, Company and Productize concepts
    ans = codexkg.cluster(
        cluster_action="cluster",
        action="k-core",
        cluster_concepts=["Product", "Company", "Productize"],
        k_min=2,
    )

    pprint.pprint(ans)

    # Find a connected component cluster using the Product, Company and Productize concepts
    ans = codexkg.cluster(
        cluster_action="cluster",
        action="connected",
        cluster_concepts=["Product", "Company", "Productize"],
    )

    pprint.pprint(ans)


def find_searches(codexkg):

    # Find all companies
    ans = codexkg.find("Company")

    pprint.pprint(ans)

    # Find Companies that has a name equal to Google
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
    )

    pprint.pprint(ans)

    # Find Companies that has a name that contains o and a budget greater than 100
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name", "budget"],
        concept_conds=["contains", "greater than"],
        concept_values=["o", 100],
    )

    pprint.pprint(ans)

    # Find Companies that produce a product that have a name equal to Pixel and a product type that equals phone
    ans = codexkg.find(
        concept="Company",
        rel_actions=["produces"],
        concept_rels=["Product"],
        concept_rel_attrs=[["name", "product_type"]],
        concept_rel_conds=[["equals", "equals"]],
        concept_rel_values=[["Pixel", "phone"]],
    )

    pprint.pprint(ans)

    # Find Companies that has a name equal to Google and a budget greater than 100,that produce products that have a name equal to Pixel and a product type that equals phone with a relation with a note that contains pixel.
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name", "budget"],
        concept_conds=["equals", "greater than"],
        concept_values=["Google", 100],
        rel_actions=["produces"],
        concept_rels=["Product"],
        concept_rel_attrs=[["name", "product_type"]],
        concept_rel_conds=[["equals", "equals"]],
        concept_rel_values=[["Pixel", "phone"]],
        with_rel_attrs=[["note"]],
        with_rel_conds=[["contains"]],
        with_rel_values=[["pixel"]],
    )

    pprint.pprint(ans)


def date_query_example(codexkg):

    # Find games released after 2019-08-18
    ans = codexkg.find(
        concept="Game",
        concept_attrs=["date"],
        concept_conds=["after"],
        concept_values=["2019-08-18"],
    )

    pprint.pprint(ans)

    # Find games released before 2019-08-18
    ans = codexkg.find(
        concept="Game",
        concept_attrs=["date"],
        concept_conds=["before"],
        concept_values=["2019-08-18"],
    )

    pprint.pprint(ans)

    # Find games released between 2017-08-18 and 2019-08-04
    ans = codexkg.find(
        concept="Game",
        concept_attrs=["date"],
        concept_conds=["between"],
        concept_values=["2017-08-18 2019-08-04"],
    )

    pprint.pprint(ans)


def date_rule(codexkg):

    # If Game A has the same release date
    cond1 = codexkg.rule_condition(
        concept="Game",
        concept_attrs=["date"],
        concept_conds=["congruent"],
        concept_values=[""],
    )

    # If Game B has the same release date
    cond2 = codexkg.rule_condition(
        concept="Game",
        concept_attrs=["date"],
        concept_conds=["congruent"],
        concept_values=[""],
    )

    # Rule Name
    rule_name = "same_day_release"

    # Create new rule in Grakn
    codexkg.make_rule(cond1, cond2, rule_name)

    # Get results
    ans = codexkg.search_rule("same_day_release")
    pprint.pprint(ans)


def delete_keyspace(codexkg, val):
    codexkg.delete_db(val)  # Delete keyspace


def load_time_data(codexkg):

    games = pd.read_csv("sample_data/example_dates.csv")

    # create entities
    codexkg.create_entity(games, "Game", "game")


def loading_data(codexkg):

    # load data from csv
    tech_companies = pd.read_csv("sample_data/tech_companies.csv")
    tech_products = pd.read_csv("sample_data/tech_products.csv")
    company_products = pd.read_csv("sample_data/tech_products_rel.csv")

    # create entities
    codexkg.create_entity(tech_companies, "Company", "name")
    codexkg.create_entity(tech_products, "Product", "name")

    # create rels
    codexkg.create_relationship(company_products, "Productize", "Product", "Company")


def main():

    pprint.pprint("This will highlight how we can use Codex to create knowledge graphs")

    # Init Codex
    codexkg = CodexKg()

    # Connect to keyspace
    codexkg.create_db("tech_example")

    # Load data
    loading_data(codexkg)

    # Search Example
    find_searches(codexkg)

    # Negation example
    not_query(codexkg)

    # Cluster Example
    cluster_searches(codexkg)

    # Compute Example
    compute_searches(codexkg)

    # Make rules example
    make_tech_rule(codexkg)

    # Delete keyspace
    delete_keyspace(codexkg, "tech_example")

    # Simple example showing off date queries

    # Connect to keyspace
    codexkg.create_db("game_dates")

    # Load data
    load_time_data(codexkg)

    # Do example date queries
    date_query_example(codexkg)

    # Show a congruent rule
    date_rule(codexkg)

    # Delete key space
    delete_keyspace(codexkg, "game_dates")

    pprint.pprint("Done and Done")


if __name__ == "__main__":
    main()
