# Codex - Programmatic Knowledge Graph Framework

Welcome to the Codex wiki! Here you will find out how Codex works and how you can use it.


## What is Codex

Codex is a Python package that provides a simple, yet powerful API designed to make working with the Grakn knowledge graph intuitive.

## How Can I get started?

This section will be a complete step by step guide on how to get Codex to work.

1. First, we need the following dependencies 

* docker (https://docs.docker.com/get-docker/)
* docker-compose (https://docs.docker.com/compose/install/)
* python3.6+ (https://www.python.org/downloads/)

Once all of those are installed, we can clone the repo


2. Clone Repo
```
git clone https://github.com/banjtheman/grakn-codex.git
cd grakn-codex
```

3. Next we need to make folders for our data folders. This is where Grakn and Redis will store data
```
mkdir -p data
mkdir -p redis_data
```

4. We can then bring up the databases using docker-compose
```
docker-compose up -d
```

5.  Next we can install the codex module locally
 
```
pip install --editable .
```


## Your first Codex project

Now that the setup is out of the way, we can now begin using Codex. For this walkthrough we will use the simple data in the sample_data folder of the repo.


1. Init Codex

First, let's make a new python file. Feel free to use your favorite editor (i.e VSCode) 

```
touch example.py
# Open example.py
```

2. Add the imports and main function
```python
import logging
import pandas as pd
from codex import CodexKg

def main():
    print("Codex Example")


if __name__ == "__main__":
    main()
```

3. Create a Codex Object

```python
def main():
    print("Codex Example")

    # Init Codex
    codexkg = CodexKg()

    # Connect to keyspace
    codexkg.create_db("tech_example")
```


4. Next import the sample data as a dataframe, and load into Grakn

```python
def loading_data(codexkg):

    # load data from csv
    tech_companies = pd.read_csv("sample_data/tech_companies.csv")
    tech_products = pd.read_csv("sample_data/tech_products.csv")
    company_products = pd.read_csv("sample_data/tech_products_rel.csv")

    # create entites
    codexkg.create_entity(tech_companies, "Company", entity_key="name")
    codexkg.create_entity(tech_products, "Product", entity_key="name")

    # create rels
    codexkg.create_relationship(company_products, "Productize", "Product", "Company")
```

the `create_entity` function takes in a dataframe, the name you want to call the entity, and the key column  

the `create_relationship` function takes in a dataframe, the name you want to call the relationship and the two entities in the relationship  
**The first two columns in the company_products dataframe must match the corresponding entities. In this example it is produced and produces, so we use Product and then Company.**

Our schema is pretty simple... Companies produce Products.  
Two entities and one relationship, thus 3 CSVs 



5. Once the data is loaded we can do a query

```python
def find_searches(codexkg):

    # Find all companies
    ans = codexkg.find("Company")

    logging.info(ans)

    # Find Companies that has a name equal to Google
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
    )

    logging.info(ans)
```


The final code should look like


```python
import logging
import pandas as pd
from codex import CodexKg

def loading_data(codexkg):

    # load data from csv
    tech_companies = pd.read_csv("sample_data/tech_companies.csv")
    tech_products = pd.read_csv("sample_data/tech_products.csv")
    company_products = pd.read_csv("sample_data/tech_products_rel.csv")

    # create entites
    codexkg.create_entity(tech_companies, "Company", entity_key="name")
    codexkg.create_entity(tech_products, "Product", entity_key="name")

    # create rels
    codexkg.create_relationship(company_products, "Productize", "Product", "Company")

def find_searches(codexkg):

    # Find all companies
    ans = codexkg.find("Company")

    logging.info(ans)

    # Find Companies that has a name equal to Google
    ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
    )

    logging.info(ans)


def main():
    print("Codex Example")

    # Init Codex
    codexkg = CodexKg()

    # Connect to keyspace
    codexkg.create_db("tech_example")
    # load data
    loading_data(codexkg)
    # find data
    find_searches(codexkg)

    logging.info("Done and Done")


if __name__ == "__main__":
    main()

```

You can test simply by running...

```
python example.py
```







































