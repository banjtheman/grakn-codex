
# Codex - Programmatic Knowledge Graph Framework

## What is Codex

Codex is a Python package that provides a simple, yet powerful API designed to make working with the [Grakn](https://grakn.ai/) knowledge graph intuitive. 

## Why develop Codex

Building, querying, and interrupting results from the Grakn knowledge graph requires expertise in learning the specific query language graql and learning how to use the verbose API. Codex abstracts away the mundane and tedious aspects of knowledge graphs, so you can focus on organizing your data and get answers. 

## Installing Codex

### Dependencies

The following are dependencies need to properly run Codex

* docker (https://docs.docker.com/get-docker/)
* docker-compose (https://docs.docker.com/compose/install/)
* python3.6+ (https://www.python.org/downloads/)


### Installing Codex

**TODO: Push to pip**   
Codex is compatible with Python 3.6 or later. The simplest way to install Codex and its dependencies is from PyPI with pip, Python's preferred package installer.


```bash
pip install grakn-codex
```

Note that Codex is an active project and routinely publishes new releases. In order to upgrade Codex to the latest version, use pip as follows

```bash
pip install -U grakn-codex
```

Codex can also be installed from source.

```bash
pip install --editable .
```

In addition to the python package, Codex leverages Grakn and the Redis docker images to store data.


### Quick Start

Here's how you can quickly use Codex

```python
import logging
import pandas as pd
from codex import CodexKg 


#Load csv data
df = pd.read_csv("tech_companies.csv")

#Make new codex object
codexkg = CodexKg()

#Create new keyspace
codexkg.create_db("tech_example")

#Load data into Grakn
codexkg.create_entity(df, "Company", entity_key="name")

# Find Company that has a name equal to Google
ans = codexkg.find(
        concept="Company",
        concept_attrs=["name"],
        concept_conds=["equals"],
        concept_values=["Google"],
)

#Display data as a DataFrame
logging.info(ans)

# {'Company':      name  budget
#				0  Google  999.99}
```

For complete documentation on Codex, tutorials and teaching resources, frequently asked questions, and more, please visit our documentation at INSERT_READ_DOCS_URL.

## Contributing to Codex

Codex is an open source project that is supported by a community who will gratefully and humbly accept any contributions you might make to the project. Large or small, any contribution makes a big difference; and if you've never contributed to an open source project before, we hope you will start with Codex!

If you are interested in contributing, check out our contributor's guide. Here are some of the many ways to contribute:

* Submit a bug report or feature request on GitHub Issues.
* Assist us with user testing.
* Add to the documentation or help with our website,
* Write unit or integration tests for our project.
* Answer questions on our issues, mailing list, Stack Overflow, and elsewhere.
* Translate our documentation into another language.
* Write a blog post, tweet, or share our project with others.
* Teach someone how to use Codex.

As you can see, there are lots of ways to get involved and we would be very happy for you to join us! The only thing we ask is that you abide by the principles of openness, respect, and consideration of others as described in the Python Software Foundation Code of Conduct.

TODO: add this
For more information, checkout the [CONTRIBUTING.md](CONTRIBUTING.md) file in the root of the repository.

