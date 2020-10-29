# Codex Entity

An entity is a thing with a distinct existence in the domain. For example, `organization`, `location` and `person`. The existence of each of these entities is independent of any other concept in the domain."   

Codex makes it very easy to create entities by importing data from a CSV.   

## Importing entities

In this example, we will create a Company entity from the following CSV.   
**Note: These are completely made up values and are for demonstration purposes only**

```
name,budget,next_product_date
Apple,123.45,2020-04-02
Google,999.99,2018-09-02T10:32:11.09
Microsoft,500.00,2017-10-03
Amazon,1000.00,2020-09-02
```

With the help of panadas, we can easily load data into Grakn, and Codex will automatically figure out what data type your column is.
* In this example name is a string  
* budget is a double  
* and next_product_date is a date  

Codex also requires you have a key column for your data. In this case, we will use the name column.
You must also name your entity, for this we will name it Company.

Here is the full example of loading the data.

```python
import pandas as pd
from codex import CodexKg

# Init Codex 
codexkg = CodexKg()

# Connect to keyspace
codexkg.create_db("tech_example")

#load df
tech_companies = pd.read_csv("sample_data/tech_companies.csv")

# create entites
codexkg.create_entity(tech_companies, "Company", "name")
```


You can verify the data was entered by doing a find all query

```python
# Find all companies
ans = codexkg.find("Company")
print(ans)
```




 

