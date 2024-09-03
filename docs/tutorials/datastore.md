---
description: "Presentation of the CoAL data store"
---

# Datastore

## What is the datastore ?

The datastore is an interface to a SQLite database you can use to store and interact with.

The idea behind it is to give you a robust system in which you can send data, query data or even create complex interactions.

Instead of putting all your data in csvs or json or other heavy file formats you can store them in the datastore and easily get them back later.

## Basic example

```python title="Basic use of the datastore"
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import store_pylist

# We initialize and reset the data store
my_datastore = Store(reset=True)

# We create a simple list of dict data
my_data = [{
    "foo": "bar"
},{
    "foo": "barbar"
},{
    "foo": "world"
},{
    "foo": "bar"
}]

# We use a bundled method to send the py_list to the store
store_pylist("my_data", my_data)

# We can make a sql query over our data
# Store.execute_query returns a pyarrow.Table object so we can make use of Table.to_pylist to get an equivalent format
results = my_datastore.execute_query("SELECT foo, count(*) as line_count FROM my_data GROUP BY foo").to_pylist()

# We can print our results now
print(results)
# > [{'foo': 'bar', 'line_count': 2}, {'foo': 'barbar', 'line_count': 1}, {'foo': 'world', 'line_count': 1}]
```