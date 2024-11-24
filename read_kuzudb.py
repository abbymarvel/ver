import kuzu
import pandas as pd

db = kuzu.Database("tmp")
conn = kuzu.Connection(db)

conn.execute("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))")
conn.execute("CREATE (a:Person {name: 'Adam', age: 30})")
conn.execute("CREATE (a:Person {name: 'Karissa', age: 40})")
conn.execute("CREATE (a:Person {name: 'Zhang', age: 50})")

result = conn.execute("MATCH (p:Person) RETURN p.*")
print(result.get_as_df())