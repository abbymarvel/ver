import duckdb

conn = duckdb.connect('/Users/abbymarvel/Desktop/Thesis/CSL/ver/profiles')

df = conn.execute("SELECT * FROM profiles").fetchdf()
df.to_csv('/Users/abbymarvel/Desktop/Thesis/CSL/ver/profiles.csv', index=False)

conn.close()
