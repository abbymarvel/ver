import duckdb

conn = duckdb.connect('/Users/abbymarvel/Desktop/Thesis/CSL/ver/profiles')

df = conn.execute("SELECT * FROM fts_data").fetchdf()
df.to_csv('/Users/abbymarvel/Desktop/Thesis/CSL/ver/fts_data.csv', index=False)

# Close the connection
conn.close()
