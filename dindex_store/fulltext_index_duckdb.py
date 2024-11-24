from typing import List
from pathlib import Path
import os
from enum import Enum

import duckdb

from dindex_store.common import FullTextSearchIndex


class FTSIndexDuckDB(FullTextSearchIndex):

    def __init__(self, config, load=False, force=False):
        # FIXME: Validate Config Name
        self.config = config
        db_path = Path(config['ver_base_path']) / Path(config['fts_duckdb_database_name'])
        self.conn = duckdb.connect(database=str(db_path))

        self.profile = config["profile_table_name"]
        self.table_name = config["fts_data_table_name"]
        self.index_column = config["fts_index_column"]

        if not load:
            fts_schema_path = Path(config['ver_base_path'] / config['fts_schema_name']).absolute()
            if not fts_schema_path.is_file():
                raise ValueError("The path to fts_schema does not exist, or is not a file")
            with open(fts_schema_path) as f:
                self.schema = f.read()
            try:
                if force:
                    fts_table_name = config["fts_data_table_name"]
                    q = f"DROP TABLE IF EXISTS {fts_table_name};"
                    self.conn.execute(q)
                # if we are building the index then we have to create the schema and the index
                self.conn.execute(self.schema)
                # create fts index on index_column
                self.create_fts_index(self.table_name, self.index_column, force=force)
            except:
                print("An error has occurred when reading the schema")
                raise

    # ----------------------------------------------------------------------
    # Modify Methods

    def create_fts_index(self, table_name, index_column, force=False):
        # Have to manually refresh fts index as per DuckDB's docs: "Note that the FTS index will not update automatically
        # when input table changes. A workaround of this limitation can be recreating the index to refresh."
        # The consequence for now is that force=True always, when this function is called
        force = True
        if force:
            try:
                query = f"PRAGMA drop_fts_index('{table_name}')"
                self.conn.execute(query)
            except duckdb.CatalogException as ce:
                print(f"error when removing an existing fts index: {ce}")

        # Create fts index over all, *, attributes
        query = f"PRAGMA create_fts_index('{table_name}', '{index_column}', '*', stopwords='none', ignore='(\\.|[^a-z0-9]+)')"
        self.conn.execute(query)

    def insert(self, profile_id, dbName, path, sourceName, columnName, data) -> bool:
        try:
            fts_data_table = self.conn.table(self.table_name)
            fts_data_table.insert([profile_id, dbName, path, sourceName, columnName, data])
            return True
        except:
            print("An error has occured when trying to add text data")
            return False

    # ----------------------------------------------------------------------
    # Query Methods
    
    def fts_query(self, keyword, search_domain, max_results, exact_search, threshold=None) -> List:
        domain_mapping = {
            'KWType.KW_CONTENT': 'data',
            'KWType.KW_SCHEMA': 'columnname'
        }

        search_domain_str = domain_mapping.get(str(search_domain))

        if isinstance(keyword, (int, float)):
            threshold = 1.5 * keyword
            # Numerical similarity search
            query = f"""
                SELECT id, dbname, path, sourcename, columnname, ABS(median - {keyword}) AS score
                FROM {self.profile}
                WHERE median BETWEEN {keyword} - {threshold} AND {keyword} + {threshold}
                ORDER BY score ASC
                LIMIT {max_results};
            """
        else:
            # Full-text search (original implementation)
            query = f"""WITH scored_docs AS (
                    SELECT *, fts_main_{self.table_name}.match_bm25(profile_id, '{keyword}', fields := '{search_domain_str}', conjunctive := {1 if exact_search else 0}) 
                    AS score FROM {self.table_name})
                SELECT DISTINCT ON (profile_id) profile_id, dbname, path, sourcename, columnname, score
                FROM scored_docs
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT {max_results};"""

        res = self.conn.execute(query)
        print("\n---fts index---\n")
        print(f"fulltext_index_duckdb.py | TABLE: {self.table_name}, Keyword: {keyword}, Search Domain: {search_domain_str}, Exact Search: {exact_search}")
        return res.fetchall()

