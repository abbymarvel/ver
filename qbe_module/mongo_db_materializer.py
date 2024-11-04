from pymongo import MongoClient
from qbe_module.join_path_search import JoinPath
from qbe_module.join_graph_search import JoinGraph, JoinGraphType
import pandas as pd
from collections import deque, defaultdict
import itertools

class MongoMaterializer:
    def __init__(self, mongo_uri: str, database_name: str, tbl_cols, sample_size: int=200):
        self.sample_size = sample_size
        self.tbl_cols = tbl_cols
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]

    def materialize_join_graph(self, join_graph: JoinGraph):
        attrs_needed_map, columns_to_project_lst = join_graph.get_attrs_needed(self.tbl_cols)

        # Single table case (no join needed)
        if join_graph.type == JoinGraphType.NO_JOIN:
            tbl = join_graph.tbl
            df = self.read_mongo_columns_with_sampling(tbl, list(attrs_needed_map[tbl]))
        else:
            graph = join_graph.graph
            graph_dict = join_graph.graph_dict
            start = list(join_graph.graph_dict.keys())[0][0]
            last = start
            node_to_df = {}
            
            # BFS to materialize join paths
            q = deque()
            q.append(start)
            visited = set()
            visited_tbl = set()
            visited.add(start)
            while q:
                cur = q.popleft()
                for nei in graph[cur]:
                    if nei in visited:
                        continue
                    q.append(nei)
                    visited.add(nei)
                    last = nei
                    edge = (cur, nei)
                    join_path = graph_dict[edge]
                    if cur in node_to_df:
                        init_df = node_to_df[cur]
                        df = self.materialize_join_path(join_path, init_df, attrs_needed_map, visited_tbl)
                    else:
                        df = self.materialize_join_path(join_path, None, attrs_needed_map, visited_tbl)
                    node_to_df[cur] = df
                    node_to_df[nei] = df
            
            df = node_to_df[last]

        if len(df) == 0:
            return []
        
        # Project columns for final views
        for columns_to_project in columns_to_project_lst:
            final_attrs_project = list(itertools.product(*columns_to_project))
            
            df_list = []
            for attrs_list in final_attrs_project:
                # Extract only the actual column names (e.g., 'Timestamp')
                column_names = [attr.to_str().split('.')[-1] for attr in attrs_list]
                print("Attempting to project columns:", column_names)
                print("Available columns in DataFrame:", df.columns)
                try:
                    # Attempt column selection with core names only
                    df_list.append(df[column_names])
                except KeyError as e:
                    print(f"KeyError encountered: {e}")

        return df_list

    def read_mongo_columns_with_sampling(self, collection_name, columns):
        """Read specified columns from a MongoDB collection with sampling."""
        collection = self.db[collection_name]
        projection = {col: 1 for col in columns}
        cursor = collection.find({}, projection).limit(self.sample_size)
        return pd.DataFrame(list(cursor))

    def materialize_join_path(self, join_path: JoinPath, init_df, attr_needed_map, visited_tbl):
        path = join_path.path
        
        prv_df = init_df
        for join_pair in path:
            key1, key2 = join_pair[0], join_pair[1]
            tbl1, tbl2 = key1.source_name, key2.source_name

            if key1 == key2:
                if prv_df is not None:
                    return prv_df
                else:
                    attrs_needed = attr_needed_map[tbl1]
                    visited_tbl.add(tbl1)
                    return self.read_mongo_columns_with_sampling(tbl1, list(attrs_needed))
            
            if prv_df is None:
                attrs_needed1 = attr_needed_map[tbl1]
                df1 = self.read_mongo_columns_with_sampling(tbl1, list(attrs_needed1))
                attrs_needed2 = attr_needed_map[tbl2]
                df2 = self.read_mongo_columns_with_sampling(tbl2, list(attrs_needed2))
                prv_df = pd.merge(df1, df2, left_on=self.get_col_name(key1).split('.')[-1], right_on=self.get_col_name(key2).split('.')[-1], how='inner')
                visited_tbl.add(tbl1)
                visited_tbl.add(tbl2)
            else:
                if len(prv_df) == 0:
                    return prv_df
                
                if tbl2 in visited_tbl:
                    continue
                attrs_needed2 = attr_needed_map[tbl2]
                df = self.read_mongo_columns_with_sampling(tbl2, list(attrs_needed2))
                prv_df = pd.merge(prv_df, df, left_on=self.get_col_name(key1), right_on=self.get_col_name(key2), how='inner')
                visited_tbl.add(tbl2)

        return prv_df

    def get_col_name(self, col):
        return "{}.{}".format(col.source_name, col.field_name)