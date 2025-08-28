import pandas as pd
import re
from difflib import SequenceMatcher

class DataProcessor:
    """
    Stateless utility class for processing invoice descriptions:
    - Clean text
    - Cluster descriptions
    - Extract representatives
    """

    # ------------------------------
    # Preprocessing
    # ------------------------------
    @staticmethod
    def clean_text(text):
        text = text.lower()
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def similar(a, b, threshold=0.8):
        return SequenceMatcher(None, a, b).ratio() >= threshold
    
    @staticmethod
    def add_id(df):
        df["ID"] = range(len(df))
        return df

    # ------------------------------
    # Clustering
    # ------------------------------
    @classmethod
    def sequential_cluster(cls, df, threshold=0.8):
        """
        Sequentially cluster descriptions within each company.
        Returns a dataframe with a new 'cluster' column.
        """
        df = df.copy()
        print("Initial dataframe shape:")
        print(df.shape)
        df = df.sort_values(by=["P_IVA", "DESCRIZIONE"]).reset_index(drop=True)
        df["cluster"] = -1

        for _, group in df.groupby("P_IVA"):
            prev_desc = None
            cluster_id = 0

            for idx in group.index:
                current_desc = cls.clean_text(df.loc[idx, "DESCRIZIONE"])

                if prev_desc is None:
                    df.loc[idx, "cluster"] = cluster_id
                else:
                    if cls.similar(prev_desc, current_desc, threshold):
                        df.loc[idx, "cluster"] = cluster_id
                    else:
                        cluster_id += 1
                        df.loc[idx, "cluster"] = cluster_id

                prev_desc = current_desc
        print("Dataframe shape after clustering:")
        print(df.shape)
        return df

    # ------------------------------
    # Representatives
    # ------------------------------
    @staticmethod
    def representatives(df):
        """
        Return one representative row per (P_IVA, cluster) from the original dataframe.
        Preserves all original columns.
        """
        # Pick the first index of each (P_IVA, cluster) group
        idx = df.groupby(["P_IVA", "cluster"]).head(1).index
        print("Dataframe shape after selecting representatives:")
        print(df.loc[idx].shape)
        return df.loc[idx].reset_index(drop=True)


    @staticmethod
    def split_into_batches(df, batch_size=20):
        """
        Split a dataframe into batches of given size.
        """
        n = len(df)
        return [
            df.iloc[i*batch_size:(i+1)*batch_size]
            for i in range((n + batch_size - 1) // batch_size)
        ]