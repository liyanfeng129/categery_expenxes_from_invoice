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
        df = df.sort_values(by=["RAGIONE_SOCIALE", "DESCRIZIONE"]).reset_index(drop=True)
        df["cluster"] = -1

        for company, group in df.groupby("RAGIONE_SOCIALE"):
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

        return df

    # ------------------------------
    # Representatives
    # ------------------------------
    @staticmethod
    def representatives(df):
        """
        Return one representative description per (company, cluster).
        """
        representatives = (
            df.groupby(["RAGIONE_SOCIALE", "cluster"])["DESCRIZIONE"]
              .first()
              .reset_index()
        )
        return representatives