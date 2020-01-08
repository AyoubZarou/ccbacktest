from .base import Pipeline
import pandas as pd
from typing import List, Dict
from ccbacktest.utils.pandas_utils import concat_dataframes, concat_series, add_parent_level


def _union_dicts(dicts: List[dict]) -> dict:
    """
    create a union from an iterable of dictionaries, without changing the undelying 
    objects
    """

    keys = [key for d in dicts for key in d.keys()]
    values = [value for d in dicts for value in d.values()]
    return dict(zip(keys, values))


class FactorPipeline(Pipeline):
    """
    Create a pipeline from a factor
    """

    def __init__(self, factor):
        self.factor = factor
        self._name = self.factor.name

    def apply(self, df) -> Dict[str, dict]:
        return_df = self.factor.apply(df)
        return return_df

    def step(self, series: pd.Series) -> Dict[str, dict]:
        return_series = self.factor.step(series)
        return return_series


class UnionPipeline(Pipeline):
    """
     A class created to represent the union of different pipelines   
    """

    def __init__(self, pipelines: List[Pipeline], name: str):
        self._pipelines = pipelines
        self._name = name

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        dfs = [p.apply(df) for p in self._pipelines]
        return_df = concat_dataframes(dfs)
        return_df.columns = add_parent_level(return_df.columns, name=self.name)
        return return_df

    def step(self, series: pd.Series) -> pd.Series:
        all_series = [p.step(series) for p in self._pipelines]
        return_series = concat_series(all_series)
        return_series.index = add_parent_level(return_series.index, name=self.name)
        return return_series


class MultiFactorPipeline(Pipeline):
    def __init__(self, factors, name):
        self._name = name
        self._factors = factors

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        dfs = [f.apply(df) for f in self._factors]
        return_df = concat_dataframes(dfs)
        return_df.columns = add_parent_level(return_df.columns, name=self.name)
        return return_df

    def step(self, series: pd.Series) -> pd.Series:
        all_series = [f.step(series) for f in self._factors]
        return_series = concat_series(all_series)
        return_series.index = add_parent_level(return_series.index, name=self.name)
        return return_series
