from .base import Pipeline
import pandas as pd


class FactorPipeline(Pipeline):
    """
    Create a pipeline from a factor
    """
    def __init__(self, factor):
        self.factor = factor

    def apply(self, df):
        return self.factor.apply(df)

    def step(self, series: pd.Series) -> pd.Series:
        return self.factor.step(series)


class UnionPipeline(Pipeline):
    def __init__(self, pipelines: list):
        self._pipelines = pipelines

    def apply(self, df):
        for p in self._pipelines:
            df = p.apply(df)
        return df

    def step(self, series: pd.Series) -> pd.Series:
        for p in self._pipelines:
            series = p.step(series)
        return series


class MultiFactorPipeline(Pipeline):
    def __init__(self, factors):
        self._factors = factors

    def apply(self, df):
        for factor in self._factors:
            df = factor.apply(df)
        return df

    def step(self, series: pd.Series) -> pd.Series:
        for factor in self._factors:
            series = factor.apply(series)
        return series
