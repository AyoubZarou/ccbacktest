from .base import Pipeline
import pandas as pd

def _union_dicts(dicts):
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

    def apply(self, df):
        factor = self.factor
        return {self.name: factor.apply(df)}

    def step(self, series: pd.Series) -> pd.Series:
        actor = self.factor
        return {self.name: factor.step(series)}


class UnionPipeline(Pipeline):
    def __init__(self, pipelines: list, name):
        self._pipelines = pipelines
        self._name = name

    def apply(self, df):
      dicts = [p.apply(df) for p in self._pipelines]
      union_dict = _union_dicts(dicts)
      return {self.name: union_dict} 

    def step(self, series: pd.Series) -> pd.Series:
      dicts = [p.step(df) for p in self._pipelines]
      union_dict = _union_dicts(dicts)
      return {self.name: union_dict} 


class MultiFactorPipeline(Pipeline):
    def __init__(self, factors, name):
        self._name = name
        self._factors = factors

    def apply(self, df: pd.DataFrame) -> dict:
        dicts = [{factor.name: factor.apply(df)} for factor in self._factors] 
        union_dict = _union_dicts(dicts)
        return {self.name: union_dict}

    def step(self, series: pd.Series) -> dict:
        dicts = [{factor.name: factor.step(df)} for factor in self._factors] 
        union_dict = _union_dicts(dicts)
        return {self.name: union_dict}
