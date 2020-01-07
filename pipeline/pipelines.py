from .base import Pipeline
import pandas as pd
from typing import List, Dict


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
        factor = self.factor
        return {self.name: factor.apply(df)}

    def step(self, series: pd.Series) -> Dict[str, dict]:
        actor = self.factor
        return {self.name: factor.step(series)}


class UnionPipeline(Pipeline):
    """
     A class created to represent the union of different pipelines   
    """
    def __init__(self, pipelines: list, name: str):
        self._pipelines = pipelines
        self._name = name

    def apply(self, df: pd.DataFrame) -> Dict[str, dict]:
      dicts = [p.apply(df) for p in self._pipelines]
      union_dict = _union_dicts(dicts)
      return {self.name: union_dict} 

    def step(self, series: pd.Series) -> Dict[str, dict]:
      dicts = [p.step(df) for p in self._pipelines]
      union_dict = _union_dicts(dicts)
      return {self.name: union_dict} 


class MultiFactorPipeline(Pipeline):
    def __init__(self, factors, name):
        self._name = name
        self._factors = factors

    def apply(self, df: pd.DataFrame) -> Dict[str, dict]:
        dicts = [{factor.name: factor.apply(df)} for factor in self._factors] 
        union_dict = _union_dicts(dicts)
        return {self.name: union_dict}

    def step(self, series: pd.Series) -> Dict[str, dict]:
        dicts = [{factor.name: factor.step(df)} for factor in self._factors] 
        union_dict = _union_dicts(dicts)
        return {self.name: union_dict}
