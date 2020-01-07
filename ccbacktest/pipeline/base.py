import abc
import pandas as pd


class Pipeline(abc.ABC, object):
    @abc.abstractmethod
    def apply(self, df:pd.DataFrame) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def step(self, series: pd.Series) -> pd.Series:
        pass
    
    @abc.abstractmethod
    @property
    def name(self):
      return self._name
