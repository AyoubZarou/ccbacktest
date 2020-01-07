import abc
import pandas as pd


class BaseFactor(abc.ABC):
    @abc.abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def func(self, df: pd.DataFrame) -> pd.Series:
        pass

    @abc.abstractmethod
    def step(self, series: pd.Series):
        pass

