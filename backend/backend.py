import abc
import pandas as pd


class Backend(abc.ABC):
    """
    an abstract class for backend implimentation, it's only for binance for now
    """

    @abc.abstractmethod
    def get_historical_data(self, ticker: str, freq: str, start: pd.datetime, end: pd.datetime = None) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def get_tick_data(self):
        pass

    @abc.abstractmethod
    def download(self, ticker: str, freq: str, start: pd.datetime, end: pd.datetime = None) -> pd.DataFrame:
        pass


