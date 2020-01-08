from abc import ABC
import pandas as pd
from .base import BaseFactor
from collections import defaultdict
from ccbacktest.utils.pandas_utils import concat_dataframes, concat_series, add_parent_level


class Factor(BaseFactor, ABC):
    """
    Base class for all factors, it supports arithmetic operations with a scaler or another factor, creating
    a Lambda Factor (a class to represent factors created from a function)
    """

    def __init__(self):
        self.name = None
        self._history = None
        self._periods = None

    def rename(self, name):
        """
        Rename the factor to another name
        :arg name: the new name for the factor"""
        # TODO : raise Exception when the factor has already been applied to a dataset (renaming it would
        # result in creating two different pandas columns with different names)
        self.name = name
        return self

    @property
    def history(self) -> dict:
        """
        :return: return the relevant history for the factor in order for it to compute futur values of the factor,
        it's sentinel value is None
        """
        if not hasattr(self, '_history'):
            self._history = None
        return self._history

    @history.setter
    def history(self, d):
        self._history = d

    @property
    def periods(self):
        """ returns the number of periods necessary to compute next value
        """
        return self._periods

    @periods.setter
    def periods(self, value):
        self._periods = value

    def update_history(self, series, new_value):
        """ Update the history when a new value is computed,
        :arg series: the new observation (ohlcv current value if ohlcv is used)
        :arg new_value: new computed factor value
        """

        self.history['data_history'] = self.history['data_history'].append(series).iloc[1:]
        self.history['factor_history'] = self.history['factor_history'].append(new_value).iloc[1:]

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ apply the factor to a historical dataset at once
        :arg df: historical data to which we apply the factor
        :return factor values for each timestamp
        """

        values = self.func(df)
        if self.periods is not None:
            self.history = {"data_history": df.iloc[-self.periods:],
                            'factor_history': values.iloc[-self.periods:]}
        if not isinstance(values, pd.DataFrame):
            values = values.rename(self.name).to_frame()
        return values

    def step(self, series: pd.Series) -> pd.Series:
        """ compute the factor value for the next timestamp
        :arg series: new observation
        :return computed factor value for the observation
        """

        df = self.history['data_history']
        updated = df.append(series)
        value = self.func(updated).iat[-1]
        to_return = pd.Series([value], name=series.name, index=[self.name])
        self.update_history(series, to_return)
        return series

    def __add__(self, other):
        if isinstance(other, Factor):
            # compute a default name for the su factor
            name = f'({self.name} + {other.name})'
            # the necessary history is the max of both histories
            # TODO: raise exception if the two factors have different timeframes
            periods = max(self.periods, other.periods)

            def func(df):
                return self.func(df) + other.func(df)

            return LambdaFactor(func, name, periods)
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({self.name} + {other})"
            periods = self.periods

            def func(df):
                return other + self.func(df)

            return LambdaFactor(func, name, periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __radd__(self, other):
        if isinstance(other, Factor):
            return other.__add__(self)
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({other} + {self.name})"

            def func(df):
                return other + self.func(df)

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __sub__(self, other):
        if isinstance(other, Factor):
            name = f'({self.name} - {other.name})'
            periods = max(self.periods, other.periods)

            def func(df):
                return self.func(df) - other.func(df)

            return LambdaFactor(func, name, periods)
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({self.name} - {other})"

            def func(df):
                return self.func(df) - other

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __rsub__(self, other):

        if isinstance(other, int) or isinstance(other, float):
            name = f"({other} - {self.name})"

            def func(df):
                return other - self.func(df)

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __mul__(self, other):
        if isinstance(other, Factor):
            name = f'({self.name} x {other.name})'

            def func(df):
                return self.func(df) * other.func(df)

            return LambdaFactor(func, name, max(self.periods, other.periods))
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({self.name} x {other})"

            def func(df):
                return other * self.func(df)

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __rmul__(self, other):
        if isinstance(other, Factor):
            name = f'({other.name} x {self.name})'

            def func(df):
                return self.func(df) * other.func(df)

            return LambdaFactor(func, name, max(self.periods, other.periods))
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({other} + {self.name})"

            def func(df):
                return other * self.func(df)

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __truediv__(self, other):
        if isinstance(other, Factor):
            name = f'({self.name} / {other.name})'

            def func(df):
                return self.func(df) / other.func(df)

            return LambdaFactor(func, name, max(self.periods, other.periods))
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({self.name} / {other})"

            def func(df):
                return self.func(df) / other

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")

    def __rtruediv__(self, other):
        if isinstance(other, Factor):
            name = f'({other.name} / {self.name})'

            def func(df):
                return other.func(df) / self.func(df)

            return LambdaFactor(func, name, max(self.periods, other.periods))
        elif isinstance(other, int) or isinstance(other, float):
            name = f"({other} / {self.name})"

            def func(df):
                return other / self.func(df)

            return LambdaFactor(func, name, self.periods)
        else:
            raise TypeError(f"Type {type(other)} not supported")


class LambdaFactor(Factor):
    __slots__ = ['name', '_func']
    """ A class used to create a factor from a function, a history depth and a name, the 
    rest of the factor operation is inherited from the  Factor class, it's used mainly to create arithmetic operations
    between other factors. 
    """
    def __init__(self, func, name, periods):
        super(Factor, self).__init__()
        self.name = name
        self._func = func
        self.periods = periods

    def func(self, df):
        return self._func(df)


class MovingAverage(Factor):
    __slots__ = ['periods', '_on', 'name', '_history']
    """ Moving averge on price data
    """
    def __init__(self, periods, on="close"):
        assert isinstance(periods, int), "Periods parameter should be an integer"
        assert (on in ['close', 'open']), 'Only open and close are supported'
        super(Factor, self).__init__()
        self.periods = periods
        self._on = on
        self._history = None
        self.name = f'MA_{self.periods}'

    def func(self, df):
        return df.rolling(self.periods)[self._on].mean()

    def step(self, series: pd.Series):
        df = self.history['data_history']
        factor = self.history['factor_history']
        s = factor.iat[-1]
        r = df[self._on].iat[-self.periods]
        value = (self.periods * s - r + series[self._on]) / self.periods
        to_return = pd.Series([value], index=[self.name], name=series.name)
        self.update_history(series, to_return)
        return to_return


class RelativeStrengthIndex(Factor):
    def __init__(self, periods=14):
        self.periods = periods
        self.name = f'RSI_{periods}'
        super(Factor, self).__init__()

    def func(self, df):
        if df.shape[0] <= self.periods:
            raise TooSmallHistoryError('History data frame is too small to compute the moving average with '
                                       f'{self.periods} periods, on {df.shape[0]} time steps')
        diff = df.close - df.open
        pos = diff >= 0
        pos_mean = diff.where(pos, 0).rolling(self.periods).mean()
        neg_mean = -diff.where(~pos, 0).rolling(self.periods).mean()
        factor = (100 - (100 / (1 + (pos_mean / neg_mean)).abs()))
        return factor


class VolumeWeightedAveragePrice(Factor):
    def __init__(self, periods):
        super(Factor, self).__init__()
        self.periods = periods
        self.name = f'VWAP_{self.periods}'

    def func(self, df: pd.DataFrame) -> pd.Series:
        product = df['open'] * df['volume']
        product_sum = product.rolling(self.periods).sum()
        vol_sum = df['volume'].rolling(self.periods).sum()
        return product_sum / vol_sum


class MovingAverageConvergenceDivergence(Factor):
    def __init__(self, fast_period, slow_period, on='close'):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.periods = self.slow_period
        self._on = on
        self.name = f'MACD_{fast_period}_{slow_period}'

    def func(self, df):
        fast_series = df.rolling(self.fast_period)[self._on].mean().rename('fast')
        slow_series = df.rolling(self.slow_period)[self._on].mean().rename('slow')
        return_df = pd.concat([fast_series, slow_series], axis=1)
        return_df.columns = pd.MultiIndex.from_product([[self.name], return_df.columns])
        return return_df

    def step(self, series: pd.Series) -> pd.Series:
        df = self.history['data_history']
        updated = df.append(series)
        value = self.func(updated).iloc[-1, :]
        self.update_history(series, value)
        return value

    def update_history(self, series, new_value):
        self.history['data_history'] = self.history['data_history'].append(series).iloc[1:]
        self.history['factor_history'] = self.history['factor_history'].append(new_value).iloc[1:]


MACD = MovingAverageConvergenceDivergence
MA = MovingAverage
RSI = RelativeStrengthIndex
VWAP = VolumeWeightedAveragePrice


class TooSmallHistoryError(Exception):
    pass
