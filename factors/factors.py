from abc import ABC
import pandas as pd
from .base import BaseFactor
from collections import defaultdict


class Factor(BaseFactor, ABC):
    def __init__(self):
        self.name = None
        self._history = None
        self._periods = None

    def rename(self, name):
        self.name = name
        return self

    @property
    def history(self):
        if not hasattr(self, '_history'):
            self._history = None
        return self._history

    @history.setter
    def history(self, d):
        self._history = d

    @property
    def periods(self):
        return self._periods

    @periods.setter
    def periods(self, value):
        self._periods = value

    def update_history(self, series, new_value):
        self.history['data_history'] = self.history['data_history'].append(series).iloc[1:]
        self.history['factor_history'] = self.history['factor_history'].append(new_value).iloc[1:]

    def apply(self, df: pd.DataFrame) -> pd.Series:
        values = self.func(df)
        if self.periods is not None:
            self.history = {"data_history": df.iloc[-self.periods:],
                            'factor_history': values.iloc[-self.periods:]}
        return values

    def step(self, series: pd.Series):
        df = self.history['data_history']
        updated = df.append(series)
        value = self.func(updated).iat[-1]
        to_return = pd.Series([value], name=self.name)
        self.update_history(series, to_return)
        return series

    def __add__(self, other):
        if isinstance(other, Factor):
            name = f'({self.name} + {other.name})'
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

    def __init__(self, func, name, periods):
        super(Factor, self).__init__()
        self.name = name
        self._func = func
        self.periods = periods

    def func(self, df):
        return self._func(df)


class MovingAverage(Factor):
    __slots__ = ['periods', '_on', 'name', '_history']

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
        to_return = pd.Series([value], name=self.name)
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
