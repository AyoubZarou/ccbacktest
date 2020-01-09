import pandas as pd
from ccbacktest.utils.pandas_utils import concat_dataframes, concat_series

def _time_frame_to_ms(t):
    tf = pd.to_timedelta(t)
    return tf.value // 1000_000

class DataLoader(object):
    def __init__(self, backend, timeframe='1h', start=None, train_end=None,
                 test_end=None, pipeline=None, format=None, window=30, symbol=None, join_ohlcv=True):
        self._backend = backend
        self._pipeline = pipeline
        self._train_end = train_end
        self._test_end = test_end
        self._timeframe = timeframe
        self._format = format
        self._start = self._parse_time(start)
        self._window = window
        self._symbol = symbol
        self._history_data = None
        self._join_ohlcv = join_ohlcv
        self._ms_step = _time_frame_to_ms(self._timeframe)

    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self, backend):
        self._backend = backend

    @property
    def pipeline(self):
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        self._pipeline = pipeline

    @property
    def train_end(self):
        return self._train_end

    @train_end.setter
    def train_end(self, train_end):
        train_end = self._parse_time(train_end)
        self._train_end = train_end

    @property
    def test_end(self):
        return self._test_end

    @test_end.setter
    def test_end(self, test_end):
        test_end = self._parse_time(test_end)
        self._test_end = test_end

    def train_data(self):
        data = self.backend.download(self._symbol, self._timeframe, self._start, self.train_end)
        if self.pipeline is not None:
            data_apply = self.pipeline.apply(data)
            if self._join_ohlcv:
                data = concat_dataframes([data, data_apply])
            else:
                data = data_apply
        self._history_data = data.iloc[-self._window:, :].copy()
        return data

    def test_data(self):
        if self._history_data is None:
            raise NotTrainedYetError("Train data should be generated first to make a history data")
        data = self.backend.download(self._symbol, self._timeframe, self.train_end, self.test_end)
        for i in range(data.shape[0]):
            series = self.step(data.iloc[i, :])
            if self._join_ohlcv:
                series = concat_series([data.iloc[i, :], series])
            self._update_history(series)
            yield self._history_data.copy()

    def _update_history(self, series: pd.Series):
        data = self._history_data.append(series)[1:]
        self._history_data = data

    def _parse_time(self, t):
        if isinstance(t, int):
            return pd.to_datetime(t, unit="ms")
        elif isinstance(t, str):
            return pd.to_datetime(t, format=self._format)
        else:
            return t

    def step(self, series: pd.Series) -> pd.Series:
        if self.pipeline is not None:
            series = self.pipeline.step(series)
        return series


class NotTrainedYetError(Exception):
    pass
