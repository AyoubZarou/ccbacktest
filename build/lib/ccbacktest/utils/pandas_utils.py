import pandas as pd
import numpy as np


def _add_n_levels(index, n):
    """ Add n empty levels to a multiindex
    """
    nlevels = index.nlevels
    if nlevels == 1:
        new_index = [[i, *[''] * n] for i in index]
    else:
        new_index = [list(old) + [''] * n for old in index]
    return pd.MultiIndex.from_tuples(new_index)


def add_parent_level(index, name):
    nlevels = index.nlevels
    if nlevels == 1:
        new_index = [[name, i] for i in index]
    else:
        new_index = [[name] + list(old) for old in index]
    return pd.MultiIndex.from_tuples(new_index)


def concat_dataframes(dfs):
    """ Concatenate a list of data_frames while creating empty columns levels for 
    data frame with less levels.
    """
    levels = [df.columns.nlevels for df in dfs]
    max_levels = max(levels)
    to_concat = []
    for df_ in dfs:
        df = df_.copy()
        nlevels = df.columns.nlevels
        if nlevels < max_levels:
            df.columns = _add_n_levels(df.columns, max_levels - nlevels)
        to_concat.append(df)
    return pd.concat(to_concat, axis=1)


def concat_series(series_list):
    if len(np.unique([series.name for series in series_list])) != 1:
        raise ValueError('Series should have the same name')
    levels = [series.index.nlevels for series in series_list]
    max_levels = max(levels)
    to_concat = []
    for series in series_list:
        nlevels = series.index.nlevels
        if nlevels < max_levels:
            series.index = _add_n_levels(series.index, max_levels - nlevels)
        to_concat.append(series)
    return pd.concat(to_concat)
