import pandas as pd
import os
import pathlib
import json
import time


def cache_download(backend: str):
    def cache(func):
        def wrapper(self, ticker: str, freq: str, start_str, end_str, format: str = None):
            base, symbol = ticker.split('/')
            directory = 'data/.historical_data/{}/{}'.format(backend, base)
            path = pathlib.Path(directory)
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
            start_str = pd.to_datetime(start_str, format=format)
            start = int(time.mktime(start_str.timetuple())) * 1000
            if end_str is not None:
                end_str = pd.to_datetime(end_str, format=format)
                end = int(time.mktime(end_str.timetuple())) * 1000
            else:
                end = int(time.time() * 1000)
                end_str = pd.to_datetime(int(time.time() * 1000), unit="ms")

            csv_path = os.path.join(directory, f'{symbol}-{freq}.csv')
            json_path = os.path.join(directory, f'{symbol}-{freq}.json')
            if os.path.exists(csv_path) and os.path.exists(json_path):
                df = pd.read_csv(csv_path, dtype={"open_time": 'int64'})
                with open(json_path, 'r') as jf:
                    status = json.load(jf)
                already_downloaded = status['already_downloaded']
                to_download, updated = get_diff_and_update([start, end], already_downloaded)
                dfs = [df]
                for part in to_download:
                    print(part)
                    df = func(self, ticker, freq, *part)
                    dfs.append(df)
                df = pd.concat(dfs)
                df = df.sort_values(by="open_time").drop_duplicates(subset=['open_time'])
                new_end = int(df.open_time.max())
                updated[-1] = new_end
                updated[0] = int(df.open_time.min())
                status['already_downloaded'] = updated
                with open(json_path, 'w') as jf:
                    json.dump(status, jf)
                df.to_csv(csv_path, index=False)
                return df[(start <= df.open_time) & (df.open_time <= end)]
            else:
                df = (func(self, ticker, freq, start, end)
                      .sort_values(by='open_time').drop_duplicates(subset=['open_time']))
                df.to_csv(csv_path, index=False)
                json_file = {"already_downloaded": [int(df.open_time.min()), int(df.open_time.max())]}
                with open(json_path, 'w') as jf:
                    json.dump(json_file, jf)
                return df

        return wrapper

    return cache


# helpers
def get_diff_and_update(v, a):
    assert (len(v) == 2)
    s, e = v
    assert (s < e)
    if len(a) == 0:
        return [v], v
    s_i = 0
    e_i = 0
    for i in range(len(a)):
        if s > a[i]:
            s_i += 1
        if e > a[i]:
            e_i += 1
    if s_i == len(a):
        return [v], a + [s, e]
    found_start = (s_i < len(a) and a[s_i] == s)
    found_end = (e_i < len(a) and a[e_i] == e)
    if found_start and s_i % 2 == 0:
        s_i += 1
    if found_end and e_i % 2 == 0:
        e_i += 1
    if e_i == 0:
        return [v], v + a
    if s_i == e_i:
        if s_i % 2 == 1:
            return [], a
        else:
            return [v], a[:s_i] + v + a[s_i:]
    l = a[s_i:e_i]
    if s_i % 2 == 0:
        l = [s] + l
        prefix = a[:s_i]
        start = s
    else:
        prefix = a[:s_i - 1]
        start = a[s_i - 1]
    if e_i % 2 == 0:
        l = l + [e]
        end = e
        suffix = a[e_i:]
    else:
        end = a[e_i]
        suffix = a[e_i + 1:]

    to_download = [l[2 * i:2 * i + 2] for i in range(len(l) // 2)]
    if len(l) >= 2:
        return to_download, prefix + [start, end] + suffix
    else:
        return to_download, prefix + suffix
