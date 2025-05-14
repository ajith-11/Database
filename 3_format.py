import glob
import pandas as pd

def get_mod_df(abs_filename):
    strike = ""
    expiry = ""
    try:
        df = pd.read_csv(abs_filename)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    file_path = abs_filename.split("\\")[-1]
    file_path = file_path.replace(" - Copy", "")
    df.drop(columns="otc", inplace=True)
    df.drop(columns="vwap", inplace=True)
    df = df.rename(columns={"transactions": "oi"})
    for i, char in enumerate(file_path):
        if char.isdigit():
            expiry = expiry + char
        if char in ("P", "C") and expiry != "":
            strike = str(int(file_path[i+1:-4]))
            break
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df['date'] = df['timestamp'].dt.strftime('%y%m%d')
    df['time'] = df['timestamp'].dt.hour * 3600 + df['timestamp'].dt.minute * 60 + df['timestamp'].dt.second
    df.drop(columns="timestamp", inplace=True)
    df.drop(columns="datetime", inplace=True)
    df["symbol"] = file_path.removesuffix(".csv")
    df["strike"] = int(strike)
    df["expiry"] = expiry
    df = df["date,time,symbol,strike,expiry,open,high,low,close,volume,oi".split(",")]
    df["open"] = (df["open"] * 100).astype(int)
    df["high"] = (df["high"] * 100).astype(int)
    df["low"] = (df["low"] * 100).astype(int)
    df["close"] = (df["close"] * 100).astype(int)
    df["strike"] = (df["strike"].astype(float) / 1000).astype(int)
    df['coi'] = df['oi'].diff().fillna(0).astype(int)

    df.rename(columns={"date": "date_utc", "time":"time_utc"}, inplace=True)
    df['datetime'] = pd.to_datetime(df['date_utc'], format='%y%m%d') + pd.to_timedelta(df['time_utc'], unit='s')
    df['datetime_GMT'] = df['datetime'].dt.tz_localize('GMT')
    df['datetime_est'] = df['datetime_GMT'].dt.tz_convert('US/Eastern')
    df['date'] = df['datetime_est'].dt.strftime('%y%m%d')
    df['time'] = df['datetime_est'].dt.hour * 3600 + df['datetime_est'].dt.minute * 60 + df['datetime_est'].dt.second

    return df

dfs = pd.DataFrame()
for file in glob.glob(f"active_symm\\*.csv"):
    print(f"trying {file}")
    df = get_mod_df(file)
    if df.empty:
        continue
    if dfs.empty:
        dfs = df
    else:
        dfs = pd.concat([dfs, df])
dfs = dfs[["date","time","symbol","strike","expiry","open","high","low","close","volume","oi","coi"]]
dfs = dfs.sort_values(by=['date', 'time'])
dfs.to_csv(f"active_df.csv", index=False)