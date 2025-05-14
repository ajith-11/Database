import pandas as pd

def do_mod(keyword):
    chunks = pd.read_csv(f"{keyword}.csv", chunksize=100000)  
    call_dfs = []
    put_dfs = []
    
    for chunk in chunks:
        call_df = chunk[chunk['symbol'].str[5:].str.contains('C', na=False)]
        put_df = chunk[chunk['symbol'].str[5:].str.contains('P', na=False)]
        call_dfs.append(call_df)
        put_dfs.append(put_df)
    
    call_df_combined = pd.concat(call_dfs)
    put_df_combined = pd.concat(put_dfs)
    
    call_df_combined.to_csv(f"{keyword}_call.csv", index=False)
    put_df_combined.to_csv(f"{keyword}_put.csv", index=False)

do_mod("active_df")