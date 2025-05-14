import pandas as pd


df1 = pd.read_csv(r'D:\\github\\current_polygon_io_downloader\\NEW SPY backup\\active_df_put.csv')
df2 = pd.read_csv(r'D:\\github\\current_polygon_io_downloader\\NEW SPY backup\\expired_df_put.csv')

combined_df = pd.concat([df1, df2])

combined_df.to_csv('combined_df_put.csv', index=False)