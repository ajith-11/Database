import pandas as pd
import glob
import os

# Get all CSV files from the directory
csv_files = glob.glob('nifty_future/nifty_jann-decc-full_data/future/*.csv')

# Create an empty list to store DataFrames
dfs = []

# Read each CSV file and append to the list
for file in csv_files:
    df = pd.read_csv(file)
    # Extract date from filename
    date_str = os.path.basename(file).split('_')[0]
    # Convert date string to datetime
    df['date'] = pd.to_datetime(date_str, format='%Y-%m-%d')
    dfs.append(df)

# Concatenate all DataFrames
merged_df = pd.concat(dfs, ignore_index=True)

# Convert time from seconds to HH:MM:SS format
def seconds_to_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

merged_df['time'] = merged_df['time'].apply(seconds_to_time)

# Sort by date and time
merged_df = merged_df.sort_values(['date', 'time'])

# Convert OHLC columns to rupees and round to two decimals
ohlc_cols = ['open', 'high', 'low', 'close']
merged_df[ohlc_cols] = (merged_df[ohlc_cols] / 100).round(2)

# Save to CSV with a new filename
output_file = 'nifty_future_merged_rounded_v2.csv'
merged_df.to_csv(output_file, index=False)

print(f"All daily data has been merged and saved to '{output_file}' with time in HH:MM:SS format and OHLC rounded to 2 decimals") 