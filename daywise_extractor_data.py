import pandas as pd
import zipfile
import os

# Input ZIP file and output CSV file
input_zip = 'GFDLNFO_BACKADJUSTED_08012025.ZIP'  # Change this to your ZIP file name
output_file = 'nifty_I_II_merged.csv'

# Unzip the file and find the CSV
with zipfile.ZipFile(input_zip, 'r') as zip_ref:
    zip_ref.extractall('.')
    csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError('No CSV file found in the ZIP archive!')
    input_file = csv_files[0]

# List of exact tickers to extract
nifty_tickers = ['NIFTY-I.NFO', 'NIFTY-II.NFO']

# Read the CSV file in chunks to handle large file
chunk_size = 100000
filtered_data = []

for chunk in pd.read_csv(input_file, chunksize=chunk_size):
    filtered_chunk = chunk[chunk['Ticker'].isin(nifty_tickers)]
    filtered_data.append(filtered_chunk)

if filtered_data:
    df = pd.concat(filtered_data, ignore_index=True)
    # Split into two DataFrames
    df_I = df[df['Ticker'] == 'NIFTY-I.NFO'].copy()
    df_II = df[df['Ticker'] == 'NIFTY-II.NFO'].copy()
    # Merge on Date and Time
    merged = pd.merge(
        df_I,
        df_II,
        on=['Date', 'Time'],
        suffixes=('_I', '_II'),
        how='left'
    )
    # Prepare column order: Date, Time, NIFTY-I columns with _I, NIFTY-II columns with _II
    base_cols = ['Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']
    cols_I = [col + '_I' for col in base_cols]
    cols_II = [col + '_II' for col in base_cols]
    final_cols = ['Date', 'Time'] + cols_I + cols_II
    # Only keep columns that exist in merged
    final_cols = [col for col in final_cols if col in merged.columns]
    merged = merged[final_cols]
    # Optionally, rename 'Open Interest' to 'OpenInterest' for consistency
    merged = merged.rename(columns={'Open Interest_I': 'OpenInterest_I', 'Open Interest_II': 'OpenInterest_II'})
    # Save to CSV
    merged.to_csv(output_file, index=False)
    print(f"Merged data saved to {output_file}")
    print(merged.head(10))
else:
    print("No matching data found in the input file.") 