import pandas as pd
import zipfile
import os
from datetime import datetime

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def format_dataframe(df):
    # Sort by Date and Time
    df = df.sort_values(['Date', 'Time'])
    
    # Format numeric columns to 2 decimal places
    numeric_cols = [col for col in df.columns if any(x in col for x in ['Open', 'High', 'Low', 'Close'])]
    for col in numeric_cols:
        df[col] = df[col].round(2)
    
    # Format Volume and OpenInterest as integers
    volume_cols = [col for col in df.columns if 'Volume' in col]
    oi_cols = [col for col in df.columns if 'OpenInterest' in col]
    for col in volume_cols + oi_cols:
        df[col] = df[col].fillna(0).astype(int)
    
    return df

# Input ZIP file and output CSV file
input_zip = '2024.zip'  # Updated to your new ZIP file name
output_file = 'banknifty_I_II_merged_251.csv'

log_message(f"Starting process with input file: {input_zip}")

# Step 1: Unzip the main ZIP file
log_message("Step 1: Extracting main ZIP file...")
with zipfile.ZipFile(input_zip, 'r') as zip_ref:
    log_message(f"Found {len(zip_ref.namelist())} files in main ZIP")
    zip_ref.extractall('.')
log_message("Main ZIP extraction completed")

# Step 2: Recursively unzip all ZIP files found after the first extraction
log_message("Step 2: Starting recursive ZIP extraction...")
zip_count = 0
for root, dirs, files in os.walk('.'):
    for file in files:
        if file.endswith('.zip') and file != input_zip:
            zip_path = os.path.join(root, file)
            log_message(f"Found nested ZIP: {zip_path}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(root)
            zip_count += 1
log_message(f"Completed extraction of {zip_count} nested ZIP files")

# Step 3: Recursively find all CSV files in the extracted directory
log_message("Step 3: Searching for CSV files...")
csv_files = []
for root, dirs, files in os.walk('.'):
    for file in files:
        if file.endswith('.csv') and file != output_file:
            csv_path = os.path.join(root, file)
            csv_files.append(csv_path)
log_message(f"Found {len(csv_files)} CSV files to process")

banknifty_tickers = ['BANKNIFTY-I.NFO', 'BANKNIFTY-II.NFO']
chunk_size = 100000
all_merged = []

# Step 4: Process each CSV file
log_message("Step 4: Processing CSV files...")
for idx, input_file in enumerate(csv_files, 1):
    log_message(f"Processing file {idx}/{len(csv_files)}: {input_file}")
    filtered_data = []
    chunk_count = 0
    
    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        chunk_count += 1
        filtered_chunk = chunk[chunk['Ticker'].isin(banknifty_tickers)]
        filtered_data.append(filtered_chunk)
        if chunk_count % 10 == 0:
            log_message(f"Processed {chunk_count} chunks in current file")
    
    if filtered_data:
        log_message("Merging filtered data...")
        df = pd.concat(filtered_data, ignore_index=True)
        df_I = df[df['Ticker'] == 'BANKNIFTY-I.NFO'].copy()
        df_II = df[df['Ticker'] == 'BANKNIFTY-II.NFO'].copy()
        
        log_message(f"Found {len(df_I)} BANKNIFTY-I records and {len(df_II)} BANKNIFTY-II records")
        
        merged = pd.merge(
            df_I,
            df_II,
            on=['Date', 'Time'],
            suffixes=('_I', '_II'),
            how='left'
        )
        
        base_cols = ['Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']
        cols_I = [col + '_I' for col in base_cols]
        cols_II = [col + '_II' for col in base_cols]
        final_cols = ['Date', 'Time'] + cols_I + cols_II
        final_cols = [col for col in final_cols if col in merged.columns]
        merged = merged[final_cols]
        merged = merged.rename(columns={'Open Interest_I': 'OpenInterest_I', 'Open Interest_II': 'OpenInterest_II'})
        all_merged.append(merged)
        log_message(f"Successfully processed and merged data from {input_file}")
    else:
        log_message(f"No matching data found in {input_file}")

if all_merged:
    log_message("Step 5: Combining all merged data...")
    result = pd.concat(all_merged, ignore_index=True)
    
    # Format the final dataframe
    log_message("Formatting final data...")
    result = format_dataframe(result)
    
    log_message(f"Total records in final dataset: {len(result)}")
    
    # Save with proper formatting
    log_message(f"Saving final data to {output_file}...")
    result.to_csv(output_file, index=False, float_format='%.2f')
    log_message("Data successfully saved!")
    
    # Display summary statistics
    log_message("\nSummary Statistics:")
    print("\nFirst 5 rows of the final dataset:")
    print(result.head().to_string())
    print("\nDataset Info:")
    print(result.info())
    print("\nBasic Statistics:")
    print(result.describe().to_string())
else:
    log_message("No matching data found in any input file.") 