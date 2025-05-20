import pandas as pd
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

# List of input CSV files
input_files = [
    'nifty_I_II_merged.csv',
    'nifty_I_II_merged_25.csv',
    'nifty_I_II_merged_251.csv'
]

output_file = 'combined_nifty_data.csv'

log_message("Starting CSV combination process...")

# Read and combine all CSV files
all_data = []
for file in input_files:
    log_message(f"Reading file: {file}")
    try:
        df = pd.read_csv(file)
        log_message(f"Successfully read {len(df)} rows from {file}")
        all_data.append(df)
    except Exception as e:
        log_message(f"Error reading {file}: {str(e)}")

if all_data:
    log_message("Combining all data...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Remove any duplicate rows based on Date, Time, and Ticker columns
    log_message("Removing duplicates...")
    combined_df = combined_df.drop_duplicates(subset=['Date', 'Time', 'Ticker_I', 'Ticker_II'])
    
    # Format the final dataframe
    log_message("Formatting final data...")
    combined_df = format_dataframe(combined_df)
    
    log_message(f"Total records in final dataset: {len(combined_df)}")
    
    # Save with proper formatting
    log_message(f"Saving combined data to {output_file}...")
    combined_df.to_csv(output_file, index=False, float_format='%.2f')
    log_message("Data successfully saved!")
    
    # Display summary statistics
    log_message("\nSummary Statistics:")
    print("\nFirst 5 rows of the final dataset:")
    print(combined_df.head().to_string())
    print("\nDataset Info:")
    print(combined_df.info())
    print("\nBasic Statistics:")
    print(combined_df.describe().to_string())
    
    # Display date range
    log_message("\nDate Range in the dataset:")
    print(f"Start Date: {combined_df['Date'].min()}")
    print(f"End Date: {combined_df['Date'].max()}")
else:
    log_message("No data found in any input file.") 