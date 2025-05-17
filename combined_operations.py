import pandas as pd
import mysql.connector
from datetime import datetime
import os
import glob

def seconds_to_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Define date range and paths
start_date = '2022-01-01'
end_date = '2025-05-01'
base_path = r"D:\stocks\raw_stocks\sensex_future"
folder_name = 'sensex_jann-decc-full_data' 
output_folder = os.path.join(base_path, folder_name)

future_folder = os.path.join(output_folder, 'future')
os.makedirs(future_folder, exist_ok=True)

print(f"Main output folder: {output_folder}")
print(f"Future data will be saved in: {future_folder}")

# Generate date range
date_range = pd.date_range(start=start_date, end=end_date)

# Loop through each date
for single_date in date_range:
    single_date_str = single_date.strftime('%Y-%m-%d')
    print(f"\nProcessing date: {single_date_str}")

    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="106.51.63.60",
            user="mahesh",
            password="mahesh_123",
            database="historicaldb"
        )
        cursor = conn.cursor()
        
        # Fetch only future data
        query = f"SELECT * FROM sensex_future WHERE DATE(date) = '{single_date_str}'"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print(f"No future data for {single_date_str}")
        else:
            columns = [desc[0] for desc in cursor.description]
            df_underlying = pd.DataFrame(rows, columns=columns)

            # Save data to CSV
            future_filename = os.path.join(future_folder, f"{single_date_str}_sensex_future.csv")
            df_underlying.to_csv(future_filename, index=False)
            print(f"Saved future data to {future_filename}")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error processing future data for {single_date_str}: {e}")
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

print(f"\nFuture data export completed.")
print(f"Future data saved in: {future_folder}")

# Now perform the merging operation
print("\nStarting merging operation...")

# Get all CSV files from the directory
csv_files = glob.glob(os.path.join(future_folder, '*.csv'))

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
merged_df['time'] = merged_df['time'].apply(seconds_to_time)

# Sort by date and time
merged_df = merged_df.sort_values(['date', 'time'])

# Convert OHLC columns to rupees and round to two decimals
ohlc_cols = ['open', 'high', 'low', 'close']
merged_df[ohlc_cols] = (merged_df[ohlc_cols] / 100).round(2)

# Save to CSV with a new filename
output_file = os.path.join(output_folder, 'sensex_future_merged_rounded_v2.csv')
merged_df.to_csv(output_file, index=False)

print(f"All daily data has been merged and saved to '{output_file}' with time in HH:MM:SS format and OHLC rounded to 2 decimals") 