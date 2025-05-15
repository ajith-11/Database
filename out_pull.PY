import pandas as pd
import mysql.connector
from datetime import datetime
import os

# Define date range and paths
start_date = '2022-01-01'
end_date = '2025-05-12'
base_path = r"D:\stocks\raw_stocks\banknifty_future" 
folder_name = 'banknifty_jann-decc-full_data' 
output_folder = os.path.join(base_path, folder_name)

# Create all parent directories first
os.makedirs(base_path, exist_ok=True)  # Create base directory
os.makedirs(output_folder, exist_ok=True)  # Create output directory
future_folder = os.path.join(output_folder, 'future')
os.makedirs(future_folder, exist_ok=True)  # Create future directory

print(f"Main output folder: {output_folder}")
print(f"future data will be saved in: {future_folder}")
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
        query = f"SELECT * FROM banknifty_future WHERE DATE(date) = '{single_date_str}'"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print(f"No future data for {single_date_str}")
        else:
            columns = [desc[0] for desc in cursor.description]
            df_underlying = pd.DataFrame(rows, columns=columns)

            # Save data to CSV
            future_filename = os.path.join(future_folder, f"{single_date_str}_banknifty_future.csv")
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

print(f"\nfuture data export completed.")
print(f"future data saved in: {future_folder}")
