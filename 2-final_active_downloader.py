import pandas as pd
from logzero import logfile, logger
import traceback
from polygon import RESTClient
import concurrent.futures
import glob
import time
import json
from datetime import datetime

logfile("qqq_options_downloader.log")

folder_name = "active_symm"
input_files = glob.glob("active_contracts_qqq/*.json")

shortlisted_option_details = []
for file in input_files:
    print(f"reading {file}")
    with open(file) as f:
        file_con = json.load(f)
    cutoff_date = datetime.strptime('2025-04-28', "%Y-%m-%d")

    filtered_data = [entry for entry in file_con['results'] if datetime.strptime(entry['expiration_date'], "%Y-%m-%d") >= cutoff_date]
    shortlisted_option_details.extend(filtered_data)

with open('active_contracts_After_oct.json', 'w') as f:
    json.dump(shortlisted_option_details, f)

symbols_list = [d['ticker'] for d in shortlisted_option_details]
symbols_downloaded = ["O:" + filename.removeprefix("active_symm\\").removesuffix(".csv") for filename in glob.glob("active_symm/*.csv")]
symbols_to_download = [sym for sym in symbols_list if sym not in symbols_downloaded]

api_keys = [
    "GO23xY0lWP7RENkBR12g_azEC5aHUWSd", "c4NGgQ4K9zxDiBkR8X669WPippasJ_8U", "FUF9Ro2lN8oX0Dg1MJuAqdG1T7HBaJ8u",
    "VXbuUBwIqECaWwsCrRWmNQxsm3xhYle8", "iMgpp0E3RFppFwOkO7PblrmI6CSF3KpF", "rNSRWNyi6oV1tAhRIp9ulaY8jFNUtX2n",
    "xjZyaBtBiIsavcm24fm0lpQNyzrXKDHC", "dRXJLmcmr6ToSMYjpsBwcdmzwH7SAtNB", "e1uMnGFPPO4leDVsHpp1NEzO2l9LTG7D",
    "CoaaQpJzJD1rS9HLUujzjc98lx2Vj1HA"
]

last_call_time = {apikey: 0 for apikey in api_keys}

def download_data(symbol, apikey):
    try:
        client = RESTClient(apikey)
        logger.info(f"trying {symbol}")

        while True:
            current_time = time.time()
            if current_time - last_call_time[apikey] >= 12:
                last_call_time[apikey] = current_time
                break
            time.sleep(1)

        aggs_dict = [a.__dict__ for a in client.list_aggs(symbol, 1, "minute", "2025-04-28", "2026-01-08", limit=50000)]
        df = pd.DataFrame(aggs_dict) if aggs_dict else pd.DataFrame()
        
        df["datetime"] = pd.to_datetime(df["timestamp"], unit='ms') if not df.empty else None
        df.to_csv(f"{folder_name}\\{symbol.removeprefix('O:')}.csv", index=False)
        logger.info(f"completed for {symbol}")
    except:
        logger.error(traceback.format_exc())

def main():
    print(f"completed preprocessing - {len(symbols_to_download)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        tasks = [(symbol, api_keys[i % len(api_keys)]) for i, symbol in enumerate(symbols_to_download)]
        executor.map(lambda x: download_data(*x), tasks)

if __name__ == "__main__":
    main()