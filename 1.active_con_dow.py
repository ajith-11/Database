import time
import os
import json
import requests
from polygon import RESTClient

client = RESTClient("Ew2FuxGYsC7Gl0ChO6C68pYKL50pcOUn")

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0',
    
}

cookies = {
    '_ga': 'GA1.1.1067315011.1729961990',
    'intercom-id-o3858vgb': '01eceb69-442f-47d2-823b-3bda68a57163',
    'intercom-device-id-o3858vgb': 'ac7f58a1-52ae-4c6a-9522-321933bb6990',
    '_gcl_au': '1.1.950507192.1737996844',
    'polygon-token': '_uaDadXfEqnHVLO8ub5Oix1HXpSsbhug',
    'polygon-account': 'eyJpZCI6IjFmMDQwZTc4LWU3ZDktNGE5YS1hOGMyLTYyMmQ1OTZlOTYzZSIsImNyZWF0ZWRfdXRjIjoiMDAwMS0wMS0wMVQwMDowMDowMFoiLCJwYXltZW50X2lkIjoiY3VzX1JVMHNHNXNzZkNXTXhKIiwiZW1haWwiOiJuaW5hZm80NTYwQGV2bmZ0LmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicHJvZmlsZV9waWN0dXJlX3VybCI6Imh0dHBzOi8vd3d3LmdyYXZhdGFyLmNvbS9hdmF0YXIvZmVhMWQ2NjE3YTA5ZmFmOWY3MzZlZTYyNmU2OTEzOWIzNDliYjJiNzMyMjJiMjZjNDI3NjUzYWQ0Yzc1N2Y1Yj9kPWlkZW50aWNvblx1MDAyNnM9MjYwIn0%253D',
    'intercom-session-o3858vgb': 'NFRFWGJtalNmemU1NmpFaVEwdGk2TWRVWm52My9abU9SMzdqSjVHQUQ0aHNqeHg0TkhnM0thVjJrMGs4WWFCKzRnT2o4QnR3TlY2SHl6SnFRUEhjM2h3ZmlNVjVvaDZxZzk3UHhuRDAwcEk9LS1nRmJ1dzQxbGluQVNwN1ZEeGwyNmdnPT0=--07133157ce7a1605f5e9bac38cf10c4bf3f29224',
    '_ga_5BMBZ91JZ3': 'GS1.1.1741871651.53.1.1741872627.29.0.0',
    'ph_phc_PShpU358xJhMvcFCipJhyMouxNWXP0ENPgIwSuyBH0J_posthog': '%7B%22distinct_id%22%3A%22ninafo4560%40evnft.com%22%2C%22%24sesid%22%3A%5B1741872628501%2C%2201958fa4-01dd-7428-ac51-bd82d7a26c22%22%2C1741871645149%5D%2C%22%24epp%22%3Atrue%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22%24direct%22%2C%22u%22%3A%22https%3A%2F%2Fpolygon.io%2F%22%7D%7D',
}
params = {
    'underlying_ticker': 'QQQ',
    'expired': 'false',
    'limit': '1000',
    'apiKey': 'Ew2FuxGYsC7Gl0ChO6C68pYKL50pcOUn',
}

def fetch_contracts(client, ticker, expired=True, output_dir="output"):

    call_count = 1
    index = 1
    next_url = None

    while True:
        try:
            print(f"trying for {index=}")
            if not next_url:
                
                response = requests.get('https://api.polygon.io/v3/reference/options/contracts', params=params, cookies=cookies, headers=headers)
            else:
                response = requests.get(next_url, params = {'apiKey': 'Ew2FuxGYsC7Gl0ChO6C68pYKL50pcOUn'}, cookies=cookies, headers=headers)

            if response.status_code != 200:
                raise Exception(f"API call failed with status code {response.status_code}")

            data = response.json()

            next_url = data.get('next_url')
            
            

            json_file = os.path.join(output_dir, f'{ticker}_contracts_{index}.json')
            with open(json_file, 'w') as f:
                json.dump(data, f)

            print(f"Saved API call {call_count} output to {json_file}")
            if not next_url:
                print("no new data hence exiting")
                break
            call_count += 1
            index +=1
            if call_count > 5:

                print("sleeping for a min")
                time.sleep(60)
                call_count = 1


        except Exception as e:
            print(f"An error occurred: {e}")
            print("sleeping for a min")
            time.sleep(60)
            call_count = 0

    return ""

if __name__ == '__main__':
    try:
        output_dir = "./active_contracts_qqq"
        os.makedirs(output_dir, exist_ok=True)

        print("Fetching active contracts...")
        active_contracts = fetch_contracts(client, ticker="QQQ", expired=False, output_dir=output_dir)



    except Exception as e:
        print(f"An error occurred: {e}")









