# import requests
# import pandas as pd
# from datetime import datetime, timedelta

# # Replace 'YOUR_NASA_API_KEY' with your actual API key
# api_key = 'zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC'
# end_date = datetime.now()
# start_date = end_date - timedelta(days=30)

# all_neos = []

# # Fetch data in a single 7-day period
# print(f"Fetching data for range {start_date.date()} to {end_date.date()}")  # Debug statement
# url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date.date()}&end_date={end_date.date()}&api_key={api_key}'
# response = requests.get(url)
# data = response.json()

# # Ensure 'near_earth_objects' key is present in the response
# if 'near_earth_objects' in data:
#     for date, neos_for_date in data['near_earth_objects'].items():
#         for neo in neos_for_date:
#             all_neos.append(neo)
# else:
#     print(f"Error fetching data: {data.get('error_message', 'Unknown error')}")

# df = pd.json_normalize(all_neos, sep='_')
# file_path = '/Users/spicegold/Documents/vscode/Bash Techincal Assesment/Nasa_2.csv'
# df.to_csv('nasa_data_2.csv', index=False)

# print("Data fetched and saved to nasa_data_2.csv")


import requests
import pandas as pd
from datetime import datetime, timedelta

# Replace 'YOUR_NASA_API_KEY' with your actual API key
api_key = 'zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC'
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

all_neos = []

# Fetch data in 7-day periods
while start_date < end_date:
    chunk_end_date = start_date + timedelta(days=7)
    if chunk_end_date > end_date:
        chunk_end_date = end_date
    
    print(f"Fetching data for range {start_date.date()} to {chunk_end_date.date()}")  # Debug statement
    url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date.date()}&end_date={chunk_end_date.date()}&api_key={api_key}'
    response = requests.get(url)
    data = response.json()
    
    # Ensure 'near_earth_objects' key is present in the response
    if 'near_earth_objects' in data:
        for date, neos_for_date in data['near_earth_objects'].items():
            for neo in neos_for_date:
                all_neos.append(neo)
    else:
        print(f"Error fetching data: {data.get('error_message', 'Unknown error')}")
    
    # Move to the next 7-day chunk
    start_date = chunk_end_date + timedelta(days=1)

# Normalize JSON to flat table
df = pd.json_normalize(all_neos, sep='_')
file_path = '/Users/spicegold/Documents/vscode/Bash Techincal Assesment/Nasa_2.csv'
df.to_csv('nasa_data_2.csv', index=False)

print("Data fetched and saved to nasa_data_2.csv")
