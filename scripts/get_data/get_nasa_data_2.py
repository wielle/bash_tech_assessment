import requests
import os
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

# Define the folder and file path
folder_path = '/Users/spicegold/Documents/vscode/Bash Techincal Assesment/Nasa_2/data'
file_path = os.path.join(folder_path, 'nasa_data_2.csv')

# Create the directory if it doesn't exist
os.makedirs(folder_path, exist_ok=True)

# Save the DataFrame to the CSV file
df.to_csv(file_path, index=False)

print(f"Data fetched and saved to {file_path}")

