import requests
import pandas as pd
from datetime import datetime, timedelta
import snowflake.connector

# Replace 'YOUR_NASA_API_KEY' with your actual API key
api_key = 'zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC'

# Function to fetch data for a given date range
def fetch_data(start_date, end_date):
    all_neos = []
    while start_date < end_date:
        chunk_end_date = start_date + timedelta(days=7)
        if chunk_end_date > end_date:
            chunk_end_date = end_date
        
        print(f"Fetching data for range {start_date} to {chunk_end_date}")  # Debug statement
        url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={chunk_end_date}&api_key={api_key}'
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
    
    return all_neos

# Load the last load date from Snowflake so as to only fetch new data
conn = snowflake.connector.connect(
    user='IANSPIES',
    password='Spiesawe123',
    account='gy57703.ap-south-1.aws'
)
cur = conn.cursor()
cur.execute("USE DATABASE nasa_data_2")
cur.execute("USE SCHEMA nasa_schema_2")
cur.execute("SELECT MAX(close_approach_date) FROM nasa_neo_data_2")
last_load_date = cur.fetchone()[0]
cur.close()
conn.close()

if last_load_date is None:
    last_load_date = (datetime.now() - timedelta(days=7)).date()
else:
    last_load_date = datetime.strptime(last_load_date, '%Y-%m-%d').date()

start_date = last_load_date + timedelta(days=1)
end_date = datetime.now().date()

# Fetch new data
df_new_data = fetch_data(start_date, end_date)

# Normalize JSON to flat table
if df_new_data:  # Check if data is not empty
    df_new_data = pd.json_normalize(df_new_data, sep='_')
    df_new_data.to_csv('nasa_data_incremental.csv', index=False)
    print(f"Data fetched and saved to nasa_data_incremental.csv for dates {start_date} to {end_date}")
else:
    print("No new data fetched.")