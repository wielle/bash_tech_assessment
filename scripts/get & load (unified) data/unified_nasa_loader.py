# import snowflake.connector
# import requests
# import pandas as pd
# from datetime import datetime, timedelta
# import json

# # Snowflake connection parameters
# snowflake_conn_params = {
#     'user': 'IANSPIES',
#     'password': 'Spiesawe123',
#     'account': 'gy57703.ap-south-1.aws'
# }

# # NASA API key
# api_key = 'zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC'

# # Function to fetch data for a given date range
# def fetch_data(start_date, end_date):
#     url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}'
#     response = requests.get(url)
#     data = response.json()
#     neos = []
#     if 'near_earth_objects' in data:
#         for date, neos_for_date in data['near_earth_objects'].items():
#             for neo in neos_for_date:
#                 neos.append(neo)
#     else:
#         print(f"Error fetching data: {data.get('error_message', 'Unknown error')}")
#     return pd.json_normalize(neos, sep='_')

# # Extract the 'close_approach_date' from the list
# def extract_close_approach_date(approach_data_list):
#     try:
#         if isinstance(approach_data_list, list) and len(approach_data_list) > 0:
#             return approach_data_list[0]['close_approach_date']
#         return None
#     except (KeyError, TypeError):
#         return None

# # Connect to Snowflake
# conn = snowflake.connector.connect(**snowflake_conn_params)
# cur = conn.cursor()

# # Use the new unified database and schema
# cur.execute("USE DATABASE nasa_unified_data_2")
# cur.execute("USE SCHEMA nasa_unified_schema_2")

# # Load the last load date from Snowflake
# cur.execute("SELECT MAX(last_load_date) FROM load_tracking_unified")
# last_load_date = cur.fetchone()[0]

# if last_load_date is None:
#     last_load_date = (datetime.now() - timedelta(days=7)).date()
# else:
#     last_load_date = last_load_date

# start_date = last_load_date + timedelta(days=1)
# end_date = datetime.now().date()

# # Fetch new data
# df_new_data = fetch_data(start_date, end_date)
# df_new_data['close_approach_data'] = df_new_data['close_approach_data'].apply(extract_close_approach_date)
# df_new_data['close_approach_data'] = pd.to_datetime(df_new_data['close_approach_data'], format='%Y-%m-%d', errors='coerce')
# df_new_data['close_approach_data'] = df_new_data['close_approach_data'].dt.strftime('%Y-%m-%d')
# df_new_data = df_new_data.where(pd.notnull(df_new_data), None)

# # Delete the last 3 days of data in Snowflake
# cur.execute("DELETE FROM nasa_neo_unified_data_2 WHERE close_approach_data >= (CURRENT_DATE - INTERVAL '3 days')")

# # Insert new data into Snowflake
# for _, row in df_new_data.iterrows():
#     cur.execute("""
#         INSERT INTO nasa_neo_unified_data_2 (id, neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, 
#         is_potentially_hazardous_asteroid, close_approach_data, is_sentry_object, links_self, 
#         estimated_diameter_kilometers_estimated_diameter_min, estimated_diameter_kilometers_estimated_diameter_max, 
#         estimated_diameter_meters_estimated_diameter_min, estimated_diameter_meters_estimated_diameter_max, 
#         estimated_diameter_miles_estimated_diameter_min, estimated_diameter_miles_estimated_diameter_max, 
#         estimated_diameter_feet_estimated_diameter_min, estimated_diameter_feet_estimated_diameter_max, 
#         sentry_data) 
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """, 
#     (row['id'], row['neo_reference_id'], row['name'], row['nasa_jpl_url'], row['absolute_magnitude_h'], 
#     row['is_potentially_hazardous_asteroid'], row['close_approach_data'], row['is_sentry_object'], row['links_self'], 
#     row['estimated_diameter_kilometers_estimated_diameter_min'], row['estimated_diameter_kilometers_estimated_diameter_max'], 
#     row['estimated_diameter_meters_estimated_diameter_min'], row['estimated_diameter_meters_estimated_diameter_max'], 
#     row['estimated_diameter_miles_estimated_diameter_min'], row['estimated_diameter_miles_estimated_diameter_max'], 
#     row['estimated_diameter_feet_estimated_diameter_min'], row['estimated_diameter_feet_estimated_diameter_max'], 
#     row['sentry_data']))

# # Extract the maximum date from the DataFrame and convert it to a string format
# max_date = pd.to_datetime(df_new_data['close_approach_data']).max().strftime('%Y-%m-%d')

# # Insert the last load date into the load_tracking_unified table
# cur.execute("INSERT INTO load_tracking_unified (last_load_date) VALUES (%s)", (max_date,))

# # Close the cursor and connection
# cur.close()
# conn.close()

# print("Unified data load process completed successfully.")

#-------------------------------------------------------------

# import snowflake.connector
# import requests
# import pandas as pd
# from datetime import datetime, timedelta
# import json

# # Snowflake connection parameters
# snowflake_conn_params = {
#     'user': 'IANSPIES',
#     'password': 'Spiesawe123',
#     'account': 'gy57703.ap-south-1.aws'
# }

# # NASA API key
# api_key = 'zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC'

# # Function to fetch data for a given date range
# def fetch_data(start_date, end_date):
#     url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}'
#     response = requests.get(url)
#     data = response.json()
#     neos = []
#     if 'near_earth_objects' in data:
#         for date, neos_for_date in data['near_earth_objects'].items():
#             for neo in neos_for_date:
#                 neos.append(neo)
#     else:
#         print(f"Error fetching data: {data.get('error_message', 'Unknown error')}")
#     return pd.json_normalize(neos, sep='_')

# # Extract the 'close_approach_date' from the list
# def extract_close_approach_date(approach_data_list):
#     try:
#         if isinstance(approach_data_list, list) and len(approach_data_list) > 0:
#             return approach_data_list[0]['close_approach_date']
#         return None
#     except (KeyError, TypeError):
#         return None

# # Connect to Snowflake
# conn = snowflake.connector.connect(**snowflake_conn_params)
# cur = conn.cursor()

# # Use the new unified database and schema
# cur.execute("USE DATABASE nasa_unified_data_2")
# cur.execute("USE SCHEMA nasa_unified_schema_2")

# # Load the last load date from Snowflake
# cur.execute("SELECT MAX(last_load_date) FROM load_tracking_unified")
# last_load_date = cur.fetchone()[0]

# if last_load_date is None:
#     last_load_date = (datetime.now() - timedelta(days=30)).date()  # Adjusted to 30 days
# else:
#     last_load_date = last_load_date

# start_date = last_load_date + timedelta(days=1)
# end_date = datetime.now().date()

# # Fetch new data
# df_new_data = fetch_data(start_date, end_date)
# df_new_data['close_approach_data'] = df_new_data['close_approach_data'].apply(extract_close_approach_date)
# df_new_data['close_approach_data'] = pd.to_datetime(df_new_data['close_approach_data'], format='%Y-%m-%d', errors='coerce')
# df_new_data['close_approach_data'] = df_new_data['close_approach_data'].dt.strftime('%Y-%m-%d')
# df_new_data = df_new_data.where(pd.notnull(df_new_data), None)

# # Delete the last 3 days of data in Snowflake
# cur.execute("DELETE FROM nasa_neo_unified_data_2 WHERE close_approach_data >= (CURRENT_DATE - INTERVAL '3 days')")

# # Insert new data into Snowflake
# for _, row in df_new_data.iterrows():
#     cur.execute("""
#         INSERT INTO nasa_neo_unified_data_2 (id, neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, 
#         is_potentially_hazardous_asteroid, close_approach_data, is_sentry_object, links_self, 
#         estimated_diameter_kilometers_estimated_diameter_min, estimated_diameter_kilometers_estimated_diameter_max, 
#         estimated_diameter_meters_estimated_diameter_min, estimated_diameter_meters_estimated_diameter_max, 
#         estimated_diameter_miles_estimated_diameter_min, estimated_diameter_miles_estimated_diameter_max, 
#         estimated_diameter_feet_estimated_diameter_min, estimated_diameter_feet_estimated_diameter_max, 
#         sentry_data) 
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """, 
#     (row.get('id'), row.get('neo_reference_id'), row.get('name'), row.get('nasa_jpl_url'), row.get('absolute_magnitude_h'), 
#     row.get('is_potentially_hazardous_asteroid'), row.get('close_approach_data'), row.get('is_sentry_object'), row.get('links_self'), 
#     row.get('estimated_diameter_kilometers_estimated_diameter_min'), row.get('estimated_diameter_kilometers_estimated_diameter_max'), 
#     row.get('estimated_diameter_meters_estimated_diameter_min'), row.get('estimated_diameter_meters_estimated_diameter_max'), 
#     row.get('estimated_diameter_miles_estimated_diameter_min'), row.get('estimated_diameter_miles_estimated_diameter_max'), 
#     row.get('estimated_diameter_feet_estimated_diameter_min'), row.get('estimated_diameter_feet_estimated_diameter_max'), 
#     row.get('sentry_data')))
# #Instead of directly accessing the key, using row.get('sentry_data') ensures that if the key is missing, it will return None/NULL instead of raising an error.

# # Extract the maximum date from the DataFrame and convert it to a string format
# max_date = pd.to_datetime(df_new_data['close_approach_data']).max().strftime('%Y-%m-%d')

# # Insert the last load date into the load_tracking_unified table
# cur.execute("INSERT INTO load_tracking_unified (last_load_date) VALUES (%s)", (max_date,))

# # Close the cursor and connection
# cur.close()
# conn.close()

# print("Unified data load process completed successfully.")

#-------------------------------------------------------------

import snowflake.connector
import pandas as pd
import json

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='IANSPIES',
    password='Spiesawe123',
    account='gy57703.ap-south-1.aws'
)

# Load CSV into DataFrame
df = pd.read_csv('nasa_data_2.csv')

# Function to extract the 'close_approach_date' from JSON
def extract_close_approach_date(json_str):
    try:
        data = json.loads(json_str.replace("'", "\""))  # Replace single quotes with double quotes for valid JSON
        if isinstance(data, list) and len(data) > 0:
            return data[0]['close_approach_date']
        return None
    except (json.JSONDecodeError, KeyError, TypeError):
        return None

# Extract 'close_approach_date' without modifying the original 'close_approach_data'
df['close_approach_date'] = df['close_approach_data'].apply(extract_close_approach_date)
df['close_approach_date'] = pd.to_datetime(df['close_approach_date'], format='%Y-%m-%d', errors='coerce')

# Ensure all dates are in the correct string format
df['close_approach_date'] = df['close_approach_date'].dt.strftime('%Y-%m-%d')

# Replace NaN values with None
df = df.where(pd.notnull(df), None)

# Create Snowflake cursor
cur = conn.cursor()

# Use database and schema
cur.execute("USE DATABASE nasa_unified_data_2")
cur.execute("USE SCHEMA nasa_unified_schema_2")

# Insert DataFrame into Snowflake table
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO nasa_neo_unified_data_2 (id, neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, 
        is_potentially_hazardous_asteroid, close_approach_data, close_approach_date, is_sentry_object, links_self, 
        estimated_diameter_kilometers_estimated_diameter_min, estimated_diameter_kilometers_estimated_diameter_max, 
        estimated_diameter_meters_estimated_diameter_min, estimated_diameter_meters_estimated_diameter_max, 
        estimated_diameter_miles_estimated_diameter_min, estimated_diameter_miles_estimated_diameter_max, 
        estimated_diameter_feet_estimated_diameter_min, estimated_diameter_feet_estimated_diameter_max, 
        sentry_data) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, 
    (row['id'], row['neo_reference_id'], row['name'], row['nasa_jpl_url'], row['absolute_magnitude_h'], 
    row['is_potentially_hazardous_asteroid'], row['close_approach_data'], row['close_approach_date'], row['is_sentry_object'], row['links_self'], 
    row['estimated_diameter_kilometers_estimated_diameter_min'], row['estimated_diameter_kilometers_estimated_diameter_max'], 
    row['estimated_diameter_meters_estimated_diameter_min'], row['estimated_diameter_meters_estimated_diameter_max'], 
    row['estimated_diameter_miles_estimated_diameter_min'], row['estimated_diameter_miles_estimated_diameter_max'], 
    row['estimated_diameter_feet_estimated_diameter_min'], row['estimated_diameter_feet_estimated_diameter_max'], 
    row['sentry_data']))

# Extract the maximum date from the 'close_approach_date' column and convert it to a string format
max_date = pd.to_datetime(df['close_approach_date']).max().strftime('%Y-%m-%d')

# Update the last load date in load_tracking table
cur.execute("UPDATE load_tracking_unified SET last_load_date = %s", (max_date,))

# Close the cursor and connection
cur.close()
conn.close()


print("Unified data successfully loaded into Snowflake")
