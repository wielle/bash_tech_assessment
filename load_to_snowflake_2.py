# import snowflake.connector
# import pandas as pd
# import json

# # Connect to Snowflake
# conn = snowflake.connector.connect(
#     user='IANSPIES',
#     password='Spiesawe123',
#     account='gy57703.ap-south-1.aws'
# )

# # Load CSV into DataFrame
# df = pd.read_csv('nasa_data_incremental.csv')

# # Function to extract the 'close_approach_date' from JSON
# def extract_close_approach_date(json_str):
#     try:
#         data = json.loads(json_str.replace("'", "\""))  # Replace single quotes with double quotes for valid JSON
#         if isinstance(data, list) and len(data) > 0:
#             return data[0]['close_approach_date']
#         return None
#     except (json.JSONDecodeError, KeyError, TypeError):
#         return None

# # Extract 'close_approach_data' and convert to string
# df['close_approach_data'] = df['close_approach_data'].apply(extract_close_approach_date)
# df['close_approach_data'] = pd.to_datetime(df['close_approach_data'], format='%Y-%m-%d', errors='coerce')
# df['close_approach_data'] = df['close_approach_data'].dt.strftime('%Y-%m-%d')  # Convert to string

# # Replace NaN values with None
# df = df.where(pd.notnull(df), None)

# # Create Snowflake cursor
# cur = conn.cursor()

# # Use database and schema
# cur.execute("USE DATABASE nasa_data_2")
# cur.execute("USE SCHEMA nasa_schema_2")

# # Insert DataFrame into Snowflake table
# for _, row in df.iterrows():
#     cur.execute("""
#         INSERT INTO nasa_neo_data_2 (id, neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, 
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
# max_date = df['close_approach_data'].max().strftime('%Y-%m-%d')

# # Update the last load date in load_tracking table
# cur.execute("UPDATE load_tracking SET last_load_date = %s", (max_date,))

# # Close the cursor and connection
# cur.close()
# conn.close()

# print("Incremental data loaded successfully into Snowflake.")

# ------------------------------------------------------------------- 
# #ONLY USIG A DATE FROM close_approach_data

# import snowflake.connector
# import pandas as pd
# import json

# # Connect to Snowflake
# conn = snowflake.connector.connect(
#     user='IANSPIES',
#     password='Spiesawe123',
#     account='gy57703.ap-south-1.aws'
# )

# # Load CSV into DataFrame
# df = pd.read_csv('nasa_data_2.csv')

# # Function to extract the 'close_approach_date' from JSON
# def extract_close_approach_date(json_str):
#     try:
#         data = json.loads(json_str.replace("'", "\""))  # Replace single quotes with double quotes for valid JSON
#         if isinstance(data, list) and len(data) > 0:
#             return data[0]['close_approach_date']
#         return None
#     except (json.JSONDecodeError, KeyError, TypeError):
#         return None

# # Extract 'close_approach_data' and convert to datetime
# df['close_approach_data'] = df['close_approach_data'].apply(extract_close_approach_date)
# df['close_approach_data'] = pd.to_datetime(df['close_approach_data'], format='%Y-%m-%d', errors='coerce')

# # Ensure all dates are in the correct string format
# df['close_approach_data'] = df['close_approach_data'].dt.strftime('%Y-%m-%d')

# # Replace NaN values with None
# df = df.where(pd.notnull(df), None)

# # Create Snowflake cursor
# cur = conn.cursor()

# # Use database and schema
# cur.execute("USE DATABASE nasa_data_2")
# cur.execute("USE SCHEMA nasa_schema_2")

# # Insert DataFrame into Snowflake table
# for _, row in df.iterrows():
#     cur.execute("""
#         INSERT INTO nasa_neo_data_2 (id, neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, 
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
# max_date = pd.to_datetime(df['close_approach_data']).max().strftime('%Y-%m-%d')

# # Update the last load date in load_tracking table
# cur.execute("UPDATE load_tracking SET last_load_date = %s", (max_date,))

# # Close the cursor and connection
# cur.close()
# conn.close()

# print("Incremental data loaded successfully into Snowflake.")

# -------------------------------------------------------------------
# USIG A DATE AS A NEW COLUMN close_approach_date FROM close_approach_data


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
cur.execute("USE DATABASE nasa_data_2")
cur.execute("USE SCHEMA nasa_schema_2")

# Insert DataFrame into Snowflake table
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO nasa_neo_data_2 (id, neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, 
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
cur.execute("UPDATE load_tracking SET last_load_date = %s", (max_date,))

# Close the cursor and connection
cur.close()
conn.close()

print("Incremental data loaded successfully into Snowflake.")
