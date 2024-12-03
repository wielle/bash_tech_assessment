# import snowflake.connector

# # Connect to Snowflake
# conn = snowflake.connector.connect(
#     user='IANSPIES',
#     password='Spiesawe123',
#     account='gy57703.ap-south-1.aws'
# )

# # Create Snowflake cursor
# cur = conn.cursor()

# # Use database and schema
# cur.execute("USE DATABASE nasa_data_2")
# cur.execute("USE SCHEMA nasa_schema_2")

# # Delete the last 3 days of data
# cur.execute("DELETE FROM nasa_neo_data_2 WHERE close_approach_data >= (CURRENT_DATE - INTERVAL '3 days')")

# # Close the cursor and connection
# cur.close()
# conn.close()

# print("Last 3 days of data deleted successfully from Snowflake.")

import snowflake.connector
from datetime import datetime, timedelta

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='IANSPIES',
    password='Spiesawe123',
    account='gy57703.ap-south-1.aws'
)

# Create Snowflake cursor
cur = conn.cursor()

# Use database and schema
cur.execute("USE DATABASE nasa_data_2")
cur.execute("USE SCHEMA nasa_schema_2")

# Calculate the date 3 days ago
three_days_ago = (datetime.now() - timedelta(days=3)).date()

# Delete the last 3 days of data
cur.execute("DELETE FROM nasa_neo_data_2 WHERE close_approach_date >= %s", (three_days_ago,))

# Close the cursor and connection
cur.close()
conn.close()

print("Last 3 days of data deleted successfully from Snowflake.")
