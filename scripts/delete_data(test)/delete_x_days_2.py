
import os
import snowflake.connector
from datetime import datetime, timedelta

# Retrieve Snowflake connection details from environment variables
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account
)


# Create Snowflake cursor
cur = conn.cursor()

# Use database and schema
cur.execute("USE DATABASE nasa_data_2")
cur.execute("USE SCHEMA nasa_schema_2") #if unified data should be deleted, change database and schema.

# Calculate the date 3 days ago
three_days_ago = (datetime.now() - timedelta(days=3)).date()

# Delete the last 3 days of data
cur.execute("DELETE FROM nasa_neo_data_2 WHERE close_approach_date >= %s", (three_days_ago,))

# Close the cursor and connection
cur.close()
conn.close()

print("Last 3 days of data deleted successfully from Snowflake.")
