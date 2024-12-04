import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='XXXX',
    password='XXXX',
    account='XXXXX'
)