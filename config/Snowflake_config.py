# import snowflake.connector

# # Connect to Snowflake
# conn = snowflake.connector.connect(
#     user='XXXX',
#     password='XXXX',
#     account='XXXXX'
# )

#Setting environment variables

def set_snowflake_environment_variables():
    # Replace these values with your actual Snowflake login details
    snowflake_user = "IANSPIES"
    snowflake_password = "Spiesawe123"
    snowflake_account = "gy57703.ap-south-1.aws"

    # Set the environment variables
    os.environ['SNOWFLAKE_USER'] = snowflake_user
    os.environ['SNOWFLAKE_PASSWORD'] = snowflake_password
    os.environ['SNOWFLAKE_ACCOUNT'] = snowflake_account

    # Save the environment variables persistently for future sessions
    with open(os.path.expanduser("~/.bash_profile"), "a") as bash_profile:
        bash_profile.write(f"\nexport SNOWFLAKE_USER={snowflake_user}\n")
        bash_profile.write(f"export SNOWFLAKE_PASSWORD={snowflake_password}\n")
        bash_profile.write(f"export SNOWFLAKE_ACCOUNT={snowflake_account}\n")

    print("Snowflake credentials have been set and saved to your .bash_profile.")

if __name__ == "__main__":
    set_snowflake_environment_variables()