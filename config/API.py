from datetime import datetime, timedelta

# Manually Replace 'YOUR_NASA_API_KEY' with your actual API key
# api_key = 'XXXXX'
# end_date = datetime.now()
# start_date = end_date - timedelta(days=30)

#or set invironment variable
import os

def set_environment_variable():
    # Replace 'your_api_key_here' with the actual API key
    api_key = "zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC"

    # Set the environment variable
    os.environ['NASA_API_KEY'] = api_key

    # Save the environment variable persistently for future sessions
    with open(os.path.expanduser("~/.bash_profile"), "a") as bash_profile:
        bash_profile.write(f"\nexport NASA_API_KEY={api_key}\n")

    print("NASA API Key has been set and saved to your .bash_profile.")

if __name__ == "__main__":
    set_environment_variable()