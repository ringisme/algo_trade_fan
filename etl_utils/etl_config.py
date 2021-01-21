import os
from os.path import join, dirname
from typing import Optional, Dict

from dateutil import tz
from dotenv import load_dotenv

# Automatically find .env file and load.
env_path = join(dirname(__file__), 'etl_info.env')
print(env_path)
load_dotenv(env_path)

# Load RDS information:
RDS_CONFIG = {
    "USERNAME": os.getenv("RDS_USER"),
    "PASSWORD": os.getenv("RDS_PASSWORD"),
    "HOST": os.getenv("RDS_HOST"),
    "DATABASE": os.getenv("RDS_DATABASE"),
    "NAME": os.getenv("RDS_NAME"),
    "PORT": os.getenv("RDS_PORT"),
    # ----- CUSTOM PART -----
    "DAILY_TABLE": 'daily_raw',
    "INTRADAY_TABLE": 'intraday_raw',
    "SPLIT_TABLE": 'split_ref',
    "CHUNK_SIZE": 100000
}

# Load Finnhub information:
FINNHUB_CONFIG = {
    "API_KEY": os.getenv("FINNHUB_API_KEY"),
    # ----- CUSTOM PART -----
    "API_LIMIT": 1.1,  # control how long time that the API can be used again.
    "INTRADAY_LIMIT": '30D'  # Finnhub limits the intraday data return period as 30 days.
}

# Load the Twilio information:
TWILIO_CONFIG = {
    "ACCOUNT_SID": os.getenv("TWILIO_ACCOUNT_SID"),
    "AUTH_TOKEN": os.getenv("TWILIO_AUTH_TOKEN"),
    "TWILIO_PHONE": os.getenv("TWILIO_PHONE"),
    "USER_PHONE": os.getenv("USER_PHONE")
}

# Set user information:
USER_CUSTOM = {
    "TIMEZONE": tz.gettz("Canada/Toronto")
}
