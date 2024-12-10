import pandas as pd 
import tweepy
import time 
from typing import List, Dict, Optional

API = API_KEY
API_SECRET = API_SECRET_KEY
BEARER_TOKEN = BEARER_TOKEN_API

client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)