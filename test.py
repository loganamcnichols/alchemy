import os
from dotenv import load_dotenv
from requests import request
import requests

load_dotenv()

survey_id = 8005443

alchemer_host = "https://api.alchemer.com"
survey_path   = f"{alchemer_host}/v5/survey/{survey_id}/surveyquestion/{5467}"

api_key    = os.environ.get("API_KEY")
api_secret = os.environ.get("API_SECRET")

res = requests.get(survey_path, params={'api_token': api_key, 'api_token_secret': api_secret})

data = res.json()["data"]

print(data)