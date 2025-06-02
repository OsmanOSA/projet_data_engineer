import os
import requests
import json
import pandas as pd

from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

url_token = "https://digital.iservices.rte-france.com/token/oauth/"
username = os.getenv("CLIENT_ID")
password = os.getenv("CLIENT_SECRET")

data = {
            "grant_type": "client_credentials"
}

response = requests.post(url_token, data=data, auth=(username, password))
token = response.json().get("access_token")
print(token)


base_url = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term?type=REALISED"
headers = {
            "Host": "digital.iservices.rte-france.com",
            "Authorization": f"Bearer {token}"
                }


def daterange(start_date, end_date, delta):
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + delta
        yield current_date, min(next_date, end_date)
        current_date = next_date


dates = []
consommation = []
start_date = datetime(2018, 1, 1)
end_date = datetime(2025, 5, 3)
six_months = timedelta(days=30*6)

for start, end in daterange(start_date, end_date, six_months):
    url = f"{base_url}&start_date={start.isoformat()}%2B02:00&end_date={end.isoformat()}%2B02:00"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        for entry in response.json()['short_term'][0]['values']:
            dates.append(entry['start_date'])
            consommation.append(entry['value'])
    else:
        print("Request failed")

df = pd.DataFrame({"Date": dates, "Consommations": consommation})
df['Date'] = pd.to_datetime(df["Date"], format="%Y-%m-%dT%H:%M:%S%z", utc=True)
df['Date'] = df['Date'].dt.strftime("%Y-%m-%d %H:%M:%S")
df.to_csv("./dataset_historique_production_pv.csv", index=False)

