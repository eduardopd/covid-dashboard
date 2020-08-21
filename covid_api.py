import requests
import json
from datetime import datetime, timedelta


def uf_deaths_total(uf):
    """
    Return the total number of deaths per UF
    
    """
    deaths_total = []
    
    url = 'https://covid19-brazil-api.now.sh/api/report/v1/brazil/uf/'
    
    response = requests.get(url + uf)
    json_response = response.json()

    deaths = json_response['deaths']

    return deaths;

def uf_deaths_past_weeks(uf, weeks):
    """
    Return the total number of deaths for past weeks
    
    """

    past_days_deaths = []

    for week in range (1, weeks+1):
        
        past_days = week*7

        past_date = datetime.today() - timedelta(days=past_days)
        past_date = past_date.strftime("%Y%m%d")

        url = 'https://covid19-brazil-api.now.sh/api/report/v1/brazil/'

        response = requests.get(url + past_date)
        json_response = response.json()

        # Capitalize UF to match API response JSON
        uf = uf.upper()

        for uf_data in json_response['data']:
            if uf_data['uf'] == uf:
                past_days_deaths.append(uf_data['deaths'])
                
    return(past_days_deaths)

def uf_deaths_per_day(uf, days):
    """
    Return the number of deaths per day
    
    """ 

    deaths_per_day = []
    date = []
    endpoint1 = 'https://covid19-brazil-api.now.sh/api/report/v1/brazil/uf/'

    today = datetime.today()
    today_r = requests.get(endpoint1 + uf)
    today_r = today_r.json()
    
    total = today_r['deaths']

    for day in range(1, days+1):

        endpoint2 = 'https://covid19-brazil-api.now.sh/api/report/v1/brazil/'        
        yesterday = today - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y%m%d")

        # Make the request to the API
        yesterday_r = requests.get(endpoint2 + yesterday_str)
        yesterday_r = yesterday_r.json()

        # Query the response
        uf = uf.upper()

        for uf_data in yesterday_r['data']:
            if uf_data['uf'] == uf:
                diff = total - uf_data['deaths']
                
                deaths_per_day.append(diff)
                date.append(yesterday)
                
                total = uf_data['deaths']

        today = yesterday

    deaths_per_day = list(deaths_per_day)
    return date, deaths_per_day
    

