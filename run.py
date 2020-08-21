from covid_api import uf_deaths_total, uf_deaths_past_weeks, uf_deaths_per_day
import pandas as pd
import csv

states = ['rj', 'mg', 'sp']

total_deaths = []

for uf in states:

    #deaths_total = uf_deaths_total(uf)
    #deaths_past_weeks = uf_deaths_past_weeks(uf, 5)
    date, deaths_per_day = uf_deaths_per_day(uf, 100)

    # Save to CSV
    file_name = (uf + '_deaths_per_day.csv')

    with open(file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(zip(date, deaths_per_day))
    
    uf = uf.upper()
    print ('Done '+ uf)


