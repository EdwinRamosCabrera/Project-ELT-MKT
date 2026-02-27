import pandas as pd
import requests

def get_data(source, start_date, end_date):
    url = f'https://my.api.mockaroo.com/project_mkt_elt.json?start_date={start_date}&end_date={end_date}'
    headers = { 'X-API-Key': '26950f30' }
    r = requests.get(url, headers=headers)
    start_date = start_date.replace("/", "-")
    end_date = end_date.replace("/", "-")
    path_file = f'data/campaign_{source}_{start_date}_{end_date}.csv'
    with open(path_file, 'wb') as f:
        f.write(r.content)
    print(f'campaign_{source}_{start_date}_{end_date}.csv created')

start_date = '1/1/2021'
end_date = '1/31/2021'
get_data('youtube', start_date, end_date)
