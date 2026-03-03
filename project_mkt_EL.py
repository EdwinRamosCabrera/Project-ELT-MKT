import pandas as pd
import pandas_gbq
import requests
import pandas_gbq
from google.oauth2 import service_account

def get_data(start_date, end_date, path_file):
    url = f'https://my.api.mockaroo.com/project_mkt_elt.json?start_date={start_date}&end_date={end_date}'
    headers = { 'X-API-Key': '26950f30' }
    r = requests.get(url, headers=headers)
    with open(path_file, 'wb') as f:
        f.write(r.content)
    print(f'{path_file} created')

def load_to_bigquery(path_file, source):
    credentials = service_account.Credentials.from_service_account_file('credentials/prueba-mkt-elt-c9b16f9e74dd.json',
    )
    df = pd.read_csv(path_file)
    df = df.assign(source=source)
    # Convertir la columna 'Date' a formato datetime porque ese formato es que que se espera en BigQuery para las fechas
    df.Date = pd.to_datetime(df.Date)

    pandas_gbq.to_gbq(
    df, f'bronce_mkt.{source}_raw', project_id='prueba-mkt-elt', if_exists='append', credentials=credentials
    )

source = 'youtube'
start_date = '1/1/2021'
end_date = '1/31/2021'
path_file = f'data/campaign_{source}_{start_date.replace("/", "-")}_{end_date.replace("/", "-")}.csv'
get_data(start_date, end_date, path_file)
load_to_bigquery(path_file, source)