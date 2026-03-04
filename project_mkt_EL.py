import pandas as pd
import pandas_gbq
import requests
import pandas_gbq
from google.oauth2 import service_account
from datetime import datetime, timedelta

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

from datetime import datetime, timedelta
def create_rango_fechas():
    anios = [2022, 2023, 2024]
    meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    range_dates = []
    for anio in anios:
        for mes in meses:
            inicio_dt = datetime(anio, mes, 1)
            inicio = f"{inicio_dt.month}/{inicio_dt.day}/{inicio_dt.year}"
            if mes == 12:
                final_dt = datetime(anio, mes, 31) - timedelta(days=1)
                final = f"{final_dt.month}/{final_dt.day}/{final_dt.year}"
            else:
                final_dt = datetime(anio, mes + 1, 1) - timedelta(days=1)
                final = f"{final_dt.month}/{final_dt.day}/{final_dt.year}"
            range_dates.append([inicio, final])
    return range_dates

# source = 'youtube_ads'
# start_date = '1/1/2021'
# end_date = '1/31/2021'
# path_file = f'data/campaign_{source}_{start_date.replace("/", "-")}_{end_date.replace("/", "-")}.csv'
# get_data(start_date, end_date, path_file)
# load_to_bigquery(path_file, source)

# source = 'Facebook', 'Instagram', 'Twitter', 'Google', 'Youtube', 'Email' (todo debe ir en minusculas)
# Rango de fechas: '1/31/2022' - '3/31/2024' [[inicio_mes, fin_mes], [inicio_mes, fin_mes], ...]
sources = ['facebook_ads', 'instagram_ads', 'twitter_ads', 'google_ads', 'youtube_ads', 'email']
date_ranges = create_rango_fechas()[:-9] # Eliminar los últimos 9 meses de 2024 porque no se han completado

for source in sources:
    for date_range in date_ranges:
        start_date = date_range[0]
        end_date = date_range[1]
        path_file = f'data/campaign_{source}_{start_date.replace("/", "-")}_{end_date.replace("/", "-")}.csv'
        get_data(start_date, end_date, path_file)
        load_to_bigquery(path_file, source)