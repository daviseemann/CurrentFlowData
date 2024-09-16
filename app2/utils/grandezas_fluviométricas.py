import pandas as pd
import requests
import os

from app2.utils.carga_energia_di import check_and_update_files
  

url_base = ' https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/grandezas_fluviometricas_di/'
years = range(2015, 2025)
urls = [f'GRANDEZAS_FLUVIOMETRICAS_{year}.csv' for year in years]

etag_df = pd.DataFrame(columns=['URL', 'ETag'])

def get_etag(url):
    response = requests.head(f'{url_base}{url}')
    etag = response.headers.get('ETag')
    
    # Adicionar a URL e o etag ao DataFrame
    etag_df.loc[len(etag_df)] = [url, etag]
    
    return etag

# Iterar sobre a lista de URLs para obter os ETags
for url in urls:
    get_etag(url)

# Exibir o DataFrame com os valores dos ETags
#print(etag_df)

for url in urls:
    etag_df.loc[len(etag_df)] = [url, get_etag(url)]

# Verificar e atualizar os arquivos
check_and_update_files(urls)

# Exibir o DataFrame com os valores dos ETags
print(etag_df)
pd.read_csv('data/CARGA_ENERGIA_2021.csv', sep=';', decimal=',')



