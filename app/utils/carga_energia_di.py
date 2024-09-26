import os 
import requests
import pandas as pd

url_base = 'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/'
years = range(2000, 2025)
urls = [f'CARGA_ENERGIA_{year}.csv' for year in years]
# Criar um DataFrame para armazenar os valores dos ETags
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

def download_file(url, save_dir='data/'):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    response = requests.get(f'{url_base}{url}')
    if response.status_code == 200:
        file_path = os.path.join(save_dir, url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f'Arquivo {url} baixado com sucesso!')
    else:
        print(f'Erro ao baixar o arquivo {url}: Status {response.status_code}')

def check_and_update_files(url_list, save_dir='data/'):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    for url in url_list:
        file_path = os.path.join(save_dir, url)
        current_etag = get_etag(url)
        stored_etag = etag_df.loc[etag_df['URL'] == url, 'ETag'].values[0] if not etag_df[etag_df['URL'] == url].empty else None
        if not os.path.exists(file_path) or current_etag != stored_etag:
            download_file(url, save_dir)
            etag_df.loc[etag_df['URL'] == url, 'ETag'] = current_etag if stored_etag else etag_df.append({'URL': url, 'ETag': current_etag}, ignore_index=True)
            print(f'{url} foi atualizado ou baixado.')
        else:
            print(f'{url} já está atualizado.')

# Obter ETags iniciais e preencher o DataFrame
for url in urls:
    etag_df.loc[len(etag_df)] = [url, get_etag(url)]

# Verificar e atualizar os arquivos
check_and_update_files(urls)

# Exibir o DataFrame com os valores dos ETags
print(etag_df)
pd.read_csv('data/CARGA_ENERGIA_2021.csv', sep=';', decimal=',')

