import pandas as pd
import requests
import os

# Base URL para os arquivos CSV
url_base = 'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_'
# Lista de anos para gerar as URLs
years = range(2000, 2025)
# Gerar lista de URLs para os anos especificados
urls = [f'{url_base}{year}.csv' for year in years]

download_dir = './csv_files'

# Função para baixar o CSV da URL
def download_csv(url):
    # Fazer a requisição HTTP para a URL
    response = requests.get(url)
    # Verificar se a requisição foi bem-sucedida
    response.raise_for_status()
    # Ler o conteúdo da resposta diretamente como um DataFrame pandas
    df = pd.read_csv(url)
    filename = os.path.join(download_dir, url.split("/")[-1])
    df.to_csv(filename, index=False)
    return df

# Lista para armazenar todos os DataFrames
data_frames = []

# Obter a lista de arquivos existentes no diretório atual que começam com 'CARGA_ENERGIA_' e terminam com '.csv'
existing_files = [f for f in os.listdir() if f.startswith('CARGA_ENERGIA_') and f.endswith('.csv')]

# Carregar DataFrames dos arquivos existentes
for file in existing_files:
    # Ler cada arquivo existente como um DataFrame e adicionar à lista
    df = pd.read_csv(file)
    data_frames.append(df)

# Baixar novos arquivos e adicioná-los ao data_frames
for url in urls:
    # Extrair o nome do arquivo da URL
    filename = url.split("/")[-1]
    # Verificar se o arquivo já existe
    if filename not in existing_files:
        try:
            # Baixar e ler o CSV da URL como um DataFrame
            df = download_csv(url)
            # Salvar o DataFrame como um arquivo CSV localmente
            df.to_csv(filename, index=False)
            # Adicionar o DataFrame à lista de DataFrames
            data_frames.append(df)
            print(f"Successfully downloaded data for {url}")
        except Exception as e:
            # Caso ocorra um erro, exibir a mensagem de erro
            print(f"Failed to download data for {url}: {e}")
    else:
        # Se o arquivo já existe, pular o download
        print(f"File {filename} already exists. Skipping download.")

# Concatenar todos os DataFrames carregados (novos e existentes)
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
    print(combined_df.head(-1))
    # Salvar o DataFrame combinado em um único arquivo CSV
    combined_df.to_csv("combined_data.csv", index=False)
else:
    print("No data frames to concatenate.")
