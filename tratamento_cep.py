import pandas as pd 
import json as js
from sqlalchemy import String, create_engine as ce
from dotenv import load_dotenv
import os

print("""
        ██╗███████╗███████╗██╗   ██╗███████╗
        ██║██╔════╝██╔════╝██║   ██║██╔════╝
        ██║█████╗  ███████╗██║   ██║███████╗
   ██   ██║██╔══╝  ╚════██║██║   ██║╚════██║
   ╚█████╔╝███████╗███████║╚██████╔╝███████║
    ╚════╝ ╚══════╝╚══════╝ ╚═════╝ ╚══════╝
                                                 
    """)
print("""
    **Estado**: Brasilia - DF 
    **Desenvolvedor responsável**: Mateus Santos de Jesus
    **Objetivo**: Esse script lê os dados de um arquivo JSON com amostras dos CEPs do Brasil, 
    converte para dataframe, faz conexão com um banco de dados postgresql que foi instalado em uma distribuição linux na núvem Azure,
    cria a tabela necessária com os tipos de dados necessários e envia esses dados tratados para o banco de dados...
    """)

print("# Carregando o arquivo de variáveis")
# Carrega o arquivo de váriáveis
load_dotenv(os.path.join(os.path.dirname(__file__), 'env', '.env'))

print("# Carregando e lendo o arquivo JSON")
# Carrega e lê o arquivo JSON e armazena na variável 'data'
caminhoArquivoJson = "cep.json"
with open(caminhoArquivoJson, "r", encoding="utf-8") as file:
    data = js.load(file)

print("# Transformando os dados do Arquivo JSON para Dataframe")
# Transforma os dados do Arquivo JSON para Dataframe e limpa os valores NaN
df = pd.json_normalize(data)
df = df.dropna()

print("# Recebendo parametros de conexão do banco de dados")
# Recebe os parametros de conexão com o banco de dados e armazena em variáveis de ambiente
host = os.getenv("POSTGRESQL_HOST")
port = os.getenv("POSTGRESQL_PORT")
database = os.getenv("POSTGRESQL_DATABASE")
schema = os.getenv("POSTGRESQL_SCHEMA")
user = os.getenv("POSTGRESQL_USER")
password = os.getenv("POSTGRESQL_PASSWORD")

print("# Realizando conexão com banco de dados, criando a tabela necessária e enviando os dados")
# Realiza a conexão com banco de dados, cria a tabela necessária e envia os dados 
motor = ce(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
df.to_sql(name='cep_brasil', con=motor, if_exists='replace', index=False, schema=schema, 
          dtype={
              'cep': String,
              'cidade': String,
              'estado': String,
              'bairro': String,
              'rua': String
              }
          )