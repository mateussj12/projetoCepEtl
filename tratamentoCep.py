import pandas as pd 
import json as js
from sqlalchemy import String, create_engine as ce
from dotenv import load_dotenv
import os

load_dotenv(os.path.join(os.path.dirname(__file__), 'env', '.env'))

caminhoArquivoJson = "cep.json"
with open(caminhoArquivoJson, "r", encoding="utf-8") as file:
    data = js.load(file)


df = pd.json_normalize(data)
df = df.dropna()

host = os.getenv("POSTGRESQL_HOST")
port = os.getenv("POSTGRESQL_PORT")
database = os.getenv("POSTGRESQL_DATABASE")
user = os.getenv("POSTGRESQL_USER")
password = os.getenv("POSTGRESQL_PASSWORD")


motor = ce(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
df.to_sql(name='cep_brasil', con=motor, if_exists='replace', index=False, schema='staging', 
          dtype={
              'cep': String,
              'cidade': String,
              'estado': String,
              'bairro': String,
              'rua': String
              }
          )