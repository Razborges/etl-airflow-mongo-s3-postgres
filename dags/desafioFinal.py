from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests
import json
import boto3
import os
from sqlalchemy import create_engine

default_args = {
  'ownew': 'Razborges',
  'depends_on_past': False,
  'start_date': datetime(2020, 12, 30, 18, 10),
  'email': ['airflow@airflow.com'],
  'email_on_failure': False,
  'email_on_retry': False
}

FILE_PATH_PNADC = '/tmp/pnadc20203.csv'
FILE_PATH_MICRORREGIOES = '/tmp/dimensao_mesorregioes_mg.csv'

@dag(default_args=default_args, schedule_interval=None, description='ETL de daos do IBGE para Data Lake e DW - Desafio Final IGTI Eng. Dados')
def etl_final():
  '''
  Um flow para obter dados do IBGE de uma base MongoDB, da API de microregiões do IBGE,
  depositar no data lake no s3 e no DW em um postgresql
  '''
  @task
  def get_data_mongo():
    import pymongo
    import pandas as pd

    MONGO_USER = Variable.get('MONGO_USER')
    MONGO_PASS = Variable.get('MONGO_PASS')
    MONGO_HOST = Variable.get('MONGO_HOST')
    MONGO_BASE = Variable.get('MONGO_BASE')

    client = pymongo.MongoClient(f'mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}{MONGO_BASE}?retryWrites=true&w=majority')
    db = client.ibge
    collection = db.pnadc20203
    df = pd.DataFrame(list(collection.find()))
    df.to_csv(FILE_PATH_PNADC, index=False, encoding='utf-8', sep=';')
    return FILE_PATH_PNADC
  
  @task
  def get_data_api():
    import pandas as pd

    PATH_IBGE_CURL = Variable.get('PATH_IBGE_CURL')

    response = requests.get(PATH_IBGE_CURL)
    resjson = json.loads(response.text)
    df = pd.DataFrame(resjson)[['id', 'nome']]
    df.to_csv(FILE_PATH_MICRORREGIOES, sep=';', index=False, encoding='utf-8')
    return FILE_PATH_MICRORREGIOES
  
  @task
  def upload_to_s3(file_name):
    print(f'Got filename: {file_name}')

    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
    S3_NAME_BUCKET = Variable.get('S3_NAME_BUCKET')
    S3_HOST_BUCKET = Variable.get('S3_HOST_BUCKET')

    logging.info(f'{S3_HOST_BUCKET} {S3_NAME_BUCKET}')

    s3_client = boto3.client(
      's3',
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      endpoint_url=S3_HOST_BUCKET,
      use_ssl=False
    )
    s3_client.upload_file(file_name, S3_NAME_BUCKET, file_name[5:])
  
  @task
  def write_to_postgres(csv_file_path):
    import pandas as pd

    POSTGRES_USER = Variable.get('POSTGRES_USER')
    POSTGRES_PASS = Variable.get('POSTGRES_PASS')
    POSTGRES_HOST = Variable.get('POSTGRES_HOST')

    conn = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}')
    df = pd.read_csv(csv_file_path, sep=';')
    if csv_file_path == FILE_PATH_PNADC:
      df = df.loc[(df.idade >= 20) & (df.idade <= 40) & (df.sexo == 'Mulher')]

    df['dt_inclusao_registro'] = datetime.today()
    df.to_sql(csv_file_path[5:-4], conn, index=False, if_exists='replace', method='multi', chunksize=1000)

  mongo = get_data_mongo()
  api = get_data_api()
  up_mongo = upload_to_s3(mongo)
  up_api = upload_to_s3(api)
  wr_mongo = write_to_postgres(mongo)
  wr_api = write_to_postgres(api)

etl_final = etl_final()