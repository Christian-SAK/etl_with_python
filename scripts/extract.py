import pandas as pd
import requests as rq
import json
import pymysql
import psycopg2
import pyarrow.parquet as pq

from io import BytesIO
from pyarrow.lib import ArrowInvalid # Type Error



def extract_from__api(url, data_format='json'):
    VALID_FORMATS = ['json', 'csv', 'parquet']

    if data_format not in VALID_FORMATS:
        raise ValueError(f"Format de données invalide. Les formats valides sont : {', '.join(VALID_FORMATS)}")
    # Effectuer la requête GET
    
    response = rq.get(url)
    
    # Vérifier si la réponse est valide
    if response.status_code != 200:
        raise Exception(f"URL incorrecte. Code de statut : {response.status_code}")

    # Traiter selon le format de données
    if data_format == 'json':
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print(f"Erreur : {e} \nFormat de données incorrect")
    
    elif data_format == 'csv':
        return response.text  # CSV sous forme de texte brut
    
    elif data_format == 'parquet':
        # Lire le fichier Parquet directement depuis la mémoire
        try:
            # Lire le fichier Parquet
            
            return pq.read_table(BytesIO(response.content))
        except ArrowInvalid as e:
            print(f"Erreur : {e} \nFormat de données incorrect")

    else:
        raise ValueError(f"Format de données non pris en charge : {data_format}")



def extract_from__rdbms(
        db_type = 'mysql'
        , connection_info = {
            "user": None,
            "password": None,
            "host": None, 
            "port": None,
            "database": None,
            "table_name": None
        }
    ):
    
    try:
        if db_type == "postgresql":
            conn = psycopg2.connect(
                user=connection_info['user'],
                password=connection_info['password'],
                host=connection_info['host'],
                port=connection_info['port'],
                database=connection_info['database']  
            )
        
        elif db_type == "mysql":
            conn = pymysql.connect(
                user=connection_info['user'],
                password=connection_info['password'],
                host=connection_info['host'],
                port=connection_info['port'],
                database=connection_info['database']  
            )
        
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {connection_info['table_name']}")
        column_names = [col[0] for col in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        conn.close()
        return df
    
    except Exception as e:
        print(f"Error extracting data from {connection_info['table_name']}: {e}")
        return None

#url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet'
#print(url)
#url= 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo&datatype=csv'
#extract_from__api(url=url, data_format='csv')
