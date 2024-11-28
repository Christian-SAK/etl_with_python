import pandas as pd
import numpy as np
import requests as rq
from io import BytesIO
from pyarrow.lib import ArrowInvalid
import json
import pymysql
import psycopg2



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
            return pd.read_parquet(BytesIO(response.content))
        except ArrowInvalid as e:
            print(f"Erreur : {e} \nFormat de données incorrect")

    else:
        raise ValueError(f"Format de données non pris en charge : {data_format}")



def extract_from__rdbms(db_type = 'mysql', connection_info=None, schema=None):
    
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
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.description)
        conn.close()
        return df
    
    except Exception as e:
        print(f"Error extracting data from {connection_info['table_name']}: {e}")
        return None
