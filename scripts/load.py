import time
import pymysql
import psycopg2
from google.cloud import bigquery, storage
from google.oauth2 import service_account


def get_sql_type(pd_type):
    """
    Mappe les types de données Pandas aux types de données SQL.

    Arguments :
        pd_type (str) : Le type de données Pandas.

    Retourne :
        str : Le type de données SQL correspondant.
    """

    mapping = {
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'object': 'TEXT',  
        'datetime64[ns]': 'TIMESTAMP'
    }

    return mapping.get(str(pd_type), 'TEXT')


def load_to__rdbms(df, db_type = 'mysql', connection_info=None, schema=None):
    
    """
    Charge un DataFrame Pandas dans une table de base de données spécifiée.
    Arguments :
        db_type (str) : Le type de base de données (par exemple, 'postgresql', 'mysql').
        connection_info (dict) : Un dictionnaire contenant les informations de connexion.
        df (pd.DataFrame) : Le DataFrame à charger.
        table_name (str) : Le nom de la table cible.
    Retourne :
        None
    """

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

        cursor.execute("SET SESSION sql_require_primary_key = OFF;")
        
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {connection_info['table_name']} (
                {', '.join([f"{col} {get_sql_type(df[col].dtype)}" for col in df.columns])}
            )
        """)

        
        for index, row in df.iterrows():
            values = tuple(row)
            sql = f"INSERT INTO {connection_info['table_name']} VALUES %s"
            cursor.execute(sql, (values,))

        conn.commit()
        cursor.close()
        conn.close()
        
        return "Table chargée avec succès !"

    except Exception as e:
        print(f"Error loading data to {connection_info['table_name']}: {e}")


def load_to__biquery(df, credential_path, project_id, dataset_name, table_id, mode = 'WRITE_TRUNCATE'):
    credentials = service_account.Credentials.from_service_account_file(credential_path)
    client_bigquery = bigquery.Client(project = project_id, credentials=credentials)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # Options : WRITE_APPEND, WRITE_EMPTY

    table_name = project_id + '.' + dataset_name + '.' + table_id

    job = client_bigquery.load_table_from_dataframe(df, table_name, job_config=job_config)
    job.result()  # Attendre la fin du chargement

    return print(f"Chargement terminé. Nombre de lignes : {job.output_rows}")

def load_to__biquery_spark(objet_spark, project_name, dataset_name, table_name, mode = 'overwrite'):
    start_time = time.time()
    table_id = f"{project_name}:{dataset_name}.{table_name}"
    objet_spark.write\
        .format("bigquery")\
        .option("parentProject", project_name)\
        .option("writeMethod", "direct") \
        .option("createDisposition", "CREATE_IF_NEEDED")\
        .mode(mode)\
        .save(table_id)
    # Calcul et affichage du temps écoulé
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Écriture terminée. Temps écoulé: {elapsed_time:.2f} secondes")


def load_to__bucket(df, credential_path, bucket_name, folder_name='dossier_test', file_name='test.csv'):
    """
    Télécharge un DataFrame pandas dans un bucket Google Cloud Storage en format CSV.

    Arguments :
        df (pd.DataFrame): DataFrame pandas à charger.
        credential_path (str): Chemin vers le fichier JSON des credentials GCP.
        bucket_name (str): Nom du bucket Google Cloud Storage.
        folder_name (str, optional): Nom du dossier dans le bucket. Par défaut 'dossier_test'.
        file_name (str, optional): Nom du fichier dans le bucket. Par défaut 'test.csv'.

    Retourne :
        bool: True si l'opération a réussi, False sinon.
    """
    try:
        # Créer un client pour interagir avec Google Cloud Storage
        client_gsc = storage.Client.from_service_account_json(credential_path)

        # Accéder au bucket
        bucket = client_gsc.bucket(bucket_name)

        # Construire le chemin cible (chemin dans le bucket)
        destination_blob_name = f"{folder_name}/{file_name}"

        # Créer un blob (objet dans le bucket)
        blob = bucket.blob(destination_blob_name)

        # Charger le DataFrame en tant que chaîne CSV dans le bucket
        blob.upload_from_string(df.to_parquet(engine="pyarrow", index=False), content_type='application/octet-stream')

        print(f"Fichier chargé avec succès dans gs://{bucket_name}/{destination_blob_name}.")
        return True

    except Exception as e:
        print(f"Erreur lors du chargement : {e}")
        return False


def load_to__bucket_spark(objet_spark, bucket_name, folder_name, format='json'):
    destination_path = f"gs://{bucket_name}/{folder_name}"
    start_time = time.time()
    
    if format == 'json':
        #df = df.toJSON()
        objet_spark.write.mode("append").json(destination_path, lineSep="\n")
    elif format == 'parquet':
        objet_spark.write.mode("append").parquet(destination_path)
    else:
        objet_spark.write.mode("append").option("header", "true").csv(destination_path)
    
    # Calcul et affichage du temps écoulé
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Écriture terminée. Temps écoulé: {elapsed_time:.2f} secondes")