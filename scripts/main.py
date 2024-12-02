from extract import extract_from__rdbms, extract_from__api
from load import load_to__biquery, load_to__bucket
import pyarrow.parquet as pq

def main():
    connection_string = {
        "user": "avnadmin",
        "password": "AVNS_1SIOWzKtEqea945vlFX",
        "host": "mysql-1c4fbc9b-christiansakandelsi-fc70.f.aivencloud.com", 
        "port": 21394,
        "database": "devlivrina_recette",
        "table_name": "model_has_roles"
        }

    #df = extract_from__rdbms(db_type="mysql", connection_info=connection_string)
    
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet'
    #url= 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo&datatype=csv'
    print(url)
    
    #df = extract_from__api(url=url, data_format='parquet')
    df = extract_from__rdbms('mysql', connection_string)
    print(df)

    load_to__biquery(
        df,
        credential_path='C:/Users/chris/OneDrive/Bureau/ML_&_Enginneering_Projects/etl_with_python/scripts/keys/my-gcp-key.json',
        project_id="tidy-gravity-437016-s4", 
        dataset_name="stock_market_data", 
        table_id="load_df_bigquery"
    )

    load_to__bucket(
        df,
        credential_path='C:/Users/chris/OneDrive/Bureau/ML_&_Enginneering_Projects/etl_with_python/scripts/keys/my-gcp-key.json',
        bucket_name='data_of_my_all_etl_projects',
        folder_name='Row_data',
        file_name='model_has_permission'
    )
if __name__ == "__main__":
    main()