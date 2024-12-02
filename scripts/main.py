from extract import extract_from__rdbms, extract_from__api
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
    #print(url)
    #url= 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo&datatype=csv'
    df = extract_from__api(url=url, data_format='parquet')
    print(df)


if __name__ == "__main__":
    main()