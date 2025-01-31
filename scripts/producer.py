import polars as pl
import duckdb, json
from kafka import KafkaProducer
import time

def pl_read_database(config: dict) -> pl.DataFrame:
    uri = f"postgresql://{config['username_main']}:{config['password_main']}@{config['host']}:{config['port']}/{config['database_main']}"
    query = "select * from public.dummy_data"
    df = pl.read_database_uri(uri = uri, query = query, engine = 'adbc')
    return df

def duck_read_database(config: dict) -> pl.DataFrame:
    conn = duckdb.connect()
    conn.sql('INSTALL postgres')
    conn.sql('LOAD postgres')
    conn.sql(f"""
        ATTACH 'host = {config['host']} dbname = {config['database_main']} user = {config['username_main']} password = {config['password_main']} port = {config['port']}' AS db_test (TYPE postgres)
    """)
    df = conn.sql('SELECT * FROM db_test.public.dummy_data').pl()
    conn.close()
    return df

def producer_kafka(config: dict, read_from: str):
    if read == 'polars':
        df = pl_read_database(config)
    elif read == 'duckdb':
        df = duck_read_database(config)
    json_data = df.to_dicts()
    producer = KafkaProducer(
        bootstrap_servers = ['localhost'],
        value_serializer = lambda x: json.dumps(x).encode('utf-8')
    )
    while True:
        print('Sending data to Kafka...')
        for data in json_data:
            print(data)
            producer.send('test', value = data)
            time.sleep(5)
        break

if __name__ == '__main__':
    from config import read_config
    config = read_config()
    read = 'duckdb'
    producer_kafka(config, read)
