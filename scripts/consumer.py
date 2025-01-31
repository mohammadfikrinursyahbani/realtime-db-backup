from kafka import KafkaConsumer
import json
import polars as pl
import duckdb

def insert_data(config: dict, data: pl.DataFrame):
    conn = duckdb.connect()
    conn.sql('INSTALL postgres')
    conn.sql('LOAD postgres')
    conn.sql(f"""
        ATTACH 'host = {config['host_backup']} dbname = {config['database_backup']} user = {config['username_backup']} password = {config['password_backup']} port = {config['port_backup']}' AS db_test (TYPE postgres)
    """)
    conn.sql('''
        INSERT INTO db_test.public.dummy_data
        SELECT * FROM data
    ''')
    print('Data inserted successfully')

if __name__ == '__main__':
    from config import read_config
    consumer = KafkaConsumer("test", bootstrap_servers='localhost')
    print("Starting the consumer")
    config = read_config()
    for msg in consumer:
        data = json.loads(msg.value)
        print(data)
        # data = pl.from_dicts(data)
        # insert_data(config, data)