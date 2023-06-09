import random
import time
import json
import psycopg2
from datetime import datetime
from config import database_config 
def connect_db():
    conn = psycopg2.connect(**database_config)
    return conn

def connect_db():
    conn = psycopg2.connect(**database_config)
    return conn

def close_db(conn):
    conn.close()

def create_table_if_not_exists(conn):
    cur = conn.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS temperature_data (
            device_id VARCHAR(50),
            temperature DECIMAL(4,2),
            timestamp TIMESTAMP
        );
    """
    cur.execute(create_table_query)
    conn.commit()

def insert_data(conn, data):
    cur = conn.cursor()

    insert_query = f"""
        INSERT INTO temperature_data (device_id, temperature, timestamp)
        VALUES ('{data['device_id']}', {data['temperature']}, '{data['timestamp']}');
    """
    cur.execute(insert_query)
    conn.commit()

def emulate_iot_device():
    conn = connect_db()
    try:
        create_table_if_not_exists(conn)
        while True:
            temperature_data = {
                'device_id': 'device_001',
                'temperature': round(random.uniform(20.0, 30.0), 2),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            insert_data(conn, temperature_data)
            time.sleep(1)
    finally:
        close_db(conn)

if __name__ == "__main__":
    emulate_iot_device()