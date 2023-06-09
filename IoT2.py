import random
import time
import json
import psycopg2
from datetime import datetime

# Configuration for the database
database_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Manonthemoon123',
    'host': 'database-1.cueq5a3aruqx.us-east-2.rds.amazonaws.com',
    'port': '5432'  # default is 5432
}


def connect_db():
    # Connect to your postgres DB
    conn = psycopg2.connect(**database_config)
    return conn

def close_db(conn):
    # Close the connection
    conn.close()

def create_table_if_not_exists(conn):
    # Create the table if it doesn't exist
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
    # Insert data into the DB
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
            # Creating random temperature data
            temperature_data = {
                'device_id': 'device_001',
                'temperature': round(random.uniform(20.0, 30.0), 2),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            # Sending data
            insert_data(conn, temperature_data)
            # Emulate data sending each 10 seconds
            time.sleep(1)
    finally:
        close_db(conn)

if __name__ == "__main__":
    emulate_iot_device()