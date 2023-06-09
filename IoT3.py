import random
import time
import psycopg2
from datetime import datetime
import uuid
from config import database_config  

def connect_db():
    conn = psycopg2.connect(**database_config)
    return conn

def close_db(conn):
    conn.close()

def create_table_if_not_exists(conn):
    cur = conn.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS bank_transactions (
            transaction_id UUID PRIMARY KEY,
            account_id VARCHAR(50),
            transaction_type VARCHAR(10),
            amount DECIMAL(10,2),
            timestamp TIMESTAMP
        );
    """
    cur.execute(create_table_query)
    conn.commit()

def insert_data(conn, data):
    cur = conn.cursor()

    insert_query = f"""
        INSERT INTO bank_transactions (transaction_id, account_id, transaction_type, amount, timestamp)
        VALUES ('{data['transaction_id']}', '{data['account_id']}', '{data['transaction_type']}', {data['amount']}, '{data['timestamp']}');
    """
    cur.execute(insert_query)
    conn.commit()

def emulate_bank_transactions():
    conn = connect_db()
    try:
        create_table_if_not_exists(conn)
        while True:
            transaction_data = {
                'transaction_id': str(uuid.uuid4()),
                'account_id': 'account_' + str(random.randint(1000, 9999)),
                'transaction_type': random.choice(['deposit', 'withdrawal']),
                'amount': round(random.uniform(100.0, 1000.0), 2),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            insert_data(conn, transaction_data)
            time.sleep(1)
    finally:
        close_db(conn)

if __name__ == "__main__":
    emulate_bank_transactions()
