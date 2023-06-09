import pandas as pd
import psycopg2
from psycopg2 import sql
from config import database_config  
import datetime

def create_db_conn():
    conn = psycopg2.connect(
        dbname = database_config['dbname'],
        user = database_config['user'],
        password = database_config['password'],
        host = database_config['host'],
        port = database_config['port']
    )
    return conn

def check_data_quality(conn):
    cursor = conn.cursor()
    
    # Query to count number of rows
    query = "SELECT COUNT(*) FROM bank_transactions"
    cursor.execute(query)
    num_rows = cursor.fetchone()[0]

    # Query to get all data
    query = "SELECT * FROM bank_transactions"
    df = pd.read_sql_query(query, con=conn)

    missing_values = df.isnull().sum()
    duplicate_rows = df.duplicated().sum()
    data_types = df.dtypes
    inconsistent_transaction_types = df[~df['transaction_type'].isin(['deposit', 'withdrawal'])]

    return num_rows, missing_values, duplicate_rows, data_types, inconsistent_transaction_types

def write_to_file(num_rows, missing_values, duplicate_rows, data_types, inconsistent_transaction_types):
    filename = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "_data_quality.csv"
    with open(filename, 'w') as f:
        f.write("Number of rows in 'bank_transactions': " + str(num_rows) + "\n")
        f.write("Missing values: \n" + str(missing_values) + "\n")
        f.write("Duplicate rows: " + str(duplicate_rows) + "\n")
        f.write("Data types: \n" + str(data_types) + "\n")
        f.write("Inconsistent transaction types: \n" + str(inconsistent_transaction_types) + "\n")

if __name__ == "__main__":
    conn = create_db_conn()
    num_rows, missing_values, duplicate_rows, data_types, inconsistent_transaction_types = check_data_quality(conn)
    write_to_file(num_rows, missing_values, duplicate_rows, data_types, inconsistent_transaction_types)
