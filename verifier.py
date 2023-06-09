import pandas as pd
from sqlalchemy import create_engine
from config import database_config  
def create_db_engine():
    database_url = f"postgresql+psycopg2://{database_config['user']}:{database_config['password']}@{database_config['host']}:{database_config['port']}/{database_config['dbname']}"
    engine = create_engine(database_url)
    return engine

def check_data_quality():
    engine = create_db_engine()

    with engine.connect() as connection:
        query = "SELECT COUNT(*) FROM bank_transactions"
        result = connection.execute(query)
        print("Number of rows in 'bank_transactions': ", result.fetchone())
    
    query = "SELECT * FROM bank_transactions"
    df = pd.read_sql(query, con=engine)

    missing_values = df.isnull().sum()
    print("Missing values: \n", missing_values)

    duplicate_rows = df.duplicated().sum()
    print("Duplicate rows: ", duplicate_rows)

    data_types = df.dtypes
    print("Data types: \n", data_types)

    inconsistent_transaction_types = df[~df['transaction_type'].isin(['deposit', 'withdrawal'])]
    print("Inconsistent transaction types: \n", inconsistent_transaction_types)

if __name__ == "__main__":
    check_data_quality()
