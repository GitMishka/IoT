import pandas as pd
from sqlalchemy import create_engine

database_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Manonthemoon123',
    'host': 'database-1.cueq5a3aruqx.us-east-2.rds.amazonaws.com',
    'port': '5432'  # default is 5432
}

def create_db_engine():
    # Create a SQLAlchemy engine
    database_url = f"postgresql+psycopg2://{database_config['user']}:{database_config['password']}@{database_config['host']}:{database_config['port']}/{database_config['dbname']}"
    engine = create_engine(database_url)
    return engine

def check_data_quality():
    engine = create_db_engine()

    # Open a connection
    with engine.connect() as connection:
        # Try running a simpler query
        query = "SELECT COUNT(*) FROM bank_transactions"
        result = connection.execute(query)
        print("Number of rows in 'bank_transactions': ", result.fetchone())
    
    # Query the data from the database
    query = "SELECT * FROM bank_transactions"
    df = pd.read_sql(query, con=engine)

    # Check for missing values
    missing_values = df.isnull().sum()
    print("Missing values: \n", missing_values)

    # Check for duplicate rows
    duplicate_rows = df.duplicated().sum()
    print("Duplicate rows: ", duplicate_rows)

    # Check data types
    data_types = df.dtypes
    print("Data types: \n", data_types)

    # Check for inconsistencies in 'transaction_type' column
    inconsistent_transaction_types = df[~df['transaction_type'].isin(['deposit', 'withdrawal'])]
    print("Inconsistent transaction types: \n", inconsistent_transaction_types)

if __name__ == "__main__":
    check_data_quality()
