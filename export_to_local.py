# Add this to your Jupyter notebook

import os
import pandas as pd
import dotenv as dot
import snowflake.connector

def get_snowflake_connection():
    """
    Create a connection to Snowflake using credentials from .env file
    """
    # Load environment variables
    dot.load_dotenv()
    
    # Get connection parameters from environment variables
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    
    return conn

def query_to_df(query):
    """
    Execute a query and return the results as a pandas DataFrame
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)

    # Get column names
    columns = [col[0] for col in cursor.description]

    # Fetch all rows and convert to list of dictionaries
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return pd.DataFrame(results)


conn = get_snowflake_connection()
cursor = conn.cursor()

cursor.execute(f"""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'DATATHON_2025_TEAM_ETA'
    AND table_catalog = 'EVENT'
    ORDER BY table_type, table_name
""")

tables_and_views = cursor.fetchall()

# Export each table/view to CSV
for table_info in tables_and_views:
    table_name = table_info[0]
    table_type = table_info[1]
    
    print(f"Exporting {table_type}: {table_name}")
    

    df = query_to_df(f"SELECT * FROM EVENT.DATATHON_2025_TEAM_ETA.{table_name}")
    path = os.getenv("CSV_DIR") + table_name +'.csv'
    df.to_csv(path, index=False)
    
    print(f"  - Exported {len(df)} rows to {path}")

cursor.close()
conn.close()
