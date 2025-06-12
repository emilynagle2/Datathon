# snowflake_utils.py
import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# Define constants
DATABASE_SCHEMA = "EVENT.DATATHON_2025_TEAM_ETA"

def get_snowflake_connection():
    """Get a connection to Snowflake using environment variables"""
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
    """Execute a query and return results as a DataFrame"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    
    columns = [col[0] for col in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return pd.DataFrame(results)

def upload_csv_to_snowflake(csv_path, table_name):
    """Upload a CSV file to a Snowflake table"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    # Create a stage if it doesn't exist
    cursor.execute("CREATE STAGE IF NOT EXISTS my_csv_stage")
    
    # Put the file into the stage
    put_command = f"PUT file://{csv_path} @my_csv_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cursor.execute(put_command)
    
    # Copy data from staged file into the table
    copy_command = f"""
    COPY INTO {DATABASE_SCHEMA}.{table_name}
    FROM @my_csv_stage/{os.path.basename(csv_path)}.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    """
    cursor.execute(copy_command)
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {DATABASE_SCHEMA}.{table_name}")
    row_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return row_count
