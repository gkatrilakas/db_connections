import pandas as pd
import snowflake.connector

def sf_sql_query(query):
    conn = snowflake.connector.connect(
                user = 'USER@DOMAIN.com',
                password = 'PASSWORD',
                accessUrl='https://emeaprod01.eu-west-1.privatelink.snowflakecomputing.com',
                account='emeaprod01.eu-west-1.privatelink',
                role="TEST_ROLE",
                warehouse='TEST_WAREHOUSE',
                database='TEST_DATABASE',
                schema='TEST_SCHEMA'
                )
    # Submit an asynchronous query for execution.
    cur = conn.cursor()
    cur.execute(f"{query}")
    res = cur.fetch_pandas_all()
    
    return res
