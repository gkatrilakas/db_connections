import boto3
from botocore.exceptions import ClientError
import psycopg2
import pandas as pd
import json
from sqlalchemy import create_engine
 
from dotenv import load_dotenv
import os

load_dotenv()

def data_to_rds(response_dict, table_name, sql_method='replace'): # For sql_method we need a list of: replace or append
    """
    Function that takes a dictionary - user's feedback -
    turns into a dataframe and uploads it to a Postgre DB
    """
    try:
        # Create the Dataframe from JSON file
        df = pd.DataFrame(response_dict)
        # Replace the placeholders with your RDS connection details
        db_username = os.getenv('DB_USERNAME')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')

        # Create the connection string
        connection_string = f'postgresql://{db_username}:{db_password}@{db_host}/{db_name}'

        # Create the engine
        engine = create_engine(connection_string)

        # Uploads a Dataframe to postgres DB
        df.to_sql(
            name=table_name,
            con=engine,
            index=False,
            if_exists=sql_method)

        return 'Success'
    
    except Exception as e:
        return f'#### Error spotted: {e} ####'
    
def get_aurora_content(table_name):
    """
    Query the db for testing purposes
    """
    # Get the credentials from .aws/secretsmanager
    session = boto3.Session(region_name=db_region, aws_secret_access_key=db_password)
    client = session.client('rds')
    
    try:
        
        conn =  psycopg2.connect(
            host=os.getenv('DB_ENDPOINT'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            sslrootcert='SSLCERTIFICATE')
        
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{table_name}";')  
        
        query_results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(query_results, columns = colnames)
        
    except Exception as e:
        cur.close()
        conn.close()
        
        return("Database connection failed due to {}".format(e))
    finally:
        cur.close()
        conn.close()
    
    return df


# Demo dict
feedback_test = {
    'User': ['user_id_'],
    'Incident': ['incident'],
    'Response': [str('result')],
    'Stars': ['star'],
    'Comment': ['comment']
}

creds_test = {
    'user_id':['tester'],
    'pass': ['1234']
}

print(data_to_rds(feedback_test, 'user_feedback', sql_method='replace'))
print(get_aurora_content('user_feedback'))

print(pandas_to_rds(dict_test, 'user_creds', sql_method='replace'))
print(get_aurora_content('user_creds'))
