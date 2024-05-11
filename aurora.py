import psycopg2
import boto3
import pandas as pd

def aurora_db_conn(query):
    ENDPOINT = 'aurdbcluxxxxxx.cluster-cepitcaxxzss.us-east-1.rds.amazonaws.com'
    PORT = 'xxxx'
    USER = 'username'
    REGION = 'us-east-1'
    DBNAME = 'DBNAME'
    PASS= 'PASSWORD'

    #gets the credentials from .aws/credentials
    session = boto3.Session(region_name = REGION, aws_secret_access_key = PASS)
    client = session.client('rds')
    try:
        conn =  psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASS, sslrootcert='SSLCERTIFICATE')
        cur = conn.cursor()
        query = """
                SELECT DISTINCT("M0_L01") AS "Revenues"
                FROM TEST_DB.TEST_TABLE
                WHERE "M0_L01" = 'International' AND "M0_L02" = 'Europe'
                """

        cur.execute(f'''
                    {query}
                    ''')  
        query_results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(query_results, columns = colnames)
        #numer_cols = df.select_dtypes(include = [int,float]).columns
        #df[numer_cols] = df[numer_cols].applymap(format_numer)       
    except Exception as e:
        cur.close()
        conn.close()
        return("Database connection failed due to {}".format(e))
    finally:
        cur.close()
        conn.close()
    return df
