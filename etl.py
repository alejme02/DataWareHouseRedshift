import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    
   '''
   This proc
     Staging tables:
         - song staging table
         - event staging table
         
     '''
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def insert_tables(cur, conn):
    
    '''
    
    Insert tables process, this will insert into all tables the necessary data from stagi

     Dimension tables:
         - artists table
         - time table
         - user table
         - song table 
         
     Fact table:
         - songplays table
       
     '''
        
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def connection():
    
    '''
    
    This process create the connection necessary to start the ETL
    
        DWH_DB          = database name
        DWH_PORT        = port
        DWH_DB_USER     = database name
        DWH_DB_PASSWORD = database password
        DWH_ENDPOINT    = endpoint of our redshift cluster 
        
     '''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_DB          = config.get("DWH","DWH_DB")
    DWH_PORT        = config.get("DWH","DWH_PORT")
    DWH_DB_USER     = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_ENDPOINT    = config.get("DWH","DWH_ENDPOINT")
    
    conn=psycopg2.connect("dbname={} host={} port={} user={} password={}".format(DWH_DB,DWH_ENDPOINT,DWH_PORT,DWH_DB_USER,DWH_DB_PASSWORD))
    return conn
    

def main():
    
    if __debug__:
        print("load staging tables doc: ")
        print(load_staging_tables.__doc__)

        print("insert tables doc: ")
        print(insert_tables.__doc__)

        print("connection doc: ")
        print(connection.__doc__)

    conn = connection()
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("finish")

    conn.close()
    
    
if __name__ == "__main__":
    main()