import configparser
import psycopg2
from sql_queries import create_table, drop_table_queries
from etl import connection
import pandas as pd

def drop_tables(cur, conn):
    
     '''
     This process delete all tables:
         - artists table
         - time table
         - user table
         - song table 
         - songplays table
    
     '''
        
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    
     '''
     This process create the next following tables:
     
     Dimension tables:
         - artists table
         - time table
         - user table
         - song table 
         
     Fact table:
         - songplays table
     
     '''
        
    for query in create_table:
        print(query)
        cur.execute(query)
        conn.commit()    

def main():

    if __debug__:
        print("drop tables doc: ")
        print(drop_tables.__doc__)

        print("create tables doc: ")
        print(create_tables.__doc__)
   
    conn = connection()
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()