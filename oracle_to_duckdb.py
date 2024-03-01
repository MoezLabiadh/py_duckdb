import os
import duckdb
import cx_Oracle
import pandas as pd

def connect_to_Oracle(username, password, hostname):
    """Returns a connection and cursor to the Oracle database."""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print("Successfully connected to the database")
    except:
        raise Exception("Connection failed! Please check your login parameters")
    return connection


def connect_to_duckdb (db= ':memory:'):
    """connects to a duckdb database and install spatial extension """
    conn = duckdb.connect()
    conn.install_extension('spatial')
    conn.load_extension('spatial')
    
    return conn


def gdf_to_duckdb (conn, gdf, table_name):
    """Insert data from a gdf into a duckdb table """
    
    create_table_query = f"""
    CREATE OR REPLACE TABLE {table_name} AS
      SELECT * EXCLUDE GEOMETRY, ST_GeomFromText(GEOMETRY) AS geometry
      FROM gdf;
    """
    conn.execute(create_table_query)



# Example usage:
if __name__ == "__main__":
        
    # Connect to the Oracle database
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    username = os.getenv('bcgw_user')
    password = os.getenv('bcgw_pwd')
    orcCnx= connect_to_Oracle(username, password, hostname)
    
    # Oracle query
    sql = """
        SELECT
            CROWN_LANDS_FILE,
            TENURE_STATUS,
            ROUND(TENURE_AREA_IN_HECTARES, 2) AS AREA_HA,
            SDO_UTIL.TO_WKTGEOMETRY(SHAPE) AS GEOMETRY
        FROM 
            WHSE_TANTALIS.TA_CROWN_TENURES_SVW
        WHERE 
            TENURE_SUBPURPOSE = 'PRIVATE MOORAGE'
            AND TENURE_STAGE = 'APPLICATION' 
            AND RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
        """
        
    
    # Run the Oracle query
    df= pd.read_sql(sql, orcCnx)
    
    # Connect to duckdb
    dckCnx= connect_to_duckdb()
    
    # Add table to duckdb
    tblName= 'crow_tenures'
    gdf_to_duckdb (dckCnx, df, tblName)

    #check table in duckdb
    df= dckCnx.execute(f"SELECT* FROM {tblName}").df()
    
    orcCnx.close()
    dckCnx.close()

