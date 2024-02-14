import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
import duckdb
from shapely.wkb import loads

def connect_to_DB(username, password, hostname):
    """Returns a connection and cursor to the Oracle database."""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor= connection.cursor()
        print("Successfully connected to the database")
    except:
        raise Exception("Connection failed! Please check your login parameters")
    return connection, cursor

def df_2_gdf(df, crs):
    """Return a GeoPandas GeoDataFrame based on a DataFrame with a Geometry column."""
    df['geometry'] = gpd.GeoSeries.from_wkb(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    return gdf

sql = """
    SELECT
        CROWN_LANDS_FILE,
        TENURE_STATUS,
        ROUND(TENURE_AREA_IN_HECTARES, 2) AS AREA_HA,
        SDO_UTIL.TO_WKBGEOMETRY(SHAPE) AS GEOMETRY
    FROM 
        WHSE_TANTALIS.TA_CROWN_TENURES_SVW
    WHERE 
        TENURE_SUBPURPOSE = 'PRIVATE MOORAGE'
        AND TENURE_STAGE = 'APPLICATION' 
        AND RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
    """

# Connect to the Oracle database
hostname = 'bcgw.bcgov/idwprod1.bcgov'
username = os.getenv('bcgw_user')
password = os.getenv('bcgw_pwd')
orcCnx, orcCur = connect_to_DB(username, password, hostname)

orcCur.execute(sql)

dckCnx = duckdb.connect(database=':memory:')
dckCnx.execute("INSTALL spatial;")
dckCnx.execute("LOAD spatial;")

dckCur= dckCnx.cursor()

tblName= 'crow_tenures'
colNames= [desc[0] for desc in orcCur.description]

dckCur.execute(f'''
               CREATE TABLE {tblName} (
                   {", ".join(f"{col} VARCHAR" if col != "GEOMETRY" else f"{col} BLOB" for col in colNames)}
                       )
''')

for row in orcCur:
    wkb_geometry= loads(row[-1])
    dckCur.execute(f'''
        INSERT INTO {tblName} ({", ".join(colNames)})
        VALUES ({", ".join("ST_GeomFromWKB(?)" if col == "GEOMETRY" else "?" for col in colNames)})
    ''', row[:-1] + (wkb_geometry,))

# Commit changes
dckCnx.commit()
