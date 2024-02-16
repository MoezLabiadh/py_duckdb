import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
import duckdb
from shapely import wkb


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
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    return gdf

sql = """
    SELECT
        CROWN_LANDS_FILE,
        TENURE_STATUS,
        ROUND(TENURE_AREA_IN_HECTARES, 2) AS AREA_HA,
        SDO_UTIL.TO_WKTGEOMETRY(SHAPE) AS SHAPE
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


df= pd.read_sql(sql, orcCnx)
df=df.loc[df['SHAPE'].notnull()]
gdf= df_2_gdf(df, 3005)


dckCnx = duckdb.connect(database=':memory:')
dckCnx.execute("INSTALL spatial;")
dckCnx.execute("LOAD spatial;")

dckCur= dckCnx.cursor()

tblName= 'crow_tenures'
colNames= [desc[0] for desc in orcCur.description]

dckCur.execute(f'''
               CREATE TABLE {tblName} (
                   {", ".join(f"{col} VARCHAR" for col in colNames)}
                       )
''')

for index, row in gdf.iterrows():
    print(index)
    wkb_geometry = wkb.dumps(row['geometry'], output_dimension=2)
    
    query = f"""
    INSERT INTO {tblName} 
         (CROWN_LANDS_FILE, GEOMETRY)
    VALUES (%s, ST_GeomFromWKB(%s))
            """
    # Use parameterized query to avoid SQL injection
    dckCur.execute(query, (row['CROWN_LANDS_FILE'], row['geometry']))
    
# Commit changes
dckCnx.commit()

res= dckCur.execute("SELECT* FROM crow_tenures ").df()
