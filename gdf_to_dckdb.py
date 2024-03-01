import geopandas as gpd
import duckdb
from shapely import wkb


def connect_to_duckdb (db= ':memory:'):
    """connects to a duckdb database and install spatial extension """
    conn = duckdb.connect()
    conn.install_extension('spatial')
    conn.load_extension('spatial')
    
    return conn


def gdf_to_duckdb (conn, gdf, table_name):
    """Insert data from a gdf into a duckdb table """
    
    gdf['geometry_wkt'] = gdf['geometry'].apply(lambda x: wkb.dumps(x, output_dimension=2))
    gdf.drop(columns=['geometry'], inplace=True)
    
    create_table_query = f"""
    CREATE OR REPLACE TABLE {table_name} AS
      SELECT * EXCLUDE geometry_wkt, ST_GeomFromWKB(geometry_wkt) AS geometry
      FROM gdf;
    """

    conn.execute(create_table_query)
    
    
# Example usage:
if __name__ == "__main__":
    # some spatial file
    shp = r'w:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230815_maanulth_reporting_2023\sep26\maan_report_2023_shapes.shp'
    
    #create a duckdb connexion
    conn= connect_to_duckdb ()
    
    gdf = gpd.read_file(shp)
    
    # Specify table name
    table_name = 'maanulth'
    
    # Add GeoDataFrame to DuckDB
    gdf_to_duckdb (conn, gdf, table_name)
    
    #check table in duckdb
    df= conn.execute("SELECT* FROM maanulth").df()
    
    conn.close()
    

