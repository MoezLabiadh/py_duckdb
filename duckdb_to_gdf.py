import duckdb
import geopandas as gpd
from shapely import wkt

def duckdb_to_gdf(conn, table):
    """Returns a geodataframe based on a duckdb spatial table"""
    
    geocol= conn.execute(f"""
                        SELECT column_name 
                        FROM duckdb_columns
                        WHERE table_name = '{table}' 
                            AND data_type = 'GEOMETRY'
                                """).fetchone()[0]
    
    df = conn.execute(f"""SELECT * EXCLUDE {geocol}, 
                            ST_AsText({geocol}) AS wkt_geom 
                        FROM {table}
                                """).fetch_df()
                
    df['geometry'] = df['wkt_geom'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.drop(columns=['wkt_geom'], inplace=True)


    return gdf