import warnings
warnings.simplefilter(action='ignore')

import os
import json
import timeit
import duckdb
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb

class OracleConnector:
    def __init__(self, dbname='BCGW'):
        self.dbname = dbname
        self.cnxinfo = self.get_db_cnxinfo()

    def get_db_cnxinfo(self):
        """ Retrieves db connection params from the config file"""
        with open(r'H:\config\db_config.json', 'r') as file:
            data = json.load(file)
        
        if self.dbname in data:
            return data[self.dbname]
        
        raise KeyError(f"Database '{self.dbname}' not found.")
    
    def connect_to_db(self):
        """ Connects to Oracle DB and create a cursor"""
        try:
            self.connection = cx_Oracle.connect(self.cnxinfo['username'], 
                                                self.cnxinfo['password'], 
                                                self.cnxinfo['hostname'], 
                                                encoding="UTF-8")
            self.cursor = self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception(f'..Connection failed: {e}')

    def disconnect_db(self):
        """Close the Oracle connection and cursor"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("....Disconnected from the database")


class DuckDBConnector:
    def __init__(self, db=':memory:'):
        self.db = db
        self.conn = None
    
    def connect_to_db(self):
        """Connects to a DuckDB database and installs spatial extension."""
        self.conn = duckdb.connect(self.db)
        self.conn.install_extension('spatial')
        self.conn.load_extension('spatial')
        return self.conn
    
    def disconnect_db(self):
        """Disconnects from the DuckDB database."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def get_wshd_list(orcCnx):
    """Return the list of watershed intersecting the WHAs """
    sql="""
        SELECT DISTINCT
            wsh.WATERSHED_FEATURE_ID 
        FROM 
            WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY wsh
            JOIN WHSE_WILDLIFE_MANAGEMENT.WCP_WHA_PROPOSED_SP wha
                ON SDO_RELATE(wsh.GEOMETRY, wha.SHAPE, 'mask=ANYINTERACT')='TRUE' 
                    AND wha.WHA_TAG IN ('4-282', '4-287', '4-283', '4-312', '4-281', 
                                '4-288', '4-307', '4-286', '4-284', '4-285', '4-306') 
        """
    df= pd.read_sql(sql, orcCnx) 
    wshd_lst= ",".join(str(x) for x in df['WATERSHED_FEATURE_ID'].to_list())
      
    return wshd_lst 

    
def load_Orc_sql(wshd_lst):
    orSql= {}
    
    
    orSql['wha']="""
        SELECT
            WHA_TAG,
            FEATURE_NOTES,
            SDO_UTIL.TO_WKTGEOMETRY(SHAPE) AS GEOMETRY
        FROM
            WHSE_WILDLIFE_MANAGEMENT.WCP_WHA_PROPOSED_SP
        WHERE
            WHA_TAG IN ('4-282', '4-287', '4-283', '4-312', '4-281', 
                        '4-288', '4-307', '4-286', '4-284', '4-285', '4-306')
        """

    orSql['watersheds']=f"""
        SELECT
            wsh.WATERSHED_FEATURE_ID,
            SDO_UTIL.TO_WKTGEOMETRY(wsh.GEOMETRY) AS GEOMETRY
        FROM 
            WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY wsh
        WHERE        
            wsh.WATERSHED_FEATURE_ID IN ({wshd_lst})
            """
            
    orSql['harvested_ctb']=f"""
        SELECT
            ctb.VEG_CONSOLIDATED_CUT_BLOCK_ID,
            ctb.HARVEST_YEAR,
            SDO_UTIL.TO_WKTGEOMETRY(ctb.SHAPE) AS GEOMETRY
        FROM 
            WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP ctb
        WHERE 
            ctb.HARVEST_YEAR >= 2016 
            AND SDO_ANYINTERACT (ctb.SHAPE, 
                                 (SELECT SDO_AGGR_UNION(SDOAGGRTYPE(wsh.GEOMETRY, 1)) AS geom
                                  FROM WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY wsh 
                                  WHERE wsh.WATERSHED_FEATURE_ID IN ({wshd_lst})
                                      )
                                         ) = 'TRUE'
        """
        
    orSql['approved_ctb']=f"""
        SELECT
            frs.MAP_LABEL,
            SDO_UTIL.TO_WKTGEOMETRY(frs.GEOMETRY) AS GEOMETRY
        FROM 
            WHSE_FOREST_TENURE.FTEN_HARVEST_AUTH_POLY_SVW frs
        WHERE 
            frs.LIFE_CYCLE_STATUS_CODE = 'PENDING'
            AND SDO_ANYINTERACT (frs.GEOMETRY, 
                                 (SELECT SDO_AGGR_UNION(SDOAGGRTYPE(wsh.GEOMETRY, 1)) AS geom
                                  FROM WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY wsh 
                                  WHERE wsh.WATERSHED_FEATURE_ID IN ({wshd_lst})
                                      )
                                         ) = 'TRUE'
        """
        
    orSql['streams']=f"""
        SELECT
            str.LINEAR_FEATURE_ID,
            SDO_UTIL.TO_WKTGEOMETRY(SDO_CS.MAKE_2D(str.GEOMETRY)) AS GEOMETRY
        FROM
            WHSE_BASEMAPPING.FWA_STREAM_NETWORKS_SP str
        WHERE 
            SDO_ANYINTERACT (str.GEOMETRY, 
                                 (SELECT SDO_AGGR_UNION(SDOAGGRTYPE(wsh.GEOMETRY, 1)) AS geom
                                  FROM WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY wsh 
                                  WHERE wsh.WATERSHED_FEATURE_ID IN ({wshd_lst})
                                      )
                                         ) = 'TRUE' 
        """            

            
    return orSql
  


def load_dck_sql():
    dkSql= {}
    dkSql['ctb_harvested_wha']="""
            SELECT
                ctb.VEG_CONSOLIDATED_CUT_BLOCK_ID,
                ctb.HARVEST_YEAR,
                wha.WHA_TAG,
                wha.FEATURE_NOTES,
                ROUND(ST_Area(ctb.geometry) / 10000.0, 2) AS CTB_AREA_HA,
                ROUND(ST_Area(wha.geometry) / 10000.0, 2) AS WHA_AREA_HA,
                ROUND(ST_Area(
                    ST_Intersection(
                        ctb.geometry, wha.geometry)::geometry) / 10000.0, 2) AS INTRSCT_AREA_HA
            FROM 
                harvested_ctb ctb
                JOIN 
                    wha wha
                        ON ST_Intersects(ctb.geometry, wha.geometry)
                    """
        
    dkSql['ctb_harvested_wshd']="""
            SELECT
                ctb.VEG_CONSOLIDATED_CUT_BLOCK_ID,
                ctb.HARVEST_YEAR,
                wsh.WATERSHED_FEATURE_ID,
                ROUND(ST_Area(ctb.geometry) / 10000.0, 2) AS CTB_AREA_HA,
                ROUND(ST_Area(wsh.geometry) / 10000.0, 2) AS WSH_AREA_HA,
                ROUND(ST_Area(
                    ST_Intersection(
                        ctb.geometry, wsh.geometry)::geometry) / 10000.0, 2) AS INTRSCT_AREA_HA
            FROM 
                harvested_ctb ctb
            JOIN 
                watersheds wsh 
                    ON ST_Intersects(ctb.geometry, wsh.geometry)
                   """
 
    dkSql['ctb_approved_wha']="""
            SELECT
                frs.MAP_LABEL,
                wha.WHA_TAG,
                wha.FEATURE_NOTES,
                ROUND(ST_Area(frs.geometry) / 10000.0, 2) AS CTB_AREA_HA,
                ROUND(ST_Area(wha.geometry) / 10000.0, 2) AS WHA_AREA_HA,
                ROUND(ST_Area(
                    ST_Intersection(
                        wha.geometry, frs.geometry)::geometry) / 10000.0, 2) AS INTRSCT_AREA_HA
            FROM 
                approved_ctb frs
                JOIN 
                    wha wha
                        ON ST_Intersects(wha.geometry, frs.geometry)
                    """
    dkSql['ctb_approved_wshd']="""
            SELECT
                frs.MAP_LABEL,
                wsh.WATERSHED_FEATURE_ID,
                ROUND(ST_Area(frs.geometry) / 10000.0, 2) AS CTB_AREA_HA,
                ROUND(ST_Area(wsh.geometry) / 10000.0, 2) AS WSH_AREA_HA,
                ROUND(ST_Area(
                    ST_Intersection(
                        wsh.geometry, frs.geometry)::geometry) / 10000.0, 2) AS INTRSCT_AREA_HA
            FROM 
                approved_ctb frs
            JOIN 
                watersheds wsh 
                    ON ST_Intersects(wsh.geometry, frs.geometry)
                   """
                   
                       
    dkSql['road_density_wshd']="""
            SELECT
                wsh.WATERSHED_FEATURE_ID,
                rds.INTEGRATED_ROADS_ID,
                ROUND(ST_Area(wsh.geometry) / 1000000.0, 2) AS WSH_AREA_sqKM,
                ROUND(ST_Length(rds.geometry) / 1000.0, 2) AS RDS_LENGTH_km
            FROM 
                watersheds wsh 
            JOIN 
                roads rds 
            ON 
                ST_Intersects(rds.geometry, wsh.geometry)
                   """

    dkSql['road_density_wha']="""
            SELECT
                wha.WHA_TAG,
                wha.FEATURE_NOTES,
                rds.INTEGRATED_ROADS_ID,
                ROUND(ST_Area(wha.geometry) / 1000000.0, 2) AS WSH_AREA_sqKM,
                ROUND(ST_Length(rds.geometry) / 1000.0, 2) AS RDS_LENGTH_km
            FROM 
                wha wha
            JOIN 
                roads rds 
            ON 
                ST_Intersects(rds.geometry, wha.geometry)
                   """
                            
    return dkSql    


def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def gdf_to_duckdb (dckCnx, loc_dict):
    """Insert data from a gdfs into a duckdb table """
    tables = {}
    counter= 1
    for k, v in loc_dict.items():
        print (f'..adding table {counter} of {len(loc_dict)}: {k}')
        print ('....export from gdb')
        df= esri_to_gdf (v)
        df['GEOMETRY']= df['geometry'].apply(lambda x: wkb.dumps(x, output_dimension=2))
        df = df.drop(columns=['geometry'])
        
        dck_tab_list= dckCnx.execute('SHOW TABLES').df()['name'].to_list()
        
        if k in dck_tab_list:
            dck_row_count= dckCnx.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0]
            dck_col_nams= dckCnx.execute(f"""SELECT column_name 
                                             FROM INFORMATION_SCHEMA.COLUMNS 
                                             WHERE table_name = '{k}'""").df()['column_name'].to_list()
                
            if (dck_row_count != len(df)) or (set(list(df.columns)) != set(dck_col_nams)):
                print ('....import to Duckdb')
                create_table_query = f"""
                CREATE OR REPLACE TABLE {k} AS
                  SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS GEOMETRY
                  FROM df;
                """
                dckCnx.execute(create_table_query)
            else:
                print('....data already in db: skip importing')
                pass
        
        else:
            print ('....import to Duckdb')
            create_table_query = f"""
            CREATE OR REPLACE TABLE {k} AS
              SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS GEOMETRY
              FROM df;
            """
            dckCnx.execute(create_table_query)
            
        
        df = df.drop(columns=['GEOMETRY'])
        
        tables[k] = df
        
        counter+= 1
        
        return tables
        
        
def oracle_2_duckdb(orcCnx, dckCnx, dict_sqls):
    """Insert data from Oracle into a duckdb table"""
    tables = {}
    counter = 1
    
    for k, v in dict_sqls.items():
        print(f'..adding table {counter} of {len(dict_sqls)}: {k}')
        print('....export from Oracle')
        df = pd.read_sql(v, orcCnx)

        dck_tab_list= dckCnx.execute('SHOW TABLES').df()['name'].to_list()
        
        if k in dck_tab_list:
            dck_row_count= dckCnx.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0]
            dck_col_nams= dckCnx.execute(f"""SELECT column_name 
                                             FROM INFORMATION_SCHEMA.COLUMNS 
                                             WHERE table_name = '{k}'""").df()['column_name'].to_list()
                
            if (dck_row_count != len(df)) or (set(list(df.columns)) != set(dck_col_nams)):
                print ('....import to Duckdb')
                create_table_query = f"""
                CREATE OR REPLACE TABLE {k} AS
                  SELECT * EXCLUDE geometry, ST_GeomFromText(geometry) AS GEOMETRY
                  FROM df;
                """
                dckCnx.execute(create_table_query)
            else:
                print('....data already in db: skip importing')
                pass
        
        else:
            print ('....import to Duckdb')
            create_table_query = f"""
            CREATE OR REPLACE TABLE {k} AS
              SELECT * EXCLUDE geometry, ST_GeomFromText(geometry) AS GEOMETRY
              FROM df;
            """
            dckCnx.execute(create_table_query)
      
        df = df.drop(columns=['GEOMETRY'])
        
        tables[k] = df
      
        counter += 1

    return tables


def run_duckdb_queries (dckCnx, dict_sqls):
    """Run duckdb queries """
    results= {}
    counter = 1
    for k, v in dict_sqls.items():
        counter = 1
        print(f'..running query {counter} of {len(dict_sqls)}: {k}')
        results[k]= dckCnx.execute(v).df()
        
        counter+= 1
        
    return results


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time
    
    wks=r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\tempo\20240318'
    print ('Connect to databases')    
    # Connect to the Oracle database
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    orcCnx= Oracle.connection
    
    # Connect to duckdb
    Duckdb= DuckDBConnector(db='wha_proj.db')
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn
    
    print ('\nLoad local datasets')
    gdb= os.path.join(wks,'test.gdb')
    loc_dict={}
    loc_dict['roads']= os.path.join(gdb, 'integrated_roads_2021')
    gdbTables= gdf_to_duckdb (dckCnx, loc_dict)
    
    try:
        print ('\nLoad BCGW datasets') 
        print('..getting watersheds list')
        wshd_lst= get_wshd_list(orcCnx)
        
        orSql= load_Orc_sql (wshd_lst)
        orcTables= oracle_2_duckdb (orcCnx, dckCnx, orSql)
        
        print ('\nRun duckdb queries')
        dk_sql= load_dck_sql()
        results= run_duckdb_queries (dckCnx, dk_sql)
        
        #remove duplicates
        for df in results.values():
            df.drop_duplicates(inplace= True)
    
    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
        Duckdb.disconnect_db()
    

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))     