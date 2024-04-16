import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import json
import duckdb
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
from datetime import datetime


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
            

def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df


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

 
def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def load_Orc_sql():
    orSql= {}
    
    orSql['wdlts'] = """
        SELECT
            FOREST_FILE_ID,
            MAP_BLOCK_ID,
            ML_TYPE_CODE,
            MAP_LABEL,
            ROUND(SDO_GEOM.SDO_AREA(
                GEOMETRY, 0.5, 'unit=HECTARE'), 2) AS WDLT_AREA_HA,
            LIFE_CYCLE_STATUS_CODE,
            CLIENT_NUMBER,
            CLIENT_NAME,
            ADMIN_DISTRICT_CODE,
            SDO_UTIL.TO_WKTGEOMETRY(GEOMETRY) AS GEOMETRY
            
        FROM 
            WHSE_FOREST_TENURE.FTEN_MANAGED_LICENCE_POLY_SVW   
            
        WHERE 
            FEATURE_CLASS_SKEY in ( 865, 866) 
            AND LIFE_CYCLE_STATUS_CODE <> 'RETIRED'
            AND SDO_WITHIN_DISTANCE (GEOMETRY, 
                                     SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=500 unit=m') = 'TRUE'
                    """

    orSql['ofd'] = """
        SELECT
            CURRENT_PRIORITY_DEFERRAL_ID,
            SDO_UTIL.TO_WKTGEOMETRY(SHAPE) AS GEOMETRY
            
        FROM
            WHSE_FOREST_VEGETATION.OGSR_PRIORITY_DEF_AREA_CUR_SP ofd
        
        WHERE 
            SDO_WITHIN_DISTANCE (SHAPE, 
                        SDO_GEOMETRY(:wkb_aoi, :srid), 'distance=5000 unit=m') = 'TRUE'
                    """
            
    return orSql


def load_dck_sql():
    dkSql= {}
    dkSql['wdlts']="""
        SELECT
            FOREST_FILE_ID,
            MAP_BLOCK_ID,
            ML_TYPE_CODE,
            MAP_LABEL,
            ROUND(ST_Area(geometry) / 10000.0, 2) AS WDLT_AREA_HA,
            LIFE_CYCLE_STATUS_CODE,
            CLIENT_NUMBER,
            CLIENT_NAME,
            ADMIN_DISTRICT_CODE
            
        FROM 
            wdlts  

                    """
                    
    dkSql['wdlts_ofd']="""
        SELECT
            wdl.MAP_LABEL,
            ROUND(ST_Area(wdl.geometry) / 10000.0, 2) AS WDLT_AREA_HA,
            SUM(ROUND(ST_Area(
                ST_Intersection(
                    ofd.geometry, wdl.geometry)) / 10000.0, 2)) AS OFD_AREA_HA
        
        FROM 
            wdlts wdl
            LEFT JOIN ofd
                ON ST_Intersects(ofd.geometry, wdl.geometry)
                
        GROUP BY 
            wdl.MAP_LABEL,
            ROUND(ST_Area(wdl.geometry) / 10000.0, 2)
                    """

    dkSql['wdlts_fhrw']="""
        SELECT
            wdl.MAP_LABEL,
            ROUND(ST_Area(wdl.geometry) / 10000.0, 2) AS WDLT_AREA_HA,
            SUM(ROUND(ST_Area(
                ST_Intersection(
                    fhrw.geometry, wdl.geometry)) / 10000.0, 2)) AS FHRW_AREA_HA
        
        FROM 
            wdlts wdl
            LEFT JOIN fisher_habitat_retention fhrw
                ON ST_Intersects(wdl.geometry, fhrw.geometry)
                
        GROUP BY 
            wdl.MAP_LABEL,
            ROUND(ST_Area(wdl.geometry) / 10000.0, 2)
                    """
                    
    return dkSql  


def oracle_2_duckdb(orcCnx, orcCur, dckCnx, dict_sqls):
    """Insert data from Oracle into a duckdb table"""
    tables = {}
    counter = 1
    
    for k, v in dict_sqls.items():
        print(f'..adding table {counter} of {len(dict_sqls)}: {k}')
        print('....export from Oracle')
        if ':wkb_aoi' in v:
            orcCur.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
            bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
            df = read_query(orcCnx, orcCur, v ,bvars)
        else:
            df = pd.read_sql(v, orcCnx)
            

        dck_tab_list= dckCnx.execute('SHOW TABLES').df()['name'].to_list()
        
        if k in dck_tab_list:
            dck_row_count= dckCnx.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0]
            dck_col_nams= dckCnx.execute(f"""SELECT column_name 
                                             FROM INFORMATION_SCHEMA.COLUMNS 
                                             WHERE table_name = '{k}'""").df()['column_name'].to_list()
                
            if (dck_row_count != len(df)) or (set(list(df.columns)) != set(dck_col_nams)):
                print (f'....import to Duckdb ({df.shape[0]} rows)')
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
            print (f'....import to Duckdb ({df.shape[0]} rows)')
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
                print (f'....import to Duckdb ({df.shape[0]} rows)')
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
            print (f'....import to Duckdb ({df.shape[0]} rows)')
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


def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    outfile= os.path.join(workspace, filename + '.xlsx')

    writer = pd.ExcelWriter(outfile,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        #workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 25)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    

if __name__ == "__main__":
    start_t = timeit.default_timer() #start time 
    
    wks= r'W:\srm\kam\Workarea\ksc_proj\Wildlife\Fisher\20240404_new_Fisher_draft_polygons'
    
    print ('Connect to databases')    
    # Connect to the Oracle database
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    orcCnx= Oracle.connection
    orcCur= Oracle.cursor
    
    # Connect to duckdb
    Duckdb= DuckDBConnector(db='wdlt.db')
    Duckdb.connect_to_db()
    dckCnx= Duckdb.conn
    
    print ('Create an AOI shape.')
    in_gdb= os.path.join(wks, 'inputs', 'data.gdb')
    gdf_aoi= esri_to_gdf(os.path.join(in_gdb, 'Draft_Fisher_WHA_ALL_AOI'))
    #gdf.to_file(os.path.join(wks, 'tests', 'union.shp'))
    wkb_aoi, srid= get_wkb_srid(gdf_aoi)
    
    try:

        print ('\nLoad BCGW datasets')
        orSql= load_Orc_sql ()
        orcTables= oracle_2_duckdb(orcCnx, orcCur, dckCnx, orSql)
        
        print ('\nLoad local datasets')
        gdb= os.path.join(wks,'test.gdb')
        loc_dict={}
        loc_dict['draft_fisher_polys']= os.path.join(in_gdb, 'Draft_Fisher_WHA_ALL')
        loc_dict['fisher_habitat_retention']= os.path.join(in_gdb, 'fisher_habitat_retention')
        gdbTables= gdf_to_duckdb (dckCnx, loc_dict)

        
        print('\nRun queries')
        dksql= load_dck_sql()
        rslts= run_duckdb_queries (dckCnx, dksql) 
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
        Duckdb.disconnect_db()
    
    
    print ('\nExport the report.')
    ouloc= os.path.join(wks, 'outputs')
    today = datetime.today().strftime('%Y%m%d')
    filename= today + '_Fisher_draftPolys_woodlotsAnalysis'
    generate_report (ouloc, rslts.values(), rslts.keys(), filename)
    
        
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print (f'\nProcessing Completed in {mins} minutes and {secs} seconds')  
    