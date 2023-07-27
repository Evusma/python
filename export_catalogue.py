##########################################################################################################################
# Script for download the lists of data and metadata stored in PostgeSQL, and the list of data store in arcgis online    #
# I use psycopg2 to query postgresql and arcgis API for python to query arcgis online                                    #
##########################################################################################################################

import os
import pandas as pd
from datetime import date
from connection import connection_sig # my script with the connection credentials (PostgreSQL)
from connection import arcgis_online # my script with the connection credentials (qrcgis online)
import datetime

today = date.today()

def catalogue():
    # this function creates a list of data of PostgreSQL
    # path where the lists (csv) will be stored
    os.chdir('C:/xxx/xxx/catalogue_donnees')
    conn = connection_sig()
    print("Database connected") 
    
    curs = conn.cursor() 
    
    # query for the list of data. We only take the schema where the data is stored
    query = """SELECT table_catalog, table_schema, table_name, table_type 
               FROM information_schema.tables
               WHERE
               table_schema != 'pg_catalog'
	           AND
	           table_schema != '_formation'
	           AND
	           table_schema != 'topology'
	           AND
	           table_schema != 'information_schema'
	           AND
	           table_schema != 'public'
	           AND
	           table_schema != '__projets'
	           AND
	           table_schema != 'pgmetadata'
	           ORDER BY table_schema, table_name ASC"""
    # export of the list of data
    with open(f'catalogue{today}.csv', 'w', encoding="utf-8") as f:  
        query2 = f"COPY ({query}) TO STDOUT WITH CSV HEADER DELIMITER ',' ENCODING 'UTF8'"
        curs.copy_expert(query2, f)
        exported = curs.rowcount
        print('number of tables exported (data): ', exported)
    conn.commit()
    curs.close()
    conn.close()

def catalogue_ao():
    # this function creates a list of data of arcgis online
    os.chdir('C:/xxx/xxx/catalogue_donnees')
    gis = arcgis_online()
    print("connected to arcgis online " + str(gis))
    
    qe = f"owner: {gis.users.me.username}"
    my_content = gis.content.advanced_search(query=qe, as_dict=True, sort_field="id", sort_order="asc")
    print("items to export: " + str(len(my_content['results'])))
    
    # selection of columns
    view_columns = ['id', 'created', 'modified', 'name', 'title','type','access', 'description', 'snippet', 'spatialReference', 'url', 'protected', 'scoreCompleteness', 'numComments', 'numViews', 'lastViewed']
    content = pd.DataFrame(my_content['results'])
    df = content[view_columns]
    df['created'] = df['created'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))
    df['modified'] = df['modified'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))
    df.to_csv(f'catalogue_ao{today}.csv', sep=',',encoding='utf-8', index=False)
    print('catalog of arcgis online exported')

def metadata():
    # this function creates a list of the metadata of PostgreSQL and of the tables without metadata 
    # (using the plugin of QGIS pgmetadata for the metadata administration)
    # path where the lists (csv) will be stored
    os.chdir('C:/xxx/xxx/catalogue_donnees')
    conn = connection_sig()
    print("Database connected") 
    
    curs = conn.cursor() 
    # query the list of metadata
    query1 =""" SELECT schema_name, table_name FROM pgmetadata.dataset ORDER BY schema_name, table_name"""
    with open(f'metadata{today}.csv', 'w', encoding="utf-8") as f:  
        query = f"COPY ({query1}) TO STDOUT WITH CSV HEADER DELIMITER ',' ENCODING 'UTF8'"
        curs.copy_expert(query, f)
        exported = curs.rowcount
        print('number of tables exported (metadata): ', exported)
    conn.commit()
    # query the list of data without metadata
    query2 =""" SELECT schemaname, tablename FROM pgmetadata.v_orphan_tables ORDER BY schemaname, tablename"""
    with open(f'no_metadata{today}.csv', 'w', encoding="utf-8") as f:  
        query = f"COPY ({query2}) TO STDOUT WITH CSV HEADER DELIMITER ',' ENCODING 'UTF8'"
        curs.copy_expert(query, f)
        exported = curs.rowcount
        print('number of tables exported (data without metadata): ', exported)
    conn.commit()
    curs.close()
    conn.close()
    
def main():
    catalogue()
    metadata()
    catalogue_ao()


if __name__ == "__main__":
    main()