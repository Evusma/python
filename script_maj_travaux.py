##############################################################################
# Script of type ETL for uploading data stored in PostgreSQL/PostGIS         #
#       * it downloads the data from arcGIS Online                           #
#       * it uploads the data to PostGIS                                     #
#       * it transforms the projection                                       #
#       * it adds the year to the data                                       # 
# Using the to manage the shapefile.                                         #
# I could not make a good PostgreSQL connection using sqlalchemy             #
##############################################################################

import csv
import geopandas as gpd
import os
import psycopg2
import shutil
import arcgis.gis 

from arcgis.gis import GIS
from datetime import date
from zipfile import ZipFile

today = date.today()

# connection to the database
def connection_db():
    return psycopg2.connect(dbname="dbname", user="user", password="password", host="host", port="port")
    
# extract zip
def extract_zip(cwd):
    zip_name = os.listdir(cwd)[0]
    
    # opening the zip file in read mode
    with ZipFile(zip_name, 'r') as zip:
        # printing all the contents of the zip file
        zip.printdir()
        
        # extracting all the files
        print('Extracting all the files now...')
        zip.extractall()
        print('Done!')

# download the data from arcgis online
def travaux_arcgis():
    # connection to arcgis online
    directory = r'C:\xxx\xxx\xxx\travaux\travaux_survey123'
    gis = GIS(username="username", password="password")
    item_download = arcgis.gis.Item(gis, itemid='78d23333b02c4c08a3048c379dfe5a98')
    print(item_download)
    
    # download the data from arcgis online
    item_export = item_download.export(title=f" nsmtravaux{today}", export_format='Shapefile')
    item_export.download(save_path=directory)
    item_export.delete(force=True)
    
    # delete the data of arcgis online
    point = gis.content.get('78d23333b02c4c08a3048c379dfe5a98').layers[0].delete_features(where="globalid <> ''")
    print(point)
    
    return directory

def main():
    # download data from arcgis online
    directory = travaux_arcgis()
    
    os.chdir(directory)
    cwd = os.getcwd()
    print(cwd)
    
    # extract the shp
    extract_zip(cwd)
    
    # read the shp with geopandas and convert it to csv (the geometry field is converted to text)
    file = 'nsm_travaux_export.shp'
    gdf = gpd.read_file(file, encoding='utf-8') # geodataframe
    csv_file = gdf.to_csv('travaux_source.csv', encoding='utf-8')
    
    # from the csv, we copy only the columns we want
    with open("travaux_source.csv","r", encoding="utf-8") as source:
        rdr = csv.reader(source)
        with open("travaux.csv","w",encoding="utf-8", newline='') as result:
            wtr = csv.writer(result)
            for r in rdr:
                wtr.writerow(( r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19]))
 
    # connection to the database
    conn = connection_db()
    print("Database connected") 
    
    curs = conn.cursor() 
    
    # delete the previous data of the table
    curs.execute("DELETE FROM __historique.travaux")
    conn.commit()
    
    # insert the data into the table
    with open('travaux.csv', 'r', encoding="utf-8") as f:
        query = "COPY __historique.travaux (adresse, debut, fin, arrete, typologie, typologie_, nom_respon, nom_resp_1, descriptio, observatio, identite, courriel, geometry_txt) FROM STDIN WITH CSV HEADER DELIMITER ',' ENCODING 'UTF8'"
        curs.copy_expert(query, f)
        print('inserted in __historique.travaux: ', curs.rowcount)
    conn.commit()
    
    # include the geometry of type geometry (geom) by transforming the geometry of type text (geometry_txt)
    # transformation from the projection EPSG 3857 to the projection EPSG 2154
    # add the year
    curs.execute("UPDATE __historique.travaux SET geom = ST_Transform(ST_GeometryFromText(geometry_txt,3857), 2154)")
    curs.execute("UPDATE __historique.travaux SET annee = (SELECT to_char(now( ), 'yyyy'))")
    conn.commit()
    
    # insert the data to the final table
    curs.execute("INSERT INTO _travaux.nsm_travaux (geom, adresse, debut, fin, arrete, typologie, typologie_, nom_respon, nom_resp_1, descriptio, observatio, identite, courriel, annee) SELECT geom, adresse, debut, fin, arrete, typologie, typologie_, nom_respon, nom_resp_1, descriptio, observatio, identite, courriel, annee FROM __historique.travaux")
    print('inserted in _travaux.nsm_travaux: ', curs.rowcount)
    conn.commit() 
    
    curs.close()
    conn.close()

    print('Database connexion closed')  
    
    # change directory to delete the export
    directory = f"C:/xxx/xxx/xxx/xxx/travaux" 
    os.chdir(directory)
    
    # delete the directory travaux_survey123 and all its contents.
    shutil.rmtree(cwd)
    

if __name__ == "__main__":
    main()
    
    
