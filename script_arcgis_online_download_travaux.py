#####################################################################
# Script for downloading data from ArcGIS Online                    #
#####################################################################


from arcgis.gis import GIS
from datetime import date

today = date.today()

def main():
    # connection to arcgis online
    gis = GIS(username="username", password="password")
    
    # accesing the data
    item_download = arcgis.gis.Item(gis, itemid='78d23333b02c4c08a3048c379dfe5a98')

    # export a shapefile and download the data from arcgis online
    # allowed formats: 'Shapefile','CSV', 'File Geodatabase', 'Feature Collection', 'GeoJson', 'Scene Package', 'KML', 'Excel', 'geoPackage', or 'Vector Tile Package'
    item_export = item_download.export(title=f" download{today}", export_format='Shapefile') 
    item_export.download(save_path=r'C:\xxx\xxx\xxx\xxx')
    
    # delete the shapefile from arcgis online
    item_export.delete(force=True)
    
    # delete the data of arcgis online
    gis.content.get('78d23333b02c4c08a3048c379dfe5a98').layers[0].delete_features(where="globalid <> ''")
    

if __name__ == "__main__":
    main()
    
    
