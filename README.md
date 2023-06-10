# Python
## Some python code I developed during my job as GIS officer

Here, there is two types of python code:
```sh
- script_arcgis_online: I use these codes to connect to our ArcGis online platform and manage data.
- script_maj: I use these codes as an "ETL" tool. 
```
**_script_arcgis_online_**

Once connected to ArcGis Online, the main data manipulations are: copy, delete, download and write a log document. The main objective here is to keep the maps updated every day.

For example, **_script_arcgis_online_delete_travaux.py_** checks end dates. If some roadworks are finished, it makes a copy and deletes data from the map.

**_script_maj_**

The main manipulations are: take csv documents, connect to our PostgreSQL database and upload the data. Depending on the updated table and its relationships, there can be other manipulations as inputs, addition of geometry or conversion of projection system.

**_scrip_maj_travaux_shp_to_postgis.py_** is an example of code that extract data from Arcgis Online and uploads it to PostgreSQL, using geopandas and some postgis functions to manage geometry column.
