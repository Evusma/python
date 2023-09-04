#############################################################################
# This code is used to take the data from a PostgreSQL/PostGIS database and #
# upload it to arcgis online, using the API arcgis                          #
#############################################################################


from connection import arcgis_online  # function to connect to arcgis online
from connection import connection_sig # function to connect to PostgreSQL/PostGIS
import geopandas as gpd
import pandas as pd


def main():
    conn = connection_sig()
    print("Database connected") 
    # select the data from PostdreSQL/PostGIS
    query_batiments = "SELECT id_batiment as nom, code_commune as commune, geom  FROM _cadastre_plu_domaine.cadastre_nsm_batiments"
    query_parcelles = "SELECT id_parcelle as id, round(m2_parcelle) as contenance, parcelle, geom FROM _cadastre_plu_domaine.cadastre_nsm_parcelles"
    gdf_batiments = gpd.read_postgis(query_batiments, conn)
    gdf_parcelles = gpd.read_postgis(query_parcelles, conn)
    
    # from geodataframe to spatial dataframe used by the API arcgis    
    gis = arcgis_online()
    print("Arcgis online connected") 
    sdf_batiments = pd.DataFrame.spatial.from_geodataframe(gdf_batiments) 
    sdf_parcelles = pd.DataFrame.spatial.from_geodataframe(gdf_parcelles) 
    
    # update feature "batiment" in arcgis online (first delete features, secondly add features)
    gis.content.get('728bb8d386d7410782b4fb85fa891f8b').layers[0].delete_features(where = "FID <> ''")
    lyr_batiment = gis.content.get('728bb8d386d7410782b4fb85fa891f8b').layers[0]
    features_batiment = sdf_batiments.spatial.to_featureset()
    lyr_batiment.edit_features(adds=features_batiment)
    
    # update feature "parcelles" in arcgis online (first delete features, secondly add features)
    gis.content.get('a27b860b35994385a957d86911c7009b').layers[0].delete_features(where = "FID <> ''")
    lyr_parcelle = gis.content.get('a27b860b35994385a957d86911c7009b').layers[0]
    features_parcelle = sdf_parcelles.spatial.to_featureset()
    lyr_parcelle.edit_features(adds=features_parcelle)


if __name__ == "__main__":
    main()
