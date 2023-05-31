#####################################################################
# Script for automating data in ArcGIS Online                       #
# It runs in the server, thanks to Task Scheduler (Windows)         #
# It gets the feature layer and query the data                      #
# It runs every day to keep the map updated:                        #
#       * it checks the building sites which are finished           #
#       * it deletes them from the map                              #
#####################################################################

from datetime import date
from arcgis.gis import GIS

today = date.today()

def main():
    # arcgis online connection
    gis = GIS(username = "username", password = "password")
    print("Connected to the GIS as {}.".format(gis.properties.user.username))

    # accesing the data (the data comes from a layer where the officer adds the information)
    pc_layer = gis.content.get('3516641b5222444be13b0e67a00ef00b').layers[0]

    # select features to delete (where dossier_pc not null)
    pc_dict = pc_layer.query(where = " dossier_pc <> '0' " ).to_dict()['features'] # list of dictionarys

    # list of sites to delete
    a = []
    for i in range(len(pc_dict)):
        a.append(pc_dict[i]['attributes']['dossier_pc'])
    
    # log of the sites to delete (as reported by the officer)
    with open( r'C:\xxx\xxx\xxx\info_pc.txt', 'a') as g:
        g.write(today.strftime("%d %B %Y"))
        g.write(', les dossiers à effacer : ')
        g.write(str(a))
        g.write('\n')

    print('les dossiers à efacer : ')
    print(a)

    # accesing the data of the map to update
    pc_projet_layer = gis.content.get('ab30d2244a389925564f09fa9558c725').layers[0]

    for i in range(len(a)):
        # where clause of layer with information
        whereclause1 = f" dossier_pc = '{a[i]}'"
        # where clause of map layer
        whereclause2 = f" DOSSIER = '{a[i]}'"
        
        pc_log = pc_projet_layer.query(where = whereclause2 ).to_dict()['features']
        
        # list of deleted sites
        b = []
        for i in range(len(pc_log)) :
            b.append(pc_log[i]['attributes']['DOSSIER'])
        
        # delete features of map
        pc_maj = pc_projet_layer.delete_features(where = whereclause2)
        
        # log of deleted features (features from the map)
        with open( r'C:\xxx\xxx\xxx\info_pc_delete.txt', 'a') as g: 
            g.write(today.strftime("%d %B %Y"))
            g.write(', dossiers effacés : ')
            g.write(str(b))
            g.write('\n\n')
        # delete features of the layer with information
        pc_layer_maj = pc_layer.delete_features(where = whereclause1)


if __name__ == "__main__":
    main()
    

