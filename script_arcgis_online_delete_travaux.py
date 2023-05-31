##
# Script for automotise data in ArcGIS Online
# It runs in the server, with Task Scheduler (Windows)
# It gets the feature layer and query the data
# It runs every day

from datetime import date
from arcgis.gis import GIS

today = date.today()

def main():
    # arcgis online connection
    gis = GIS(username = "username", password = "password")

    # accesing the data
    travaux = gis.content.get('78d23333b02c4c08a3048c379dfe5a98') # accesing feature layer using item id
    travaux_layer = travaux.layers[0] # accesing layer

    # select features to delete and to write the logs    
    feat_set = travaux_layer.query(where = " fin < CURRENT_TIMESTAMP ") # in this case, we look for the roadwork which are finished
    feat_dict = feat_set.to_dict()

    # write the log with the information of roadworks
    with open( r'C:\xxx\xxx\xxx\info_travaux.txt', 'a') as g:
        if feat_dict['features'] != []:
            g.write('deleted ')
            g.write(today.strftime("%d %B %Y"))
            g.write('\n')
            g.write(' features: \n')
            g.writelines(''.join(str((feat_dict['features']))))
        else:
            g.write('no deleted features ')
            g.write(today.strftime("%d %B %Y"))
            g.write('\n')

    # security copy of features before deleting them (they are donwloaded once a month with other script, and uploaded to the PostgreSQL database)
    security_copy = gis.content.get('a2b8e770622e4ab686520d096a2157dd') # accesing feature layer using item id
    security_copy_layer = security_copy.layers[0] # accesing layer
    security_copy_layer.edit_features(adds = feat_set)

    # delete features where date < today
    travaux_maj = travaux_layer.delete_features(where = " fin < CURRENT_TIMESTAMP ")

    # write the log with the list of deleted features
    list_deleted = travaux_maj['deleteResults']
    with open( r'C:\xxx\xxx\xxx\deleted_travaux.txt', 'a') as f:
        for i in range(len(list_deleted)):
            f.write(' objectID: ')
            f.writelines(''.join(str(list_deleted[i]['uniqueId'])))
            f.write(', success: ')
            f.writelines(''.join(str(list_deleted[i]['success'])))
            f.write('\n')


if __name__ == "__main__":
    main()
    

