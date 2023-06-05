##############################################################################
# Script of type ETL for updating data stored in PostgreSQL/PostGIS          #
#       * it gets the data (csv format)                                      #
#       * it uploads the data to PostGIS                                     #
#       * it checks missing data in the view due to a data not joined        #
#       * it adds the borough to the data                                    #
##############################################################################

import os
import psycopg2


# connection to the database
def connection_db():
    return psycopg2.connect(dbname="dbname", user="user", password="password", host="host", port="port")
 
def main():
    # working directory with the document
    os.chdir('C:/xxx/xxx/xxx/xxx/nsm_observatoire') 
    cwd = os.getcwd()
    print(cwd)
      
    # to ensure the encoding UTF-8
    with open('pc.csv', mode='r', encoding='utf-8-sig') as f:
        text = f.read()
        open('pc.csv', mode='w', encoding='utf-8').write(text)
    
    # connection to the database
    conn = connection_db()
    print("Database connected")
    
    curs = conn.cursor() 
    
    # insert the new data into the table
    with open('pc.csv', 'r', encoding="utf-8") as f:  
        query = "COPY _pc_ads.nsm_2016_2022_pc (dossier, demandeur, parcelles, terrain_adresse, description, logements, annee, m2_plancher, chambres, f_1, f_2, f_3, f_4, f_5, f_6, type) FROM STDIN WITH CSV HEADER DELIMITER ';' ENCODING 'UTF8'"
        curs.copy_expert(query, f)
        print('inserted: ', curs.rowcount) # check number of rows
    conn.commit()
    
    # add the geometry
    curs.execute("UPDATE _pc_ads.nsm_2016_2022_pc SET geom = (SELECT b.geom FROM _cadastre_plu_domaine.cadastre_nsm_parcelles as b WHERE b.parcelle = parcelles) WHERE geom IS NULL")
    conn.commit()
    
    # check the geometry and data with multiple values in the field to join
    curs.execute("SELECT dossier, parcelles FROM _pc_ads.nsm_2016_2022_pc WHERE geom IS NULL")
    print('with no geom: ', curs.rowcount)
    curs.fetchall()
    conn.commit()
    
    # add the bourogh to the data
    curs.execute("UPDATE _pc_ads.nsm_2016_2022_pc as f SET quartier = b.nom FROM __historique.nsm_anciens_quartiers as b WHERE ST_Contains(b.geom, f.geom)")
    conn.commit()
    
    # rename the table
    curs.execute("SELECT __donnees.alter_table_observatoire_pc()")
    conn.commit()
    
    curs.close()
    conn.close()
    
    print('Database connexion closed')


if __name__ == "__main__":
    main()
