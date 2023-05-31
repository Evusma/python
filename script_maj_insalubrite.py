##############################################################################
# Script of tyep ETL for updating data stored in PostgreSQL/PostGIS          #
#       * it gets the data (csv format)                                      #
#       * it uploads the data to PostGIS                                     #
#       * it checks missing data in the view due to a data not joined        #
##############################################################################

import os
import psycopg2


# connection to the dtabase sig
def connection_db():
    return psycopg2.connect(dbname="dbname", user="user", password="password", host="host", port="port")

def main(): 
    # working directory with the document
    os.chdir('C:/xxx/xxx/xxx/xxx/nsm_insalubrite')
    cwd = os.getcwd()
    print(cwd)
    
    # to ensure the encoding UTF-8
    with open('nsm_insalubrite.csv', mode='r', encoding='utf-8-sig') as f:
        text = f.read()
        open('nsm_insalubrite.csv', mode='w', encoding='utf-8').write(text)
        
    # connexion to the database
    conn = connection_db()
    print("Database connected") 
    
    curs = conn.cursor() 
    
    # delete the previous data of the table
    curs.execute("DELETE FROM __donnees.securite_nsm_insalubrite")
    print('deleted from __donnees.securite_nsm_insalubrite: ', curs.rowcount)
    conn.commit() 
    
    # insert the new data into the table
    with open('nsm_insalubrite.csv', 'r', encoding="utf-8") as f:  
        query = "COPY __donnees.securite_nsm_insalubrite (dossier, etat, type_dossier, problematique, n_rue, nom_voie, parcelle) FROM STDIN WITH CSV HEADER DELIMITER ';' ENCODING 'UTF8'"
        curs.copy_expert(query, f)
        inserted = curs.rowcount
        print('inserted in __donnees.securite_nsm_insalubrite: ', inserted)
        conn.commit()
        curs.execute("UPDATE __donnees.securite_nsm_insalubrite SET maj = current_date")
    conn.commit()

    # check if all the data has geom
    curs.execute("SELECT * FROM _securite.nsm_insalubrite")
    view_data = curs.rowcount
    print('avec geom _securite.nsm_insalubrite: ', view_data)
    conn.commit()
    
    if inserted == view_data:
        print('update finished')
    
    else:
        # check the missing data
        print('check missing geom data:')
        curs.execute("SELECT c.dossier, c.parcelle FROM __donnees.securite_nsm_insalubrite as c WHERE c.dossier NOT IN (SELECT b.dossier FROM _securite.nsm_insalubrite as b) ")
        print('missing data: ', curs.rowcount)
        for row in curs.fetchall():
            try:
                print("Dossier " + row[0] + " parcelle " + row[1])
            except TypeError:
                print("Dossier " + row[0] + " sans parcelle")
            finally:
                print("Check finished")
        conn.commit() 
    
    curs.close()
    conn.close()

    print('Database connexion closed')
    return 


if __name__ == "__main__":
    main()
