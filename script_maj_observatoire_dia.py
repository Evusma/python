###############################################################################
# Script for updating data stored in PostgreSQL/PostGIS                       #
#       * it calls a stored function which does the updates                   #
#       * it checks the information that may skip the automatic updates       #
###############################################################################

import psycopg2

current_year = '2023'

# connection to the database
def connection_db():
    return psycopg2.connect(dbname="dbname", user="user", password="password", host="host", port="port")
 
def main():
    # connection to the database
    conn = connection_db()
	  print("Database connected")
    
    curs = conn.cursor() 
    
    # execute the update and rename the table (current year)
    try:
        curs.execute("SELECT __donnees.alter_table_observatoire__dia()")
	conn.commit()
    except:
        print('postgresql error, not updated')
    else:
        # check update 1 (missing boroughs)  
        sql1 = f"SELECT n_dia, date FROM _foncier.nsm_2016_{current_year}_dia WHERE quartier IS NULL"
        curs.execute(sql1)
        print('missing boroughs: ', curs.rowcount)
        for data in curs.fetchall():
            print("n_dia " + data[0] + " date " + data[1])
        conn.commit()
        
        # check update 2 (missing departements) 
        sql2 = f"SELECT n_dia, cp, date FROM _foncier.nsm_2016_{current_year}_dia WHERE depart_acq IS NULL"
        curs.execute(sql2)
        print('missing departements: ', curs.rowcount)
        for data in curs.fetchall():
            try:
                print("n_dia " + data[0] + " cp " + data[1] + " date " + data[2])
            except TypeError:
                print("n_dia " + data[0] + " cp null " + " date " + data[2])
        conn.commit()
    finally:
        conn.commit()
    
    curs.close()
    conn.close()
    
    print('Database connexion closed')


if __name__ == "__main__":
    main()
