##############################################################################
# Script of type ETL for updating data stored in PostgreSQL/PostGIS          #
#       * it gets the data (csv format)                                      #
#       * it uploads the data to PostGIS                                     #
#       * it checks missing data in the view due to a data not joined        #
##############################################################################

import csv
import os
import psycopg2
import sys


# connection to the database
def connection_db():
    return psycopg2.connect(dbname="dbname", user="user", password="password", host="host", port="port")

# check numeric values to avoid error from PostgreSQL
def check_numeric():
    checked = True
    with open('nsm_dia.csv', 'r', encoding="utf-8") as f: 
        rdr = csv.reader(f, delimiter=';')
        headers = next(rdr)
        for r in rdr:
            if r[0] == '' or not r[0].isdigit(): checked = False; print("error in line", rdr.line_num - 1, "DIA", r[1],"column", headers[0], r[0])
            if not r[12] == '' and not r[12].isdigit(): checked = False; print("error in line", rdr.line_num - 1, "DIA", r[1],"column ", headers[12], r[12])
            if not r[13] == '' and not r[13].isdigit(): checked = False; print("error in line", rdr.line_num - 1, "DIA", r[1],"column ", headers[13], r[13])
            if not r[14] == '' and not r[14].isdigit():checked = False; print("error in line", rdr.line_num - 1, "DIA", r[1],"column ", headers[14], r[14])
        if checked == False: sys.exit("text in numeric field")
        print("numeric values checked: ok")
 
def main():
    # working directory with the document
    os.chdir('C:/xxx/xxx/xxx/xxx/dia_excel')
    cwd = os.getcwd()
    print(cwd)      

    # to ensure the encoding UTF-8 
    with open('nsm_dia.csv', mode='r', encoding='utf-8-sig') as f:
        text = f.read()
        open('nsm_dia.csv', mode='w', encoding='utf-8').write(text)
        
    check_numeric()
    
    # connexion to the database
    conn = connection_db()
    print("Database connected") 
    
    curs = conn.cursor() 
    
    # delete the previous data of the table
    curs.execute("DELETE FROM __historique.nsm_dia")
    print('deleted from __historique.nsm_dia: ', curs.rowcount)
    conn.commit()
    
    # insert the new data into the table (the view is updated)
    with open('nsm_dia.csv', 'r', encoding="utf-8") as f:  
        query = "COPY __historique.nsm_dia FROM STDIN WITH CSV HEADER DELIMITER ';' ENCODING 'UTF8'"
        curs.copy_expert(query, f)
        inserted = curs.rowcount
        print('inserted in __historique.nsm_dia: ', inserted)
    conn.commit()
    
    # check the data of the view
    curs.execute("SELECT * FROM __historique.nsm_dia_view")
    view_data = curs.rowcount
    conn.commit()
    
    # insert the data of the view into the final table
    curs.execute("INSERT INTO _foncier.nsm_dia_2023 SELECT * FROM __historique.nsm_dia_view")
    print('inserted in nsm_2023: ', curs.rowcount)
    conn.commit()
    
    # check if all the data has geometry
    if inserted == view_data:
        print('update finished')
    else:
        # check the missing data: the view has a jointure to add the geometry. 
        # Here, we search the values which are not joined - reasons: multiple values in the field, missing values or new values not registered yet in the inventory
        print('check missing geom data:')
        curs.execute("SELECT c.n_dia, c.parcelle FROM __historique.nsm_dia as c WHERE c.n_dia NOT IN (SELECT b.n_dia FROM __historique.nsm_dia_view as b) ")
        print('missing data: ', curs.rowcount)
        for row in curs.fetchall():
            try:
                print("DIA " + row[0] + " parcelle " + row[1])
            except TypeError:
                print("DIA " + row[0] + " sans parcelle")
            finally:
                print("checked finished")
        conn.commit()
    
    curs.close()
    conn.close()
    
    print('Database connexion closed')


if __name__ == "__main__":
    main()
