#from http import client
import pandas
#from psycopg2 import connect
import model
import connection


from sqlalchemy import create_engine
from hdfs import InsecureClient
from datetime import datetime

if __name__ == "__main__":
    ## postgres
    engine = create_engine('postgresql://postgres:221299@localhost:5432/digitalskola')

    ## setting connection to postgresql
    conf_postgresql = connection.param_config("postgresql")
    conf_hadoop = connection.param_config("hadoop")["ip"]

    ## make connection to postgresql
    conn = connection.postgres_conn(conf_postgresql)
    cur = conn.cursor()

    ## make connection to hadoop
    client = InsecureClient(conf_hadoop)
    
    ## section pengambilan data 
    list_tables = model.list_tables()
    for table in list_tables:
        sql = table[1] ## mengambil data dari function yg sudh dibuat di model.py. index 1 = mengambil functionnya atau datanya dengan query yg sudh dibuat
        
        cur.execute(sql) ## execute variabel sql untuk mengambil data dengan query didalam function di file model.py
        data = cur.fetchall() ## menggunakan fetchall untuk mengambil valuenya saja dari table data/ nama columnya tidak ada
       
        ## formating datetime di hadoop nanti
        time = datetime.now().strftime("%Y%m%d")

        ## memasukan data kedalam dataframe 
        df = pandas.DataFrame(data, columns=[col[0] for col in cur.description]) ## memasukan data ke dalam data frame, looping digunaakan untuk memunculkan nama colum pada di data frame  dengan cur.description
        
        ## ingestion to hadoop(insert data to hadoop)
        with client.write(f'/DigitalSkola/{time}/{table[0]}_{time}.csv', encoding='utf-8') as writer:
            df.to_csv(writer, index=False)
