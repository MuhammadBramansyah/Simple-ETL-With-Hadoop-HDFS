
from numpy import column_stack
import pandas as pd 
import connection
import model

from sqlalchemy import create_engine
from hdfs import InsecureClient
from datetime import datetime

if __name__ == "__main__":
    ## setup time 
    time = datetime.now().strftime("%Y%m%d")

    ## create variabel postgresql
    engine = create_engine('postgresql://postgres:221299@localhost:5432/dwh_digitalskola')

    ## setup connection hadoop
    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    ## tarik data dari hadoop (read data from hadoop)
    with client.read(f"/DigitalSkola/{time}/user_{time}.csv", encoding = "utf-8") as writer: 
        df = pd.read_csv(writer)

    ## sending data yg udah jadi dataframe ke dwh
    df.to_sql("dwh_dim_users", engine, if_exists='replace', index=False)
    
    ## setting connection to postgresql
    conf_postgresql = connection.param_config("postgresql")
    conn = connection.postgres_conn(conf_postgresql)
    cur = conn.cursor()

    ## make sql variabel untuk manggil query table buat di dwh
    sql = model.dwh_fact_orders()

    ## execute query
    cur.execute(sql)

    ## memasukan data yg sudah di query kedalam variabel data
    data = cur.fetchall() ## fetchall = mengambil valuenya sajja

    ## memasukan data kedalam data frame
    df = pd.DataFrame(data, columns=[col[0] for col in cur.description])
    
    ## sending dataframe yg sudah dibuat ke dwh
    df.to_sql("dwh_fact_orders", engine, if_exists='replace', index=False)

