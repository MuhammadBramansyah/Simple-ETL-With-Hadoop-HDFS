import os
import json

import pandas
#import psycopg2

from sqlalchemy import create_engine

## insert data to database
if __name__ == "__main__":

    path = os.getcwd() + "\\" + "dataset" + "\\"

    for dic in [("TR_OrderDetails.csv","fact_orderdetails"),
                ("TR_Products.csv","dim_products"),
                ("TR_PropertyInfo.csv","dim_location"),
                ("TR_UserInfo.csv","dim_users")]:
        
        df = pandas.read_csv(path + dic[0])
        engine = create_engine('postgresql://postgres:221299@localhost:5432/digitalskola')
        df.to_sql(dic[1], engine, if_exists='replace', index=False)



