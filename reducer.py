
import pandas as pd
import pickle
import json
import time


import connection

from datetime import datetime
from hdfs import InsecureClient

## reducing Section ## 
def reduce(shuffled_dict):
    reduced = {}
    
    for i in shuffled_dict: 
        ## Agregation
        reduced[i] = sum(shuffled_dict[i])
    return reduced


if __name__ == "__main__":
    
    time = datetime.now().strftime("%Y%m%d")

    file= open('shuffled.pkl','rb')
    shuffled = pickle.load(file)

    ## setup hadoop
    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    ## Reducing step
    final = reduce(shuffled)
    
    ## make to dataframe 
    df = pd.DataFrame(final, index=[0])

    ## load data reduce ke hadoop
    with client.write(f'/DigitalSkola/{time}/db_mart_quantity_day_{time}.csv', encoding='utf-8') as writer:
            df.to_csv(writer, index=False)
    
    print("quantity Transaction ... ")