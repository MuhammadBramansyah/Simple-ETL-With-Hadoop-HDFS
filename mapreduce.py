import os
import pandas as pd
import pickle
import connection

from hdfs import InsecureClient
from datetime import datetime

### mapper section ###
def mapper(data):
    mapped = []
    for index,row in data.iterrows():
        mapped.append((row['OrderDate'],row['Quantity']))        
    return mapped

if __name__ == "__main__":
    ## mengambil time
    time = datetime.now().strftime("%Y%m%d")

    ### ambil data di hadoop ###
    ## setup conf/connection hadoop connection 
    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    ## ambil data orders dari hadoop
    with client.read(f'/DigitalSkola/{time}/orders_{time}.csv', encoding='utf-8') as writer:
        df = pd.read_csv(writer)

    ### section mapper ###
    slice1 = df.iloc[0:1000,:]
    slice2 = df.iloc[1000:,:]

    map1 = mapper(slice1)
    map2 = mapper(slice2)

    ## mapper alogrithm
    shuffled = {}
    for i in [map1,map2]:
        for j in i:
            if j[0] not in shuffled:
                shuffled.update({j[0]:[]})
                
            shuffled[j[0]].append(j[1])
    print(shuffled)

    file = open('shuffled.pkl','ab')
    pickle.dump(shuffled,file)
    file.close()


