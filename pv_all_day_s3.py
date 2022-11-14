from AmazonS3 import AmazonS3
import json
from tqdm import tqdm
import os
import datetime
import pickle
from db import DBhelper
import collections
import pandas as pd

def PickleLoad(path_read):
    with open(path_read, 'rb') as f:
        data_list = pickle.load(f)
    return data_list


if __name__ == '__main__':
    awsS3 = AmazonS3('elephants3')
    utc_now = datetime.datetime.utcnow()
    utc_yesterday = utc_now - datetime.timedelta(days=1)
    utc_day = utc_now.strftime("%Y-%m-%d")
    utc_yd = utc_yesterday.strftime("%Y-%m-%d")
    dic = collections.defaultdict(int)
    object_list = []
    for i in range(16):
        object_list += list(awsS3.getDateHourObjects(utc_day, i))
    for j in range(16, 24):
        object_list += list(awsS3.getDateHourObjects(utc_yd, j))


    for i, obj in tqdm(enumerate(object_list), ascii=True, desc=f"{utc_day}"):
        raw = json.loads(awsS3.Read(obj.key))
        for r in raw:
            curr = r.get('web_id', '')
            if curr:
                dic[curr] += 1
    df = pd.DataFrame.from_dict(dic, orient='index', columns=['record'])
    df = df.reset_index()
    df['web_id'] = df['index']
    df = df[['web_id', 'record']]
    df['date'] = utc_day
    DBhelper.ExecuteUpdatebyChunk(df, db='dione', table='pageview_record_day', chunk_size=100000, is_ssh=False)