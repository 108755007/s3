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
    date_list = []
    for d in range(334, 658):
        date_list.append(str(datetime.date.today() - datetime.timedelta(d)))
    for dd in range(len(date_list) - 1):
        utc_day = date_list[dd]
        utc_yd = date_list[dd + 1]
        dic = collections.defaultdict(int)
        object_list = []
        for i in range(16):
            object_list += list(awsS3.getDateHourObjects(utc_day, i))
        for j in range(16, 24):
            object_list += list(awsS3.getDateHourObjects(utc_yd, j))
        for i, obj in tqdm(enumerate(object_list), ascii=True, desc=f"{utc_day}"):
            try:
                raw = json.loads(awsS3.Read(obj.key))
                for r in raw:
                    curr = r.get('web_id', '')
                    if curr:
                        dic[curr] += 1
            except json.decoder.JSONDecodeError as e:
                try:
                    n = int(str(e).split('char')[-1][:-1])
                    raw = json.loads(awsS3.Read(obj.key)[:n]+']')
                    for r in raw:
                        curr = r.get('web_id', '')
                        if curr:
                            dic[curr] += 1
                except:
                    pass
            except:
                pass
        df = pd.DataFrame.from_dict(dic, orient='index', columns=['record'])
        df = df.reset_index()
        df['web_id'] = df['index']
        df = df[['web_id', 'record']]
        df['date'] = utc_day
        DBhelper.ExecuteUpdatebyChunk(df, db='dione', table='pageview_record_day', chunk_size=100000, is_ssh=False)