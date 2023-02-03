from AmazonS3 import AmazonS3
import json
from tqdm import tqdm
import os
import datetime
import pickle
from db import DBhelper
import collections
import pandas as pd
import re
from tqdm import tqdm

def PickleLoad(path_read):
    with open(path_read, 'rb') as f:
        data_list = pickle.load(f)
    return data_list

def fetch_source_domain_mapping(web_id):
    query = f"SELECT domain FROM source_domain_mapping where web_id='{web_id}'"
    #print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    source_domain_mapping = [d[0] for d in data]
    return source_domain_mapping

def fetch_web_id(utc_yd):
    query = f"""SELECT web_id FROM pageview_record_day where date ='{utc_yd}' ORDER by abs(record) desc limit 100"""
    data_a = DBhelper('dione').ExecuteSelect(query)
    return set([i[0] for i in data_a])

if __name__ == '__main__':
    domain_dict = {}
    awsS3 = AmazonS3('elephants3')
    utc_now = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    utc_yesterday = utc_now - datetime.timedelta(days=1)
    utc_day = utc_now.strftime("%Y-%m-%d")
    utc_yd = utc_yesterday.strftime("%Y-%m-%d")
    object_list =[[] for i in range(24)]
    for i in range(16):
        object_list[i] += list(awsS3.getDateHourObjects(utc_day, i))
    for j in range(16, 24):
        object_list[j] += list(awsS3.getDateHourObjects(utc_yd, j))
    dic = collections.defaultdict(int)
    for hour,obj_hour in enumerate(object_list):
        for i,obj in tqdm(enumerate(obj_hour), ascii=True, desc=f"{utc_day}"):
            raw = json.loads(awsS3.Read(obj.key))
            for r in raw:
                curr = r.get('web_id', '')
                if curr:
                    dic[hour] += 1
    print(dic)