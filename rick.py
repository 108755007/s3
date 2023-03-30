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


def str_to_timetamp(s):
    return datetime.datetime.timestamp(datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    for kk in range(2, 6):
        domain_dict = {}
        awsS3 = AmazonS3('elephant-new')
        utc_now = datetime.datetime.utcnow() - datetime.timedelta(days=kk)
        utc_yesterday = utc_now - datetime.timedelta(days=1)
        utc_day = utc_now.strftime("%Y-%m-%d")
        utc_yd = utc_yesterday.strftime("%Y-%m-%d")
        object_list =[[] for i in range(24)]
        for i in range(16):
            object_list[i] += list(awsS3.getDateHourObjects(utc_day, i))
        for j in range(16, 24):
            object_list[j] += list(awsS3.getDateHourObjects(utc_yd, j))
        g = {}
        g['upmedia'] = collections.defaultdict(list)
        g['nownews'] = collections.defaultdict(list)
        ans = collections.defaultdict(list)
        d = 0
        u = 0
        for hour,obj_hour in enumerate(object_list):
            for i,obj in tqdm(enumerate(obj_hour), ascii=True, desc=f"{utc_day}"):
                raw = json.loads(awsS3.Read(obj.key))
                for r in raw:
                    if r.get('web_id') == 'upmedia' or r.get('web_id') == 'nownews':
                        if r.get('web_id') == 'upmedia':
                            u += 1
                        if r.get('web_id') == 'nownews':
                            d +=1
                        if not r.get('datetime'):
                            continue
                        if not r.get('referrer_url'):
                            continue
                        if not r.get('uuid'):
                            continue
                        g[r.get('web_id')][str(r.get('uuid'))].append([str_to_timetamp(r.get('datetime')),r.get('referrer_url')])
        g['u'] = u
        g['d'] = d
        with open(f'rick_{kk}.pickle', 'wb') as f:
            pickle.dump(g, f)


