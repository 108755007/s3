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
    ans = collections.defaultdict(list)
    for hour,obj_hour in enumerate(object_list):
        for i,obj in tqdm(enumerate(obj_hour), ascii=True, desc=f"{utc_day}"):
            raw = json.loads(awsS3.Read(obj.key))
            for r in raw:
                if r.get('web_id') != 'beauty321':
                    continue
                if not r.get('datetime'):
                    continue
                if not r.get('referrer_url'):
                    continue
                ans[r.get('uuid')].append([str_to_timetamp(r.get('datetime')),r.get('referrer_url')])
    # all_view = 0
    # likr_view = 0
    # for uuid,val in ans.items():
    #     val = sorted(val,key=lambda x:x[0])
    #     last_time = 0
    #     for i, view in enumerate(val):
    #         all_view += 1
    #         if last_time != 0 and (view[0] - last_time) < 1200:
    #             likr_view += 1
    #             last_time = view[0]
    #             continue
    #         like = re.findall('.*&utm_source=likr&utm_medium=pubnews.*', view[1])
    #         if like:
    #             likr_view += 1
    #             last_time = view[0]
    with open('rick.pickle', 'wb') as f:
        pickle.dump(ans, f)


