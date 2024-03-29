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

def fetch_specific():
    dic = collections.defaultdict(set)
    query = f"""SELECT web_id,article_code FROM pageview_specific_article """
    data = DBhelper('dione').ExecuteSelect(query)
    for web_id,code in data:
        dic[web_id].add(code)
    return dic

#
if __name__ == '__main__':
    domain_dict = {}
    awsS3 = AmazonS3('elephants3')
    utc_now = datetime.datetime.utcnow()
    utc_yesterday = utc_now - datetime.timedelta(days=1)
    utc_day = utc_now.strftime("%Y-%m-%d")
    utc_yd = utc_yesterday.strftime("%Y-%m-%d")
    web_id_list = fetch_web_id(utc_yd)
    specific = fetch_specific()
    specific_df = pd.DataFrame(columns=['web_id','article_code','current_url','referrer_url','datetimes','date'])
    d = {}
    for web_id in tqdm(web_id_list):
        d[web_id] = fetch_source_domain_mapping(web_id)
    for web_id, domains in d.items():
        domain_dict[web_id] = [web_id]
        for domain in domains:
            if not re.findall(web_id, domain):
                domain_dict[web_id].append(domain)
    dic = collections.defaultdict(int)
    dic_in = collections.defaultdict(int)
    object_list = []
    l, i3, u = [], [], []
    for i in range(16):
        object_list += list(awsS3.getDateHourObjects(utc_day, i))
    for j in range(16, 24):
        object_list += list(awsS3.getDateHourObjects(utc_yd, j))
    for i, obj in tqdm(enumerate(object_list), ascii=True, desc=f"{utc_day}"):
        raw = json.loads(awsS3.Read(obj.key))
        for r in raw:
            curr = r.get('web_id', '')
            if curr == 'lovingfamily':
                l.append(r)
            if curr == 'i3fresh':
                i3.append(r)
            if curr == 'upmedia':
                u.append(r)
            if curr:
                dic[curr] += 1
                if curr in web_id_list and r.get('referrer_url'):
                    for domain in domain_dict[curr]:
                        if re.findall(domain,r.get('referrer_url')):
                            dic_in[curr] += 1
                            break
                if curr in specific:
                    url = r.get('current_url')
                    if url:
                        for code in specific[curr]:
                            if code in url:
                                ref_url = r.get('referrer_url')
                                ref_url = ref_url if ref_url else '_'
                                specific_df.loc[len(specific_df)] = [curr,code,url, ref_url,r.get('datetime'),utc_day]

    with open(f'ntu_data/lovingfamily_{utc_day}.pickle', 'wb') as f:
        pickle.dump(l, f)
    with open(f'ntu_data/i3fresh_{utc_day}.pickle', 'wb') as f:
        pickle.dump(i3, f)
    with open(f'ntu_data/upmedia_{utc_day}.pickle', 'wb') as f:
        pickle.dump(u, f)


    df = pd.DataFrame.from_dict(dic, orient='index', columns=['record'])
    df = df.reset_index()
    df['web_id'] = df['index']
    df = df[['web_id', 'record']]

    df_in = pd.DataFrame.from_dict(dic_in, orient='index', columns=['internal'])
    df_in = df_in.reset_index()
    df_in['web_id'] = df_in['index']
    df_in = df_in[['web_id', 'internal']]

    df_mix = pd.merge(df,df_in,how='left',on='web_id')
    df_mix.fillna(0,inplace=True)
    df_mix['internal'] = df_mix['internal'].astype('int')
    df_mix['external'] = df_mix.apply(lambda x:x['record']-x['internal'] if x['web_id'] in web_id_list else 0,axis=1)
    df_mix['external_Traffic_percentage'] =[round(i,2) for i in df_mix['external']/df_mix['record']]
    df_mix['is_calculation'] = df_mix.apply(lambda x: 1 if x['web_id'] in web_id_list else 0,axis=1)
    df_mix['date'] = utc_day
    DBhelper.ExecuteUpdatebyChunk(df_mix, db='dione', table='pageview_record_day', chunk_size=100000, is_ssh=False)
    DBhelper.ExecuteUpdatebyChunk(specific_df, db='dione', table='pageview_specific_article_record', chunk_size=100000, is_ssh=False)
