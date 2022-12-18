#  coding: utf-8 
"""
Created on Tue Jun 18 11:46:38 2019

@author: xucoolboy
"""

from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import sys
import os
import requests
import json
import pymysql 
from urllib3 import request

#建立mgo库连接
engine=create_engine("mysql+pymysql://bi:VS6w{{6wiv@rr-2ze2k9sn688w5h3z51o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format('mgo'))
def reader(query):
    sql=query
    df=pd.read_sql(sql,engine)
    return df
#建立mring库连接
engine1=create_engine("mysql+pymysql://bi:VS6w{{6wiv@rr-2ze2k9sn688w5h3z51o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format('mring'))
def reader_mring(query):
    sql=query
    df=pd.read_sql(sql,engine1)
    return df


#建立bi库连接
con = pymysql.connect(host='47.96.122.220',user='bi',
                      passwd='VS6w{6wiv',charset='utf8')
cur = con.cursor()#获取光标
cur.execute('use bi')#使用数据库

#token获取
header2={"Content-Type":"application/json"}
data1={"username":"admin","password":"17fuwc"}
response0=requests.post(url="https://data.mobilemart.cn/api/auth/login?project=production",
     data=json.dumps(data1),headers=header2)
#测试token是否可用
header1={"Content-Type": "application/json", "sensorsdata-token": 
         json.loads(response0.text)['token']}
#获取神策数据方法1
def get_sensor(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame(columns=['video','num'])
    for i in range (0,len(response.text.split('\n'))-1):
        video=json.loads(text_split[i])['video']
        num=json.loads(text_split[i])['num']
        sub_df=pd.DataFrame(data={'video':[video],'num':[num]})
        df=pd.concat([df,sub_df],ignore_index=True)
    return df

#获取神策数据方法2
def get_sensor2(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame(columns=['day','num'])
    for i in range (0,len(response.text.split('\n'))-1):
        day=json.loads(text_split[i])['day']
        num=json.loads(text_split[i])['num']
        sub_df=pd.DataFrame(data={'day':[day],'num':[num]})
        df=pd.concat([df,sub_df],ignore_index=True)
    return df

#获取神策数据方法3
def get_sensor3(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame(columns=['date','trip_cnt','trip_time_sum'])
    for i in range (0,len(response.text.split('\n'))-1):
        day=json.loads(text_split[i])['date']
        trip_cnt=json.loads(text_split[i])['trip_cnt']
        trip_time_sum=json.loads(text_split[i])['trip_time_sum']
        sub_df=pd.DataFrame(data={'date':[day],'trip_cnt':[trip_cnt],'trip_time_sum':[trip_time_sum]})
        df=pd.concat([df,sub_df],ignore_index=True)
    return df

#获取神策数据简化方法
def get_sensor_info(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame()
    for i in range (0,len(text_split)-1):
        df=df.append(json.loads(text_split[i]),ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df

test_dev_list=''' '1d5559d3cb1dc2ad24051519cf546a18',
'5fbe08d956f7abed75f2e0773e628ccf',
'a85293469e50afddb55c9bbcdfccb566',
'64261dc5ff5e1ffe5f8b3a296bc7ef12',
'3dcbbcbefbc3b4565ea491268105ae77',
'3d288ed8ced8010349f2e68a7d9e7563',
'79e1a35bc7a5191e413d8ffa26dcfbaf',
'a40357a62156df25938d10aa8056138e',
'44f748d3fb8ae6f6dd908bc02361c00b',
'973a857ab199ba24160739e053244476',
'6890aa362c0fbb55dd34ab44d442bb33',
'29cfe8445736a92a135dfc24de8f2ecf',
'fe9deacee9a4422b23fb27847a69442d',
'a184f8ee18fa537b8c16e669a0176c65' '''
#获取神策累计值方法
def get_total_sensor(sql,start_date,end_date):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info(sql.format(test=test_dev_list,day=day))
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df

def get_sensor_info2(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame()
    for i in range (0,len(text_split)-1):
        df=df.append(json.loads(text_split[i]),ignore_index=True)
    return df

def get_mring_total_info(sql,start_date,end_date):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        sub_df=reader_mring(sql.format(day,day,day,day,day))
        df=pd.concat([df,sub_df],ignore_index=True)
    return df

def get_mring_zr_info(sql,start_date,end_date):
    sql0='''SELECT  a.third_code
    FROM  mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  (b.main_app='com.mgo.gad')
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
    AND b.desc NOT LIKE '%%测试%%'
    '''
    df=pd.DataFrame(columns=['day','num'])
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        df0=reader_mring(sql0).fillna(0)
        df1=get_sensor_info2(sql.format(day=day,test=test_dev_list)).fillna(0)
        num=df1.distinct_id[df1.distinct_id.isin(df0.third_code)].count()
        sub_df=pd.DataFrame(data={'day':[day],'num':[num]})
        df=pd.concat([df,sub_df],ignore_index=True)
    return df

def get_mring_zr_sum_info(sql,start_date,end_date):
    sql0='''SELECT  a.third_code
    FROM  mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  (b.main_app='com.mgo.gad')
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
    AND b.desc NOT LIKE '%%测试%%'
    '''
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        df0=reader_mring(sql0).fillna(0)
        df1=get_sensor_info2(sql.format(day=day,test=test_dev_list)).fillna(0)
        cols=[col for col in df1.columns if col not in ['distinct_id'] ]
        ss=df1[cols][df1.distinct_id.isin(df0.third_code)].sum()
        ss['day']=day
        df=df.append(ss,ignore_index=True)
    return df


sql0='''SELECT  a.third_code,c.city_sn,c.car_type
    FROM  mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  (b.main_app='com.mgo.gad')
    AND b.desc NOT LIKE '%%测试%%'
    '''
df_city=reader_mring(sql0)

def get_sensor_info_mgo_sum(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame()
    for i in range (0,len(text_split)-1):
        df=df.append(json.loads(text_split[i]),ignore_index=True)
    df=df.merge(df_city,left_on='distinct_id',right_on='third_code',how='inner').drop('third_code',axis=1).\
    groupby(['date','city_sn','car_type']).sum().reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df

def get_sensor_info_mgo_count(sql):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame()
    for i in range (0,len(text_split)-1):
        df=df.append(json.loads(text_split[i]),ignore_index=True)
    df=df.merge(df_city,left_on='distinct_id',right_on='third_code',how='inner').drop('third_code',axis=1).\
    groupby(['date','city_sn','car_type']).count().reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df

def get_total_sensor_mgo_sum(sql,start_date,end_date):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info_mgo_sum(sql.format(test=test_dev_list,day=day))
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df

def get_total_sensor_mgo_count(sql,start_date,end_date):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info_mgo_count(sql.format(test=test_dev_list,day=day))
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df

def get_sensor_info_mgo_group_sum(sql,L=[]):
    response=requests.post(url="https://data.mobilemart.cn/api/sql/query?{0}&format=format&project=production"\
                                   .format(request.urlencode({'q':sql})),data='{"mode": "raw","raw": ""}',headers=header1)
    text_split=response.text.split('\n')
    df=pd.DataFrame()
    for i in range (0,len(text_split)-1):
        df=df.append(json.loads(text_split[i]),ignore_index=True)
    df=df.merge(df_city,left_on='distinct_id',right_on='third_code',how='inner').drop('third_code',axis=1).\
    groupby(L).sum().reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df

## 单广告&内容每日数据
#建立mring库连接
con0 = pymysql.connect(host='rm-2zej847swc7p120t0o.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w{6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库

#日播放次数播放行程数
sql0='SELECT MAX(day) FROM video_sensor_daily'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,19)
sql6='''SELECT date,contentId,tag,COUNT(DISTINCT traceId) play_trip_cnt,
SUM(CASE WHEN tag='SCREEN_OTHER_CONTENT_BEGIN' THEN 1 
WHEN tag='splash_show_begin' THEN 1 
WHEN (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%') THEN 1 
ELSE 0 END ) play_cnt
FROM events 
WHERE tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','SCREEN_OTHER_RECOVER_END','video_banner_click','click_video_detail')
AND env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#日播放设备数&时间
sql7='''SELECT date,contentId,tag,COUNT(DISTINCT distinct_id) play_dev_cnt,SUM(playTime) play_time
FROM events 
WHERE env='prod'
AND tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','SCREEN_OTHER_RECOVER_END','video_banner_click','click_video_detail')
AND date BETWEEN '{}' AND '{}'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND distinct_id NOT IN ({})
GROUP BY date,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#日扫码次数
sql8='''SELECT date,CASE WHEN utmcontent LIKE '%http://l.zuzuche.com/sBLPP%' THEN '3897f62797cc8926e2bfc0280949b28b' 
  WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/pengCar' THEN '9300b8665e10f25cc6c4201473d7c8a6'
 WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/NJBank' THEN '38c14707cf33c08f0e213310342b1c2c'
 WHEN utmcontent='http://t.cn/AiY3hTFc' THEN '1f194067f49862b087238381cad0fa07'
 WHEN utmcontent='http://t.cn/AiT9t99D' THEN 'b83f67867c4d0324e3f5cda04c7a7318' 
 WHEN utmcontent='http://t.cn/AiQYGkPG' THEN '13e9ceb5b606d87cf1ce266e1e694660'
 WHEN utmcontent='https://dwz.cn/fSLEoGZr' THEN '635ada9d3102a1a7ac7d5c63818d9dfc'
 WHEN utmcontent='https://dwz.cn/yCSk977F' THEN '643356f5876623b6d13a87b6916f0cbb'
 WHEN utmcontent='https://dwz.cn/ZFBaiNFH' THEN '881cd15c414ceb26506a857f9b4d0b0d'
 WHEN utmcontent IN ('https://dwz.cn/VFm7cWbK','http://h5.mobilemart.cn/screenMarketing/creditCard') AND date>='2019-10-14' 
 THEN 'd24873d3f4f2aa352b71f151870c0d23'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN 'e83611ec7a0556351e2e9548cc741d2d'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN 'd4398ebad582e9ce7cc06c39f9fbcf17'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN '21bc96c9691539b5ea6fee66cf2919db'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN '400d6d636f3b66cd2c47e75641756bf5'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN '7af074ceb499fb6dd2074e43d7ae5770'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN 'e1bf5dfe25705c78e62dcef9a63a83bd'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN 'd308656e6b8d07338f46735e1f520184'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN '5d14aee1e31f9afa98fc0cabde7b3dcf'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN '1c89145b29e80e84aeb8876ea9c62c3c'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN 'd1815729522b865d324b11c1696213cd'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN '0652c2d252db49bf0d6e536d769b9a8f'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN 'e18bec5d6b895f7f668716f56c88f488'
 ELSE 'else ' END video,COUNT(1) scan_cnt FROM events 
WHERE event='SCAN'
AND date BETWEEN '{}' AND '{}'
AND env='prod'
-- AND versioncode>=179
AND distinct_id NOT IN ({})
GROUP BY 
date,video'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#累计播放次数点位数
sql9='''SELECT '{day}' `date`,contentId,tag,COUNT(DISTINCT distinct_id) total_play_dev_cnt,
SUM(CASE WHEN tag='SCREEN_OTHER_CONTENT_BEGIN' THEN 1 WHEN tag='splash_show_begin' THEN 1 
WHEN tag='video_banner_click' THEN 1 WHEN tag='click_video_detail' THEN 1
WHEN (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%') THEN 1 ELSE 0 END ) total_play_cnt,
SUM(playTime) total_play_time FROM events 
WHERE env='prod'
AND tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','SCREEN_OTHER_RECOVER_END','video_banner_click','click_video_detail')
AND date<='{day}'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND distinct_id NOT IN ({test})
GROUP BY contentId,tag'''
#累计扫码次数
sql10='''SELECT '{day}' `date`,CASE WHEN utmcontent LIKE '%http://l.zuzuche.com/sBLPP%' THEN '3897f62797cc8926e2bfc0280949b28b' 
  WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/pengCar' THEN '9300b8665e10f25cc6c4201473d7c8a6'
 WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/NJBank' THEN '38c14707cf33c08f0e213310342b1c2c'
 WHEN utmcontent='http://t.cn/AiY3hTFc' THEN '1f194067f49862b087238381cad0fa07'
 WHEN utmcontent='http://t.cn/AiT9t99D' THEN 'b83f67867c4d0324e3f5cda04c7a7318' 
 WHEN utmcontent='https://dwz.cn/fSLEoGZr' THEN '635ada9d3102a1a7ac7d5c63818d9dfc'
 WHEN utmcontent='https://dwz.cn/yCSk977F' THEN '643356f5876623b6d13a87b6916f0cbb'
 WHEN utmcontent='https://dwz.cn/ZFBaiNFH' THEN '881cd15c414ceb26506a857f9b4d0b0d'
 WHEN utmcontent IN ('https://dwz.cn/VFm7cWbK','http://h5.mobilemart.cn/screenMarketing/creditCard') AND date>='2019-10-14' 
 THEN 'd24873d3f4f2aa352b71f151870c0d23'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN 'e83611ec7a0556351e2e9548cc741d2d'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN 'd4398ebad582e9ce7cc06c39f9fbcf17'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN '21bc96c9691539b5ea6fee66cf2919db'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN '400d6d636f3b66cd2c47e75641756bf5'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN '7af074ceb499fb6dd2074e43d7ae5770'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='hgyj' AND date>='2019-10-17' THEN 'e1bf5dfe25705c78e62dcef9a63a83bd'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN 'd308656e6b8d07338f46735e1f520184'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN '5d14aee1e31f9afa98fc0cabde7b3dcf'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN '1c89145b29e80e84aeb8876ea9c62c3c'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN 'd1815729522b865d324b11c1696213cd'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN '0652c2d252db49bf0d6e536d769b9a8f'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='ttjianshen' AND date>='2019-10-24' THEN 'e18bec5d6b895f7f668716f56c88f488'
 ELSE 'else ' END video,COUNT(1) total_scan_cnt FROM events 
WHERE event='SCAN'
AND date<='{day}'
AND env='prod'
-- AND versioncode>=179
AND distinct_id NOT IN ({test})
GROUP BY 
video'''
sql01='''insert into video_sensor_daily values('{}','{}','{}',{},{},{},{},{},{},{},{},{})'''
df6=get_sensor_info(sql6)
df7=get_sensor_info(sql7)
df8=get_sensor_info(sql8)
df9=get_total_sensor(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df10=get_total_sensor(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df_video=df6.merge(df7,how='left',on=['date','contentid','tag']).merge(df8,how='left',left_on=['date','contentid'],right_on=['date','video']).\
drop('video',axis=1).merge(df9,how='left',on=['date','contentid','tag']).\
merge(df10,how='left',left_on=['date','contentid'],right_on=['date','video']).fillna(0).drop('video',axis=1)
df_video=df_video[['date','contentid','tag','play_dev_cnt','play_trip_cnt','play_cnt','scan_cnt','total_play_dev_cnt',
                   'total_play_cnt','total_scan_cnt','play_time','total_play_time']]
for i in range(0,len(df_video)):
    L=[]
    for j in range(0,len(df_video.iloc[0])):
        L.append(df_video.iloc[i][j])
    cur0.execute(sql01.format(*L))
    if i/100==0:
        con0.commit()
    else:continue
con0.commit()

### 更新扫码记录表

sql0='SELECT MAX(day) FROM mring_sensor_scan'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,9,30)
sql='''SELECT utmSource,utmCampaign,utmContent,date  FROM events 
WHERE event='SCAN'
AND env='prod' 
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql0='''INSERT INTO mring_sensor_scan values('{}','{}','{}','{}')'''
df=get_sensor_info(sql)
df=df[['utmsource','utmcampaign','utmcontent','date']]
for i in range(0,len(df)):
    L=[]
    for j in range(0,len(df.iloc[0])):
        L.append(df.iloc[i][j])
    cur0.execute(sql0.format(*L))
con0.commit()


## 3.0数据

#导入神策每日行程数据
#导入神策每日行程数据
sql0='SELECT MAX(day) FROM mring_sensor_trip_day_v3'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,19)
#每日灰度设备行程及内容广告播放情况,1011安装adSn不为空和视频播放正常结束判断贴片
sql7='''
SELECT distinct_id,date,COUNT(1) cnt,SUM(inter) total,SUM(ad_cnt) ad_cnt,
SUM(show_video_start) show_video_start,SUM(video_time) video_time FROM 
(SELECT distinct_id,traceId,date,(MAX(timestamp)-MIN(timestamp))/1000 inter,
SUM(CASE WHEN (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'
AND adSn!='') 
OR tag='splash_show_begin' THEN 1 ELSE 0 END) ad_cnt,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) show_video_start,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' THEN playTime ELSE 0 END) video_time
FROM events WHERE traceId IS NOT NULL AND traceId!=''
AND env='prod' AND versioncode>=179
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY distinct_id,traceId,date) a
GROUP BY distinct_id,date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#当日点位数
sql8='''
SELECT date,distinct_id
FROM events WHERE traceId IS NOT NULL AND traceId!=''
AND env='prod' AND versioncode>=179
AND date BETWEEN '{}' AND '{}' 
AND distinct_id NOT IN ({})
GROUP BY distinct_id,date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#累计点位数(根据是否播放开屏定义点位)
sql9='''select DISTINCT '{day}' `date`,distinct_id
FROM events where env='prod' AND tag='splash_show_begin'
AND date<='{day}'
AND distinct_id NOT IN ({test})
'''
#累计出车情况
sql10='''SELECT DISTINCT '{day}' `date`,distinct_id FROM events 
WHERE env='prod' AND versioncode>=179 AND traceId!='' 
AND traceId IS NOT NULL AND date<='{day}'
AND distinct_id NOT IN ({test})
'''
#当日点位数
sql11='''SELECT date,distinct_id
FROM events WHERE env='prod'
AND tag='splash_show_begin'
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date,distinct_id'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
df7=get_sensor_info_mgo_sum(sql7)
df8=get_sensor_info_mgo_count(sql8)
df9=get_total_sensor_mgo_count(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df10=get_total_sensor_mgo_count(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df11=get_sensor_info_mgo_count(sql11)
sql101='''insert into mring_sensor_trip_day_v3 values('{}','{}','{}',{},{},{},{},{},{},{},{},{})'''
df_ad=df7.merge(df8,on=['date','city_sn','car_type'],how='left').merge(df9,on=['date','city_sn','car_type'],how='left').\
merge(df10,on=['date','city_sn','car_type'],how='left').merge(df11,on=['date','city_sn','car_type'],how='left')
df_ad.columns=['date', 'city_sn', 'car_type', 'ad_cnt', 'trip_cnt', 'show_video_start',
       'trip_time_sum', 'video_time', 'daily_trip_dev_cnt', 'total_play_dev_cnt',
       'total_trip_dev_cnt', 'daily_play_dev_cnt']
df_ad=df_ad[['date','city_sn', 'car_type','daily_trip_dev_cnt','daily_play_dev_cnt','total_trip_dev_cnt',
                         'total_play_dev_cnt','ad_cnt','show_video_start','trip_cnt','trip_time_sum','video_time']].fillna(0).\
sort_values(['date','city_sn','car_type'])
for i in range(0,len(df_ad)):
    L=[]
    for j in range(0,len(df_ad.iloc[0])):
        L.append(df_ad.iloc[i][j])
    cur0.execute(sql101.format(*L))
con0.commit()

### 更新mring_day表日期

sql0='SELECT MAX(day) FROM mring_day'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,1)
sql='''insert into mring_day values('{}')'''
for day in  pd.date_range(start=recent_day+timedelta(days=1),end=datetime.now().date()-timedelta(days=1),freq='1D'):
    day=day.date()
    cur0.execute(sql.format(day)) 
con0.commit()

## 更新非正常设备列表
sql0='SELECT MAX(day) FROM abnormal_device_list'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
sql1='''SELECT device_name FROM abnormal_device_list WHERE day='{}' '''
sql2='''SELECT DISTINCT distinct_id FROM events WHERE env='prod' AND tag='splash_show_begin' AND date='{}' '''
sql3='''INSERT abnormal_device_list values('{}','{}','{}') '''
sql4='''SELECT DISTINCT distinct_id FROM events WHERE env='prod' AND tag='splash_show_begin' AND date<='{}' '''
sql5='''SELECT  a.third_code
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad' ) AND b.desc NOT LIKE '%%测试%%' AND DATE(a.add_time)<='{}' '''
#未出车设备列表
sql6='''SELECT  a.third_code
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad' ) AND b.desc NOT LIKE '%%测试%%' AND DATE(a.add_time)<='{}' 
    AND a.third_code NOT IN
    (SELECT DISTINCT device_name FROM
    (SELECT device_name,add_time FROM mgo.mgo_screen_trip
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_201908
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_201909
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_201910
    ) A WHERE DATE(add_time)<='{}' )'''
#出车设备列表
sql7='''SELECT  a.third_code
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad' ) AND b.desc NOT LIKE '%%测试%%' AND DATE(a.add_time)<='{}' 
    AND a.third_code IN
    (SELECT DISTINCT device_name FROM
    (SELECT device_name,add_time FROM mgo.mgo_screen_trip
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_201908
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_201909
    UNION
    SELECT device_name,add_time FROM mgo.mgo_screen_trip_201910
    ) A WHERE DATE(add_time)<='{}') '''
#播放开屏设备列表
sql8='''SELECT DISTINCT distinct_id FROM events 
WHERE env='prod' AND tag='splash_show_begin'
                        AND date<='{}'
                        AND distinct_id NOT IN ({})'''
for day in  pd.date_range(start=recent_day+timedelta(days=1),end=date.today()-timedelta(days=1),freq='1D'):
    day=day.date()
    #从神策API返回数据
    df1=reader_mring(sql1.format(day-timedelta(days=1)))
    df2=get_sensor_info2(sql2.format(day))
    df4=get_sensor_info2(sql4.format(day))
    df5=reader_mring(sql5.format(day))
    #剔除有开屏播放后的设备列表
    df_dim1=df1.device_name[(~df1.device_name.isin(df2.distinct_id))&df1.device_name.isin(df5.third_code)].reset_index(drop=True)
    #加入所有未有开屏播放的设备列表
    df_dim2=df5.third_code[~df5.third_code.isin(df4.distinct_id)].reset_index(drop=True)
    df6=reader_mring(sql6.format(day,day))
    df7=reader_mring(sql7.format(day,day))
    df8=get_sensor_info2(sql8.format(day,test_dev_list))
    df6['desc']='未出车'
    df9=df7[~df7.third_code.isin(df8.distinct_id)].reset_index(drop=True)
    df9['desc']='出车未开屏'
    df10=pd.DataFrame(pd.concat([df_dim1,df_dim2]))
    df10.columns=['third_code']
    df10=df10.drop_duplicates('third_code')
    df11=pd.concat([df6,df9])
    df12=df10.merge(df11,how='left',on='third_code')
    for i in range (0,len(df12)):
        cur0.execute(sql3.format(df12['third_code'][i],day,df12['desc'][i]))
    con0.commit()

cur0.close()
con0.close()

##sensor_daily
#建立mring库连接
con0 = pymysql.connect(host='rm-2zej847swc7p120t0o.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w{6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库
## 神策每日行程及行程时间数据
## mring_sensor_daily
#更新魔晶业务日报中间表sensor_daily
#更新魔晶业务日报中间表加入trip_v3
sql0='SELECT MAX(day) FROM mring_sensor_daily'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,7,31)
sql8='''SELECT a.day,a.city_sn,a.car_type,累计安装设备数,累计出车数,g.total_play_dev_cnt,
g.trip_cnt 行程总数,ROUND(g.trip_time_sum/60,0) 行程总时间 FROM 
(SELECT a.day,b.city_sn,b.car_type,COUNT(1) 累计安装设备数 FROM
(SELECT day FROM mring.mring_day) a 
JOIN
(SELECT  a.third_code,DATE(a.add_time) day,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad' ) AND b.desc NOT LIKE '%%测试%%') b
ON a.day>=b.day
WHERE a.day>='2019-08-12' AND a.day BETWEEN '{}' AND '{}'
GROUP BY a.day,b.city_sn,b.car_type) a 
LEFT JOIN 
(SELECT a.day,b.city_sn,b.car_type,COUNT(DISTINCT b.device_name) 累计出车数 FROM
(SELECT day FROM mring.mring_day) a 
JOIN 
(SELECT `day`,device_name,b.city_sn,b.car_type FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201908
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201909
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201910
) A) a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad' ) AND b.desc NOT LIKE '%%测试%%'
) b ON a.device_name=b.third_code
GROUP BY a.day,device_name,b.city_sn,b.car_type) b ON a.day>=b.day
GROUP BY a.day,b.city_sn,b.car_type) f ON a.day=f.day AND a.city_sn=f.city_sn AND a.car_type=f.car_type
LEFT JOIN 
mring_sensor_trip_day_v3 g ON a.day=g.day AND a.city_sn=g.city_sn AND a.car_type=g.car_type
order by a.day DESC'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql9='''SELECT '{}' day,city_sn,car_type,COUNT(1) normal_dev_num FROM
(SELECT  a.third_code,c.city_sn,c.car_type,DATE(a.add_time) day
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad' ) AND b.desc NOT LIKE '%%测试%%'
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') ) a
WHERE a.day<='{}' 
GROUP BY city_sn,car_type'''
sql10='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT device_name) daily_trip_dev_num FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A) a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  (b.main_app='com.mgo.gad') AND b.desc NOT LIKE '%%测试%%'
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') 
) b ON a.device_name=b.third_code
GROUP BY b.city_sn,car_type'''
sql11='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT sn) daily_online_dev_num FROM mring_heartbeat_day a
JOIN 
(
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  (b.main_app='com.mgo.gad') AND b.desc NOT LIKE '%%测试%%'
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}')
) b ON a.sn=b.third_code
WHERE a.day='{}'
AND a.sn IN
(SELECT DISTINCT device_name FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A)
GROUP BY b.city_sn,b.car_type'''
sql12='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT a.sn) daily_play_dev_num
from (SELECT * FROM mring.mring_sensor_summary where tag='splash_show_begin' AND day='{}') a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  (b.main_app='com.mgo.gad') AND b.desc NOT LIKE '%%测试%%'
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') 
) b ON a.sn=b.third_code
GROUP BY b.city_sn,b.car_type'''
df8=reader_mring(sql8).fillna(0)
df8['day']=df8['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df9=get_mring_total_info(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df10=get_mring_total_info(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df11=get_mring_total_info(sql11,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df12=get_mring_total_info(sql12,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df_daily=df8.merge(df9,how='left',on=['day','city_sn','car_type']).merge(df10,how='left',on=['day','city_sn','car_type']).\
merge(df11,how='left',on=['day','city_sn','car_type']).merge(df12,how='left',on=['day','city_sn','car_type'])
df_daily=df_daily[['day','city_sn','car_type','累计安装设备数','累计出车数','total_play_dev_cnt','normal_dev_num','daily_trip_dev_num',
'daily_online_dev_num','daily_play_dev_num','行程总数','行程总时间']].sort_values(['day','city_sn','car_type']).\
fillna(0).reset_index(drop=True)
sql13='''insert into mring_sensor_daily values('{}','{}','{}',{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df_daily)):
    L=[]
    for j in range(0,len(df_daily.iloc[0])):
        L.append(df_daily.iloc[i][j])
    cur0.execute(sql13.format(*L))
con0.commit()   

## 展荣代理商数据

#更新魔晶业务日报中间表加入trip_v3
sql0='SELECT MAX(day) FROM mring_sensor_daily_shzr'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,10,18)
sql7='''SELECT a.day,累计安装设备数,累计出车数 FROM 
(SELECT a.day,COUNT(1) 累计安装设备数 FROM
(SELECT day FROM mring.mring_day) a 
JOIN
(SELECT  a.third_code,DATE(a.add_time) day
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  (b.main_app='com.mgo.gad' )
AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')) b
ON a.day>=b.day
WHERE a.day>='2019-08-12' AND a.day BETWEEN '{}' AND '{}'
GROUP BY a.day) a 
LEFT JOIN 
(SELECT a.day,COUNT(DISTINCT b.device_name) 累计出车数 FROM
(SELECT day FROM mring.mring_day) a 
JOIN 
(SELECT `day`,device_name FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201908
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201909
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201910
) A) a
JOIN (
    SELECT  a.third_code,a.bound_time
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  (b.main_app='com.mgo.gad' )
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
) b ON a.device_name=b.third_code
GROUP BY a.day,device_name) b ON a.day>=b.day
GROUP BY a.day) f ON a.day=f.day
order by a.day'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql8='''select DISTINCT distinct_id
FROM events where env='prod' AND tag='splash_show_begin'
AND date<='{day}'
AND distinct_id NOT IN ({test})
'''
sql9='''SELECT '{}' day,COUNT(1) normal_dev_num FROM
(SELECT  a.third_code,DATE(a.add_time) day
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  (b.main_app='com.mgo.gad' )
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') ) a
WHERE a.day<='{}' '''
sql10='''SELECT '{}' day,COUNT(DISTINCT device_name) daily_trip_dev_num FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A) a
JOIN (
    SELECT  a.third_code,a.bound_time
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  (b.main_app='com.mgo.gad')
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') 
) b ON a.device_name=b.third_code'''
sql11='''SELECT '{}' day,COUNT(DISTINCT sn) daily_online_dev_num FROM mring_heartbeat_day a
JOIN 
(
    SELECT  a.third_code,a.bound_time
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND b.desc NOT LIKE '%%测试%%' AND  (b.main_app='com.mgo.gad')
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}')
) b ON a.sn=b.third_code
WHERE a.day='{}'
AND a.sn IN
(SELECT DISTINCT device_name FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A)'''
sql12='''SELECT '{}' day,COUNT(DISTINCT a.sn) daily_play_dev_num
from (SELECT * FROM mring.mring_sensor_summary where tag='splash_show_begin' AND day='{}') a
JOIN (
    SELECT  a.third_code,a.bound_time
    FROM      mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND b.desc NOT LIKE '%%测试%%' AND (b.main_app='com.mgo.gad')
    AND c.leasing_company_sn IN ('SH-0001','SH-0002','SH-0003','SH-0004','SH-0005','SH-0006','SH-0037')
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') 
) b ON a.sn=b.third_code'''
sql13='''SELECT distinct_id,COUNT(1) trip_cnt,SUM(inter) trip_time_total,SUM(ad_cnt) ad_cnt,
SUM(show_video_start) show_video_start,SUM(video_time) video_time FROM 
(SELECT distinct_id,traceId,(MAX(timestamp)-MIN(timestamp))/1000 inter,
SUM(CASE WHEN (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'
AND adSn!='') 
OR tag='splash_show_begin' THEN 1 ELSE 0 END) ad_cnt,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) show_video_start,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' THEN playTime ELSE 0 END) video_time
FROM events WHERE traceId IS NOT NULL AND traceId!=''
AND env='prod' AND versioncode>=179
AND date='{day}' 
AND distinct_id NOT IN ({test})
GROUP BY distinct_id,traceId) a
GROUP BY distinct_id '''
df7=reader_mring(sql7).fillna(0)
df7['day']=df7['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df8=get_mring_zr_info(sql8,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df9=get_mring_total_info(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df10=get_mring_total_info(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df11=get_mring_total_info(sql11,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df12=get_mring_total_info(sql12,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df13=get_mring_zr_sum_info(sql13,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df_all=pd.concat([df7,df8,df9,df10,df11,df12,df13],axis=1)[['day','累计安装设备数','累计出车数','num','normal_dev_num',
    'daily_trip_dev_num','daily_online_dev_num','daily_play_dev_num','trip_cnt','trip_time_total','ad_cnt','show_video_start','video_time']].\
    iloc[:,6:]

sql14='''insert into mring_sensor_daily_shzr values('{}',{},{},{},{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df_all)):
    L=[]
    for j in range(0,len(df_all.iloc[0])):
        L.append(df_all.iloc[i][j])
    cur0.execute(sql14.format(*L))
con0.commit()

cur0.close()
con0.close()

#非日报重点数据
#建立mring库连接
con0 = pymysql.connect(host='rm-2zej847swc7p120t0o.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w{6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库

### tab点击明细

sql0='SELECT MAX(day) FROM mring_tab_click'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,31)
sql33='''SELECT date,distinct_id,targetValue,COUNT(1) click_times,
COUNT(DISTINCT traceId) click_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND tag='screen_tab_click' 
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date,distinct_id,targetValue'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql34='''SELECT date,distinct_id,targetValue,COUNT(DISTINCT distinct_id) click_dev_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND tag='screen_tab_click' 
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date,distinct_id,targetValue'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql14='''insert into mring_tab_click values('{}','{}','{}','{}',{},{},{})'''
df33=get_sensor_info_mgo_group_sum(sql33,L=['date','city_sn','car_type','targetvalue'])
df34=get_sensor_info_mgo_group_sum(sql34,L=['date','city_sn','car_type','targetvalue'])
df35=df33.merge(df34,how='left',on=['date','city_sn','car_type','targetvalue'])
df35=df35[['date','city_sn','car_type','targetvalue','click_times','click_trip_cnt','click_dev_cnt']]

for i in range(0,len(df35)):
    L=[]
    for j in range(0,len(df35.iloc[0])):
        L.append(df35.iloc[i][j])
    cur0.execute(sql14.format(*L))
con0.commit()

##韩国艺匠&TT健身
## 去重行程数计算

sql0='SELECT MAX(day) FROM mring_ad_trip'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,10,17)
recent_day=date(2019,10,17)
sql1='''select date,count(distinct traceId) hgyj_trip_cnt from events where env='prod'
and traceId!=''
and (contentId in ('d4398ebad582e9ce7cc06c39f9fbcf17','e83611ec7a0556351e2e9548cc741d2d','7af074ceb499fb6dd2074e43d7ae5770','400d6d636f3b66cd2c47e75641756bf5',
'21bc96c9691539b5ea6fee66cf2919db','e1bf5dfe25705c78e62dcef9a63a83bd','234f734a4ad7b63a7e6a2d2e056862ff')
or (tag='screen_tab_click' and targetValue='play'))
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
group by date
order by date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql2='''select date,count(distinct traceId) ttjs_trip_cnt from events where env='prod'
and traceId!=''
and (contentId in ('5d14aee1e31f9afa98fc0cabde7b3dcf','d308656e6b8d07338f46735e1f520184','d1815729522b865d324b11c1696213cd','e18bec5d6b895f7f668716f56c88f488',
'1c89145b29e80e84aeb8876ea9c62c3c','0652c2d252db49bf0d6e536d769b9a8f','351c2f8ed5442ea0cff2de9e06362c9a')
or (tag='screen_tab_click' and targetValue='eat'))
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
group by date
order by date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql3='''insert into mring_ad_trip values('{}',{},{})'''
df1=get_sensor_info2(sql1)
df2=get_sensor_info2(sql2)
df=df1.merge(df2,on='date',how='left')
df=df[['date','hgyj_trip_cnt','ttjs_trip_cnt']]

for i in range(0,len(df)):
    L=[]
    for j in range(0,len(df.iloc[0])):
        L.append(df.iloc[i][j])
    cur0.execute(sql3.format(*L))
con0.commit()

### 交互数据

#当日交互行程内容数据
sql0='SELECT MAX(day) FROM mring_inter_day'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,31)
sql34='''SELECT date,COUNT(DISTINCT traceId) inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
 AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#当日内容交互
sql35='''SELECT a.date,a.daily_play_trip_content_cnt,b.inter_trip_content_cnt FROM 
(SELECT date,COUNT(DISTINCT CONCAT(traceId,contentId)) daily_play_trip_content_cnt
FROM events WHERE env='prod' AND traceId IS NOT NULL AND traceId!=''
AND tag='SCREEN_OTHER_CONTENT_BEGIN'
AND versioncode>=179
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
AND contentId IS NOT NULL
AND contentId!=''
AND (adSn='' OR adSn IS NULL)
GROUP BY date) a
LEFT JOIN 
(SELECT date,COUNT(DISTINCT CONCAT(traceId,contentId)) inter_trip_content_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND event IN ('CLICK','SLIDE')
AND contentId IS NOT NULL
 AND contentId!=''
AND (adSn='' OR adSn IS NULL)
AND tag NOT IN ('video_start','SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date) b ON a.date=b.date
ORDER BY a.date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list,
                          recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql36='''SELECT date,COUNT(DISTINCT traceId) close_trip_cnt FROM events 
WHERE env='prod' AND tag='screen_close'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date
ORDER BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql37='''SELECT date,COUNT(DISTINCT traceId) recommend_click_trip_cnt FROM events 
WHERE env='prod' AND tag='video_recommend_click'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date
ORDER BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql38='''SELECT date,COUNT(DISTINCT traceId) eat_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND page IN ('eat','3','page:main, subPage:eat')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql39='''SELECT date,COUNT(DISTINCT traceId) play_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND page IN ('play','4','page:main, subPage:play')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql40='''SELECT date,COUNT(DISTINCT traceId) video_trip_cnt FROM events 
WHERE 
env='prod'
AND traceId!=''
AND date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('targetPage: main ,targetSubPage: video','video')
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql41='''SELECT date,COUNT(DISTINCT traceId) play_trip_cnt FROM events 
WHERE 
env='prod'
AND traceId!=''
AND date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('targetPage: main ,targetSubPage: play','play')
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql42='''SELECT date,COUNT(DISTINCT traceId) eat_trip_cnt FROM events 
WHERE 
env='prod'
AND traceId!=''
AND date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('targetPage: main ,targetSubPage: eat','eat')
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql43='''SELECT date,COUNT(DISTINCT traceId) trip_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
 AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND page IN ('trip','1')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql44='''SELECT date,COUNT(DISTINCT traceId) video_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND page IN ('video','page:main, subPage:video','2')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql45='''SELECT date,COUNT(DISTINCT traceId) eat_effect_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click')
AND page IN ('eat','3','page:main, subPage:eat')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql46='''SELECT date,COUNT(DISTINCT traceId) play_effect_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click')
AND page IN ('play','4','page:main, subPage:play')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql47='''SELECT date,COUNT(DISTINCT traceId) video_effect_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click','SCREEN_CLICK_SCREEN')
AND page IN ('video','2','page:main, subPage:video')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
df34=get_sensor_info(sql34)
df35=get_sensor_info(sql35)
df36=get_sensor_info(sql36)
df37=get_sensor_info(sql37)
df38=get_sensor_info(sql38)
df39=get_sensor_info(sql39)
df40=get_sensor_info(sql40)
df41=get_sensor_info(sql41)
df42=get_sensor_info(sql42)
#df43=get_sensor_info(sql43)
df44=get_sensor_info(sql44)
df45=get_sensor_info(sql45)
df46=get_sensor_info(sql46)
df47=get_sensor_info(sql47)
df_all=df34.merge(df35,how='left',on='date').merge(df36,how='left',on='date').merge(df37,how='left',on='date').\
merge(df38,how='left',on='date').merge(df39,how='left',on='date').merge(df40,how='left',on='date').merge(df41,how='left',on='date').\
merge(df42,how='left',on='date').merge(df44,how='left',on='date').merge(df45,how='left',on='date').merge(df46,how='left',on='date').\
merge(df47,how='left',on='date')
df_all=df_all[['date','inter_trip_cnt','daily_play_trip_content_cnt','inter_trip_content_cnt','close_trip_cnt',
               'recommend_click_trip_cnt','eat_inter_trip_cnt','play_inter_trip_cnt','video_trip_cnt','eat_trip_cnt','play_trip_cnt',
               'video_inter_trip_cnt','eat_effect_inter_trip_cnt','play_effect_inter_trip_cnt','video_effect_inter_trip_cnt']]
sql102='''insert into mring_inter_day values('{}',{},{},{},{},{},{},{},{},{},{},0,{},{},{},{})'''
for i in range(0,len(df_all)):
    L=[]
    for j in range(0,len(df_all.iloc[0])):
        L.append(df_all.iloc[i][j])
    cur0.execute(sql102.format(*L))
con0.commit()

## 过去7天内容交互数据

#当日交互行程内容数据
sql0='DELETE FROM mring_content_inter'
cur0.execute(sql0)
sql='''SELECT a.contentId,MIN(a.play_trip_cnt) play_trip_cnt,MIN(b.total_inter_trip_cnt) total_inter_trip_cnt,
SUM(CASE WHEN c.tag IN ('SCREEN_CLICK_SCREEN','screen_click') THEN tag_inter_trip_cnt ELSE 0 END) screen_click_trip_cnt,
SUM(CASE WHEN c.tag='video_recommend_swith' THEN tag_inter_trip_cnt ELSE 0 END) recommend_swith_trip_cnt,
SUM(CASE WHEN c.tag='SCREEN_CLICK_SPEAKER_BUTTON' THEN tag_inter_trip_cnt ELSE 0 END) click_speaker_button_trip_cnt,
SUM(CASE WHEN c.tag='SCREEN_OTHER_VOLUME_CHANGE' THEN tag_inter_trip_cnt ELSE 0 END) volume_change_trip_cnt,
SUM(CASE WHEN c.tag='video_recommend_click' THEN tag_inter_trip_cnt ELSE 0 END) recommend_click_trip_cnt,
SUM(CASE WHEN c.tag='video_recommend_change' THEN tag_inter_trip_cnt ELSE 0 END) recommend_change_trip_cnt,
SUM(CASE WHEN c.tag='video_progress_change' THEN tag_inter_trip_cnt ELSE 0 END) progress_change_trip_cnt,
SUM(CASE WHEN c.tag='video_pause_click' THEN tag_inter_trip_cnt ELSE 0 END) pause_click_trip_cnt,
SUM(CASE WHEN c.tag='video_start_click' THEN tag_inter_trip_cnt ELSE 0 END) start_click_trip_cnt
FROM 
(SELECT contentId,COUNT(DISTINCT traceId) play_trip_cnt
FROM events WHERE env='prod' AND traceId IS NOT NULL AND traceId!=''
AND tag='SCREEN_OTHER_CONTENT_BEGIN'
-- AND versioncode>=179
AND date BETWEEN CURRENT_DATE()-INTERVAL '7' DAY AND CURRENT_DATE()-INTERVAL '1' DAY
AND contentId IS NOT NULL
AND contentId!=''
AND adSn=''
AND distinct_id NOT IN ({})
GROUP BY contentId) a
LEFT JOIN 
(SELECT contentId,COUNT(DISTINCT traceId) total_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE')
AND contentId IS NOT NULL
 AND contentId!=''
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND adSn=''
AND date BETWEEN CURRENT_DATE()-INTERVAL '7' DAY AND CURRENT_DATE()-INTERVAL '1' DAY
AND distinct_id NOT IN ({})
GROUP BY contentId) b ON a.contentId=b.contentId
LEFT JOIN 
(SELECT contentId,tag,COUNT(DISTINCT traceId) tag_inter_trip_cnt
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE')
AND contentId IS NOT NULL
AND contentId!=''
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND adSn=''
AND date BETWEEN CURRENT_DATE()-INTERVAL '7' DAY AND CURRENT_DATE()-INTERVAL '1' DAY
AND distinct_id NOT IN ({})
GROUP BY contentId,tag) c ON a.contentId=c.contentId
GROUP BY a.contentId'''
df=get_sensor_info2(sql.format(test_dev_list,test_dev_list,test_dev_list))
df=df[['contentid','play_trip_cnt','total_inter_trip_cnt','screen_click_trip_cnt','recommend_swith_trip_cnt',
'click_speaker_button_trip_cnt','volume_change_trip_cnt','recommend_click_trip_cnt','recommend_change_trip_cnt',
'progress_change_trip_cnt','pause_click_trip_cnt','start_click_trip_cnt']].fillna(0)

sql0='''insert into mring_content_inter values('{}',{},{},{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df)):
    L=[]
    for j in range(0,len(df.iloc[0])):
        L.append(df.iloc[i][j])
    cur0.execute(sql0.format(*L))
con0.commit()

### 广告播放策略监控

sql0='SELECT MAX(day) FROM mring_sensor_ad_monitor'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,10,9)
sql36='''SELECT day `date`,
CASE WHEN trace_time<3 THEN '<3'
WHEN trace_time>=3 AND trace_time<6 THEN '3-6'
WHEN trace_time>=6 AND trace_time<9 THEN '6-9'
WHEN trace_time>=9 AND trace_time<12 THEN '9-12'
WHEN trace_time>=12 AND trace_time<15 THEN '12-15'
WHEN trace_time>=15 AND trace_time<18 THEN '15-18'
WHEN trace_time>18 THEN '>18'
ELSE 'ELSE' END trace_time_type,
SUM(CASE WHEN IFNULL(kaiping,0)+IFNULL(tiepian,0)=0 THEN 1 ELSE 0 END) no_ads,
SUM(CASE WHEN IFNULL(kaiping,0)=0 AND IFNULL(tiepian,0)!=0 THEN 1 ELSE 0 END) zero_one,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND IFNULL(tiepian,0)=0 THEN 1 ELSE 0 END) one_zero,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian=1 THEN 1 ELSE 0 END) one_one,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian=2 THEN 1 ELSE 0 END) one_two,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian=3 THEN 1 ELSE 0 END) one_three,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian=4 THEN 1 ELSE 0 END) one_four,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian=5 THEN 1 ELSE 0 END) one_five,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian=6 THEN 1 ELSE 0 END) one_six,
SUM(CASE WHEN IFNULL(kaiping,0)!=0 AND tiepian>6 THEN 1 ELSE 0 END) one_morethan_six
FROM 
(SELECT traceId,FROM_UNIXTIME(CAST(MIN(shijian) AS INT),'yyyy-MM-dd') day,(MAX(shijian)-MIN(shijian))/60 trace_time
FROM
(SELECT traceId,tag,MIN(timestamp/1000) shijian
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND tag IN ('trip_start','trip_end')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY traceId,tag) a
GROUP BY traceId 
HAVING COUNT(1)=2 AND day BETWEEN '{}' AND '{}') A
LEFT JOIN 
(SELECT traceId,
SUM(CASE WHEN tag IN ('SCREEN_OTHER_RECOVER_END') AND message like '内容正常结束%' THEN 1 ELSE 0 END) tiepian,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) kaiping
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND adSn!=''
GROUP BY traceId) B ON A.traceId=B.traceId
GROUP BY day,trace_time_type
ORDER BY day DESC'''.format(recent_day,date.today()-timedelta(days=1),test_dev_list,
                            recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql14='''insert into mring_sensor_ad_monitor values('{}','{}',{},{},{},{},{},{},{},{},{},{})'''
df36=get_sensor_info(sql36)
df36=df36[['date','trace_time_type','no_ads','zero_one','one_zero','one_one',
           'one_two','one_three','one_four','one_five','one_six','one_morethan_six']]
for i in range(0,len(df36)):
    L=[]
    for j in range(0,len(df36.iloc[0])):
        L.append(df36.iloc[i][j])
    cur0.execute(sql14.format(*L))
con0.commit()

## 异常播放行程记录
### 异常播放行程概览
sql0='SELECT MAX(day) FROM mring_unusual_trip_summary'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date.today()-timedelta(days=2)
sql31='''SELECT A.day `date`,A.traceId,ROUND(A.trace_time,0) trace_time,B.kaiping,B.tiepian
FROM 
(SELECT traceId,FROM_UNIXTIME(CAST(MIN(shijian) AS INT),'yyyy-MM-dd') day,(MAX(shijian)-MIN(shijian))/60 trace_time
FROM
(SELECT traceId,tag,MIN(timestamp/1000) shijian
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND tag IN ('trip_start','trip_end')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY traceId,tag) a
GROUP BY traceId
HAVING COUNT(1)=2 AND day BETWEEN '{}' AND '{}') A
LEFT JOIN 
(SELECT traceId,
SUM(CASE WHEN tag IN ('SCREEN_OTHER_RECOVER_END') AND message like '内容正常结束%' THEN 1 ELSE 0 END) tiepian,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) kaiping
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
AND adSn!=''
GROUP BY traceId) B ON A.traceId=B.traceId
WHERE kaiping!=1 
OR (tiepian>=trace_time/5*2)'''.format(recent_day,date.today()-timedelta(days=1),test_dev_list,
                                       recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                       recent_day,date.today()-timedelta(days=1),test_dev_list)
sql14='''insert into mring_unusual_trip_summary values('{}','{}',{},{},{})'''
df31=get_sensor_info(sql31)
df31=df31[['date','traceid','trace_time','kaiping','tiepian']]
for i in range(0,len(df31)):
    L=[]
    for j in range(0,len(df31.iloc[0])):
        L.append(df31.iloc[i][j])
    cur0.execute(sql14.format(*L))
    if i/100==0:
        con0.commit()
    else: continue
con0.commit()


### 异常播放行程播放明细
sql0='SELECT MAX(day) FROM mring_unusual_trip_detail'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date.today()-timedelta(days=2)
sql32='''SELECT traceId,tag,message,date,time,FROM_UNIXTIME(CAST(timestamp/1000 AS INT)) `timestamp` FROM events 
WHERE 
env='prod'
AND versioncode>=179
AND (tag IN ('splash_show_begin','splash_show_end','trip_start','trip_end')
OR (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','SCREEN_OTHER_RECOVER_END')
AND adSn!=''))
AND traceId IN 
(SELECT a.traceId
FROM 
(SELECT traceId,FROM_UNIXTIME(CAST(MIN(shijian) AS INT),'yyyy-MM-dd') day,(MAX(shijian)-MIN(shijian))/60 trace_time
FROM
(SELECT traceId,tag,MIN(timestamp/1000) shijian
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND tag IN ('trip_start','trip_end')
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY traceId,tag) a
GROUP BY traceId
HAVING COUNT(1)=2 AND day BETWEEN '{}' AND '{}') A
LEFT JOIN 
(SELECT traceId,
SUM(CASE WHEN tag IN ('SCREEN_OTHER_RECOVER_END') AND message like '内容正常结束%' THEN 1 ELSE 0 END) tiepian,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) kaiping
FROM events 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND adSn!=''
AND date BETWEEN '{}' AND '{}'
AND distinct_id NOT IN ({})
GROUP BY traceId) B ON A.traceId=B.traceId
WHERE kaiping!=1 
OR (tiepian>=trace_time/5*2))'''.format(recent_day,date.today()-timedelta(days=1),test_dev_list,
                                        recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                       recent_day,date.today()-timedelta(days=1),test_dev_list)
sql15='''insert into mring_unusual_trip_detail values('{}','{}','{}','{}','{}','{}')'''
df32=get_sensor_info(sql32)
df32=df32[['traceid','tag','message','date','time','timestamp']]
for i in range(0,len(df32)):
    L=[]
    for j in range(0,len(df32.iloc[0])):
        L.append(df32.iloc[i][j])
    cur0.execute(sql15.format(*L))
    if i/100==0:
        con0.commit()
    else:continue
con0.commit()

cur0.close()
con0.close()