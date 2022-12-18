# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 18:53:35 2021

@author: chong.xu6138
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 10:51:31 2021

@author: chong.xu6138
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from difflib import get_close_matches
from difflib import SequenceMatcher
hipac_shop_df=pd.read_excel('Hipac所有门店地址.xlsx')
new_shop_df=pd.read_excel('品牌ka门店清单.xlsx',sheet_name='汇总')
total_shop_name_list=hipac_shop_df.shop_name.to_list()
total_address_name_list=hipac_shop_df.total_address.to_list()
new_shop_df['matched_shop_name']='无'
new_shop_df['matched_shop_name_score']=0.00
new_shop_df['matched_address_name']='无'
new_shop_df['matched_address_name_score']=0.00
L1=[]
L2=[]
L3=[]
L4=[]
print("开始运行详细地址校验"+datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))
for i in range(0,25000):
    try: 
        s=get_close_matches(new_shop_df.详细地址[i], total_address_name_list, n=1, cutoff=0.7)[0]
        L3.append(s)
        L4.append(SequenceMatcher(None, new_shop_df.详细地址[i], s).ratio())
    except:
        L3.append('无')
        L4.append(0.00)
    if (i+1)%5000==0:
        print("结束第"+str(i)+"行"+datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))
new_shop_df.loc[0:len(L3)-1,'matched_address_name']=L3
new_shop_df.loc[0:len(L4)-1,'matched_address_name_score']=L4
print("结束详细地址校验"+datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))
new_shop_df.to_excel('new_shop_address1.xlsx')
