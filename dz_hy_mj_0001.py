####            0. 导入包
import MySQLdb
import numpy as np
import numpy
import pandas as pd
import pandas
import re
import datetime
import time
import os
from pandas import Series,DataFrame
import matplotlib
from matplotlib import pyplot as plt
from pandas import read_csv
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from pandas.tools.plotting import scatter_matrix
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn import neighbors
from sklearn.model_selection import cross_val_score
from sklearn.naive_bayes import BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import cross_val_score
import urllib.request
import urllib.parse
import urllib.error
from pprint import pprint
import http.cookiejar
import requests
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
import seaborn as sns
import hashlib
from selenium import webdriver
from sqlalchemy import create_engine
from importlib import reload
import math
import random
import tushare as ts
import functools
import operator
from lxml import etree

os.getcwd()
os.chdir(r'C:/PY/nail')

connection = MySQLdb.connect(
    host='rm-uf675p1vvls0t85vko.mysql.rds.aliyuncs.com',
    user='root',
    passwd='Tt65212879',
    port=3306,
    db='meijia_dazhong',
    charset='utf8'
);

df10000 = pandas.read_sql(
    """
        SELECT * FROM kb_shanghai;
    """, 
    con=connection
);

#df10 = pd.read_pickle(r'C:\PY\nail\pickle\kb2808.pickle')
df10=df10000.iloc[3000:4000,1:100]
#url_bases=list([s+r'/review_more?pageno=1' for s in df10.店铺link])
url_bases=list(df10.网友个人链接)
#url_base=url_bases[0]
#url_base='http://www.dianping.com/member/10025'

b_urls=[]
b_nicknames=[]
b_infos=[]
url_error=[]

time_start = time.clock()    
for url_base in url_bases:
    headers={
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
    'referer':'https://login.tmall.com/?spm=875.7931836/B.a2226mz.1.425f76a4kEw4ev&redirectURL=https%3A%2F%2Fwww.tmall.com%2F%3Fspm%3Da220o.1000855.a2226mz.1.70a352e4aDB1IB',
    'cookie':r'_hc.v=56504369-216a-3412-3fc6-735606b91168.1510413851; _lxsdk_cuid=15fabaf5ca0c8-0b3ec1ec68b716-464a0129-100200-15fabaf5ca1c8; _lxsdk=15fabaf5ca0c8-0b3ec1ec68b716-464a0129-100200-15fabaf5ca1c8; dper=ced4b08e4e32a28204842a53fb1e8944efd80e5cf6107eff43e74f0346cd7d52; ua=%E5%A4%A9%E4%B8%80%E6%AC%A1; ctu=fa83ae574c84f1075e7b1660468b81ac8f9cef852c8fe7ab43c980c4fb33102c; uamo=13916380623; cy=1; cye=shanghai; ll=7fd06e815b796be3df069dec7836c3df; s_ViewType=10; _lxsdk_s=15fbd1f60ce-539-c11-2e0%7C%7C120; JSESSIONID=2E4591AABA1CD8AD629E23C65FA66AF2; aburl=1; cy=1; cye=shanghai'
    }
    content=requests.get(url_base,headers=headers).text
    r=requests.get(url_base,headers=headers)

    try:
        info = re.findall(r'<div class="user-message Hide" id="J_UMoreInfoD">(.*?)<a class="more-info" href="javascript:;" id="J_UMoreInfo"', content,re.S)
        info == None
    except Exception:
        url_error.append(url_base)
        print('403被拒绝')        

    nickname=re.findall(r'<title>(.*?)的主页-会员-大众点评网</title>',content)
    info = re.findall(r'<div class="user-message Hide" id="J_UMoreInfoD">(.*?)<a class="more-info" href="javascript:;" id="J_UMoreInfo"', content,re.S) 

    b_urls.append(url_base)
    b_nicknames.append(nickname)
    b_infos.append(info)
    time.sleep(3)
time_end = time.clock()
time_count=str(time_end-time_start)

#print(b_infos[0])
#####    
url=pd.DataFrame(b_urls)
nickname=pd.DataFrame(b_nicknames)
info=pd.DataFrame(b_infos)

df=pd.concat([url,nickname,info],axis=1,ignore_index=True)
df.columns=['url_hy','nickname','info']

#       导出至mysql
yconnect = create_engine('mysql+mysqldb://root:T0000009@rm-uf675p1vvls0t85vko.mysql.rds.aliyuncs.com:3306/dzdp?charset=utf8')
pd.io.sql.to_sql(df,'dz_meijia_hy_sh', yconnect, schema='dzdp', if_exists='append',index=False)

#       导出单个pickle临时留档   
#df.to_pickle('id'+df.itemid[0]+'_'+contentcount+'.pickle')
#picklepath='C:\\PY\\sunstar\\pickle\\'+'id'+df.itemid[0]+'_'+contentcount+'.pickle'
picklepath=r'C:\PY\nail\pickle\kb_2600-2700.pickle'
df.to_pickle(picklepath)

#去重
df.drop_duplicates(inplace=True)












