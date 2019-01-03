import pymongo
import re
import pandas as pd
import numpy as np
from collections import Counter
from utils import detect_outliers,sub_content,get_price,get_final_data,get_pri_data

client = pymongo.MongoClient(host='localhost',port=27017)
collection = client['jw3_price']['jw3_price']

pattern_buy = r'(\d+).*?收'
pattern_sale = r'(\d+).*?出'
pattern = '<.*?>'
sale_price_list = []
buy_price_list = []
reply_time_list = []

for post in collection.find():
    if '出' in post['reply_content']:
        try:
            text = sub_content(pattern,post['reply_content'])
            price = get_price(pattern_sale,text)
            sale_price_list.append({'price':price,'time':post['reply_time'].split(' ')[0]})
        except Exception as e:
            pass 
    if '收' in post['reply_content']:
        try:
            text = sub_content(pattern,post['reply_content'])            
            price = get_price(pattern_buy,text)
            buy_price_list.append({'price':price,'time':post['reply_time'].split(' ')[0]})
        except Exception as e:
            pass

pri_buy_datas = get_pri_data(buy_price_list)
Outliers_to_drop = detect_outliers(pri_buy_datas,0,['price'])
final_buy_data = get_final_data(pri_buy_datas,Outliers_to_drop,buy_price_list)


pri_sale_datas = get_pri_data(sale_price_list)
Outliers_to_drop = detect_outliers(pri_sale_datas,0,['price'])
final_sale_data = get_final_data(pri_sale_datas,Outliers_to_drop,buy_price_list)
