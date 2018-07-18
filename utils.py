import re
import pandas as pd
import numpy as np
from collections import Counter
from collections import defaultdict
from dateutil.parser import parse
# 获取异常值索引
def detect_outliers(df,n,features):
    outlier_indices = []
    for col in features:
        Q1 = np.percentile(df[col],15)
        Q3 = np.percentile(df[col],85)
        IQR = Q3 - Q1
        outlier_step = 1.5 * IQR
        outlier_list_col = df[(df[col] < Q1 - outlier_step) | (df[col] > Q3 + outlier_step )].index
        outlier_indices.extend(outlier_list_col)

    outlier_indices = Counter(outlier_indices)        
    multiple_outliers = list( k for k, v in outlier_indices.items() if v > n )
    return multiple_outliers

# 替换
def sub_content(pattern,reply_content):
    return re.sub(pattern,'',reply_content,re.S)

# 获取价格
def get_price(pattern,text):
    price = re.search(pattern,text,re.S).group(1)
    return price

# 获取初步数据
def get_pri_data(price_list):
    datas = pd.DataFrame(price_list,columns=['price','time'])  
    datas['price'] = datas['price'].astype('int')
    return datas

# 获取最终数据
def get_final_data(datas,Outliers_to_drop,price_list):
    # 删除异常值
    new_datas = datas.drop(Outliers_to_drop, axis = 0).reset_index(drop=True)
    new_datas = new_datas.sort_values(by='time')
    a = new_datas['price'].tolist()
    b = new_datas['time'].tolist()
    #price_info = [{'prcie':i[0],'time':i[1]} for i in zip(a,b)]
    final_data = defaultdict(list)
    for v,k in zip(a,b):
        final_data[k].append(v)
    price_num = [len(final_data[i]) for i in final_data]   # 价格出现频次
    final_data={i:int(sum(final_data[i])/len(final_data[i])) for i in final_data}
    final_data['price_num'] = price_num
    return final_data

