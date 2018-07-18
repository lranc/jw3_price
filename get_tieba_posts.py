import pymongo
import requests
import re
import json
from lxml import etree
from multiprocessing.pool import Pool
# 采集数据

def insert_db(tieba_post):
    client = pymongo.MongoClient(host='localhost',port=27017)
    db = client['jw3_price']
    collection=db['jw3_price']
    collection.insert(tieba_post)

def get_tieba_page_tuple(url,headers):
    s = requests.get(url)
    html = etree.HTML(s.content)
    pages_num = html.xpath('.//ul[@class="l_posts_num"]/li[2]/span[2]/text()')[0]
    page_tuple =([x for x in range(1,int(pages_num))])
    return page_tuple

def get_page_post(page,headers):
    url = 'https://tieba.baidu.com/p/5775783468'
    url_detail = url + '?pn=' + str(page)
    s = requests.get(url_detail,headers=headers)
    html = etree.HTML(s.content)
    post_bodys = html.xpath('.//div[@id="j_p_postlist"]/div[@class="l_post l_post_bright j_l_post clearfix  "]')
    try:
        for post in post_bodys:
            tieba_post={}
            data_dict = json.loads(post.xpath('@data-field')[0])
            tieba_post['reply_content'] = data_dict['content']['content']
            tieba_post['url'] = url_detail
            floor_num = data_dict['content']['post_no']
            tieba_post['reply_time'] = post.xpath('.//div[@class="post-tail-wrap"]/span[last()]/text()')[0]
            yield tieba_post
    except Exception as e:
        print(e)
        print(floor_num)
        print(url_detail)
        print(tieba_post)

def main(page):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',}
    posts = get_page_post(page,headers)
    for i in posts:
        insert_db(i)


if __name__ == '__main__':
    url = 'https://tieba.baidu.com/p/5775783468'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',}
    page_tuple = get_tieba_page_tuple(url,headers)
    pool=Pool()
    pool.map(main,page_tuple)
    pool.close()
    pool.join()