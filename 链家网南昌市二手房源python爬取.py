import requests
from bs4 import BeautifulSoup
import random
import time
import csv
import os
from concurrent.futures import ThreadPoolExecutor

#词云相关模块
import jieba
import pandas as pd
from collections import Counter
from wordcloud import WordCloud

def interval(url):
    num = url.split('pg')[1].split('/')[0]
    intervals = random.randint(2, 5)  # [2,5]秒之间的随机时间间隔来发送请求
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] 已爬取第{num}页")
    time.sleep(intervals)


def write_to_csv(data, header=None):
    file_exists = os.path.isfile('house_info.csv')
    with open('house_info.csv', mode='a', newline='',encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        header = ['标题', '小区名称', '户型','面积','朝向','装修','楼层','类型', '关注人数', '发布时间', '看房方式', '总价','单价']
        if not file_exists and header is not None:
            writer.writerow(header)
        writer.writerow(data)

#单面网页数据据的爬取
def fun(url):
    try:
        agent_list = [
        {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
        {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
        {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;'},
        {'User-Agent':'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)'},
        {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        ]

        # proxy = {'https':'114.231.42.231'}  代理ip池
        resp = requests.get(url,headers=random.choice(agent_list))
        resp.encoding ='utf-8'
        response = resp.text
        soup = BeautifulSoup(response,'html.parser')
        div_list = soup.find_all('div',attrs={'class':'info clear'})

        for div in div_list:
            #获取div标签里的每一个房源数据的title标题简介
            title = div.find('div',attrs={'class':'title'}).find('a').get_text()
            #将获取到的每一个flood元素标签里的所有a标签的文本合并在一起
            flood_list = div.find('div',attrs={'class':'flood'}).find('div').find_all('a')
            position_info = flood_list[0].get_text() + '-' + flood_list[1].get_text()

            #将house_infp标签元素里的关于户型,面积,朝向,装修,楼层,类型的信息提取出来
            house_info = div.find('div', attrs={'class': 'address'}).find('div').get_text()
            room = house_info.split('|')[0]
            square = house_info.split('|')[1]
            direction = house_info.split('|')[2]
            decorate = house_info.split('|')[3]
            floor = house_info.split('|')[4]
            house_type = house_info.split('|')[5]

            #将获取到的follow_info关注人数和发布时间分离
            follow_info = div.find('div', attrs={'class': 'followInfo'}).get_text()
            follow = follow_info.split('/')[0]
            release_time = follow_info.split('/')[1]

            tag = div.find('div', attrs={'class': 'tag'}).get_text()
            #获取房源---二手房的价格与单价
            price = div.find('div', attrs={'class': 'priceInfo'}).find('div',attrs={'class':'totalPrice totalPrice2'}).get_text()
            unit_price  = div.find('div', attrs={'class': 'priceInfo'}).find('div',attrs={'class':'unitPrice'}).get_text()
            list_1 = []
            list_1.extend([title,position_info,room,square,direction,decorate,floor,house_type,follow,release_time,tag,price,unit_price])
            write_to_csv(list_1)
    except requests.exceptions.RequestException as e:
        # 发生异常或无响应数据时等待一段时间后重新尝试发送请求
        print(f"请求数据出现异常：{e}")
        time.sleep(3)
        return fun(url)

    except Exception as e:
        # 处理其他异常
        print(f"发生了一些其他的异常：{e}")

#定义一个生成csv文件“标题”列的词云图函数
def title_picture():
    data = pd.read_csv('house_info.csv')
    text = data["标题"].to_string(index=False)

    words = jieba.cut(text)
    print(words)
    text_words =[]
    for j in words:
        if len(j)>=2:
            text_words.append(j)
    result = Counter(text_words).most_common(75)
    word_list = []
    #将元组列表里的第一个关键词提取出来
    for i,j in result:
        word_list.append(i)
    print(result)

    content = ' '.join(word_list)

    wc = WordCloud(width=500, height=500, background_color='white',font_path="simhei.ttf")
    # 生成词云图
    wc.generate(content)
    wc.to_file('ciyun.png')


def main(url):
    fun(url)
    interval(url)



if __name__ == '__main__':
        url_list = []
        # for i in range(101):从网页中查看页数最多100页，每页30条数据，最多可获取3000条
        for num in range(1,101):
            url_list.append(f'https://nc.lianjia.com/ershoufang/pg{num}/')
        #运用线程池创建多线程执行爬取任务，提高爬取效率
        with ThreadPoolExecutor(10) as t:
            for url in url_list:
                t.submit(main,url)
        print('已下载完成')
        #调用生成词云函数title_picture()将生成由“标题”列组成的词云图
        # title_picture()





