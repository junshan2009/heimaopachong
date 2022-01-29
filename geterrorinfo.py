import os
import codecs
import requests
import urllib3
from bs4 import BeautifulSoup
import utils

urllib3.disable_warnings()

errorinfile = 'data/error1.txt'  # 要抓的错误id位置
erroroutfile = 'data/error2.txt'  # 再次出错id保存位置

BASE_DIR = os.path.dirname(__file__)
FLODER_PATH = os.path.join(BASE_DIR, 'data')
OUT_FILE = FLODER_PATH + "/" + 'errhtml.txt'
headers = utils.getheaders()
outfil_object = codecs.open(OUT_FILE, 'w', 'utf-8')

sql3 = 'insert into tousu_sina(`title投诉标题`,`sn投诉编号`,`cotitle投诉对象`,`reason投诉问题`,`appeal投诉要求`,`request_repay_fee涉诉金额（元）`,`status投诉进度`,`attitude服务态度评分`,`process处理速度评分`,`satisfaction满意度评分`,`evalContent处理后用户的评论`,`appeal_date投诉产生时间`,`crawler_time爬取时间`) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
sql4 = 'insert into tousu_steplist(`sn投诉编号`,`operator该流程操作人`,`status该流程的当前状态`,`operate_content该流程的操作内容`,`operate_time该流程的时间节点`,`提取时间`,`小时`) values(%s,%s,%s,%s,%s,%s,%s)'
db2 = utils.sqlct()
cur2 = db2.cursor()

with open(errorinfile, 'r', encoding='utf8') as f:
    errfile = open(erroroutfile, 'w', encoding='utf-16')  # 初始化error
    errfile.close()
    list1 = f.readlines()
    # 去重id，并排序
    list2 = list(set(list1))
    list2.sort(key=list1.index)
    countjs = 0
    for i in list2:
        # time.sleep(0.5)
        try:
            i = i.strip()
            countjs = countjs + 1
            url = 'https://tousu.sina.com.cn/complaint/view/' + str(i) + '//'
            print(str(countjs), url)
            response = requests.get(url, headers=headers, timeout=30, verify=False)
            html_doc = response.text
            soup = BeautifulSoup(html_doc, "html.parser")
            tsdl = soup.find("div", attrs={"class": "ts-d-l"})
            outfil_object.write(str(tsdl) + '\n')
            outfil_object.flush()
        except Exception as e:
            print(e)
            with open(erroroutfile, 'a+', encoding='utf8') as f:
                f.write(i)
                f.write('\n')
            continue

INT_FILE = OUT_FILE  # html信息
intfil_object = codecs.open(INT_FILE, 'rb', 'utf-8')
intfil_lines = intfil_object.read()

soup = BeautifulSoup(intfil_lines, "html.parser")
tsdls = soup.find_all("div", attrs={"class": "ts-d-l"})

utils.getinfo(tsdls, sql3)  # 解析投诉单信息
utils.getstep(tsdls, sql4)  # 解析投诉流程信息

intfil_object.close()

utils.dbclose(db2)
