# -*- coding=utf-8 -*-
import os
import pymysql
import time
import re
import requests
from selenium import webdriver
import json
import codecs
from bs4 import BeautifulSoup


# 解析方法
def str_fs_db_b(str):
    return str.replace('\t', '').replace('\n', '').replace('\r', '').replace("'", "’").replace(',', '，').strip()


# 返回头部
def getheaders():
    headers = {
        'Host': 'tousu.sina.com.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cookie': 'U_TRS1=000000e5.51045ee2.5a3cb848.38ccd7c7; SINAGLOBAL=39.155.160.229_1513928776.521283; TOUSU-SINA-CN=; UOR=,tousu.sina.com.cn,; ULV=1593567584011:2:2:2:112.97.247.25_1593567400.196344:1593567398057; Apache=112.97.247.25_1593567400.196344; UM_distinctid=1730804cc9914e-0852b772886078-15387640-100200-1730804cc9b183; CNZZDATA1273941306=2113887062-1593564151-%7C1593564151',
        'Upgrade-Insecure-Requests': '1',
        'TE': 'Trailers'
    }
    return headers


# 返回当前工作路径
def getworkpath():
    BASE_DIR = os.path.dirname(__file__)
    FLODER_PATH = os.path.join(BASE_DIR, 'data')
    return FLODER_PATH


# 数据库建库对象
def sqlcd():
    # 运行前修改相关信息
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'port': 3306,
        'charset': 'utf8',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    db1 = pymysql.connect(**config)
    return db1


# 数据库建表对象
def sqlct():
    # 运行前修改相关信息
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'db': 'heimaodata',
        'port': 3306,
        'charset': 'utf8',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    db2 = pymysql.connect(**config)
    return db2


# 提交投诉单信息
def commitinfo(sql,
               bt, sn, duixiang, question, appeal, repay, jindu, attitude, process, manyi, evacont, tousutime, dats):
    '''

    :param sql: 插入语句
    :param bt: 投诉标题
    :param sn: 投诉编号
    :param duixiang: 投诉对象
    :param question: 投诉问题
    :param appeal: 投诉要求
    :param repay: 涉诉金额
    :param jindu: 投诉进度
    :param attitude: 服务态度评分
    :param process: 处理速度评分
    :param manyi: 满意度评分
    :param evacont: 处理后用户评价
    :param tousutime: 投诉产生时间
    :param dats: 爬取时间
    :return:
    '''
    db = sqlct()
    with db.cursor() as cursor:
        # cursor = db.cursor()
        cursor.execute(sql, (
            bt, sn, duixiang, question, appeal, repay, jindu, attitude, process, manyi, evacont, tousutime, dats))
        db.commit()


# 提交投诉流程信息
def commitlist(sql, tousubh, name, status, content, udate, timee, hour):
    '''

    :param sql: 插入语句
    :param tousubh: 投诉编号
    :param name: 该流程操作人
    :param status: 该流程当前状态
    :param content: 该流程操作内容
    :param udate: 该流程时间节点
    :param timee: 提取时间
    :param hour: 小时
    :return:
    '''
    db = sqlct()
    with db.cursor() as cursor:
        # cursor = db.cursor()
        cursor.execute(sql, (tousubh, name, status, content, udate, timee, hour))
        db.commit()


# 关闭数据库
def dbclose(cursor):
    cursor.close()


# 解析抓包信息得到id
def getsnid(filepath, idfilepath):
    '''

    :param filepath: 抓包信息文件位置
    :param idfilepath: 保存id文件位置
    :return:
    '''
    outfile = open(idfilepath, 'w', encoding='utf-16')  # 初始化
    outfile.close()
    with open(filepath, 'r', encoding='utf16') as f:
        list = f.readlines()
        for i in list:
            js = re.sub(r'Response body: try{jQuery\d+_\d+\(', '', i).replace(');}catch(e){};', '').strip()
            rt = json.loads(js)
            datas = rt['result']['data']['complaints']
            # print(datas)
            for j in datas:
                conid = j['main']['sn']

                with open(idfilepath, 'a+', encoding='utf8') as f:
                    f.write(str(conid) + '\n')

#6553436127
# 模拟滑动
def selpath(filepath, i,conid):
    '''
    :param filepath: 抓包信息保存位置
    :param i: 滑动次数
    :return:
    '''
    # 初始化抓包抓到的信息
    outfile = open(filepath, 'w', encoding='utf-16')  # 文件目录为fiddler中设置的目录
    outfile.close()

    # 进入浏览器设置
    options = webdriver.ChromeOptions()
    # 设置静默模式，不用打开浏览器
    options.add_argument('headless')
    # 更换头部
    options.add_argument(
        'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"')

    #conid = [5501611052, 6756505772, 5228185692, 2365681750, 2146783270, 3201585302, 1996777023, 3614813151]
    # conid = [5501611052] # 中国邮政
    # conid = [6756505772,5228185692] # 京东物流 顺丰
    # conid = [2365681750,2146783270] # 中通 圆通
    # conid = [3201585302, 1996777023] # 申通 百世快递
    # conid = [3614813151] # 韵达
    #chrome_driver = 'C:\Program Files(x86)\Google\Chrome\Application\chromedriver.exe'  # 不配置环境变量需指定chromedriver的文件位置
    # for id in conid:
    #browser = webdriver.Chrome(executable_path=chrome_driver,options=options) #不配置环境变量写法
    browser = webdriver.Chrome(options=options)
    if str(conid) == '5501611052':
        CSTR = '中国邮政'
    elif str(conid) == '6756505772':
        CSTR = '京东物流'
    elif str(conid) == '5228185692':
        CSTR = '顺丰'
    elif str(conid) == '2365681750':
        CSTR = '中通'
    elif str(conid) == '2146783270':
        CSTR = '圆通'
    elif str(conid) == '3201585302':
       CSTR = '申通'
    elif str(conid) == '1996777023':
       CSTR = '百世快递'
    elif str(conid) == '3614813151':
       CSTR = '韵达'
    count = 1
    a = 20000
    try:
        url = 'https://tousu.sina.com.cn/company/view/?couid=' + str(conid)
        browser.get(url)
        while True:
            if count > i:  # 下滑次数
                break
            if count % 10 == 0  or count == 1:
                print(CSTR + ':第' + str(count) + '页')
            a += 20000
            js1 = "document.documentElement.scrollTop={}".format(a)
            browser.execute_script(js1)
            time.sleep(5)
            count += 1
    except Exception as e:
        print(e)

    finally:
        print('--------------------------')
        browser.close()


# 拼接投诉链接并请求
def reqinfo(idfile, errorfile):
    '''

    :param idfile: 投诉编号文件路径
    :param errorfile: 出错编号保存路径
    :return: tsdls beautifulsoup 对象
    '''
    BASE_DIR = os.path.dirname(__file__)
    FLODER_PATH = os.path.join(BASE_DIR, 'data')
    OUT_FILE = FLODER_PATH + '/' + 'html.txt'
    outfil_object = codecs.open(OUT_FILE, 'w', 'utf-8')
    #print('OUT_FILE',OUT_FILE)
    #print('outfil_object', outfil_object)
    headers = getheaders()

    with open(idfile, 'r', encoding='utf8') as f:
        errfile = open('data/error1.txt', 'w', encoding='utf-16')  # 初始化error
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
                if countjs % 50 == 0 or countjs == 1:
                    print(str(countjs), url)
                response = requests.get(url, headers=headers, timeout=30,proxies = { "http": None, "https": None}, verify=False)
                html_doc = response.text
                soup = BeautifulSoup(html_doc, "html.parser")
                tsdl = soup.find("div", attrs={"class": "ts-d-l"})
                outfil_object.write(str(tsdl) + '\n')
                outfil_object.flush()
            except Exception as e:
                print(e)
                with open(errorfile, 'a+', encoding='utf8') as f:
                    f.write(i)
                    f.write('\n')
                continue
    outfil_object.close()

    INT_FILE = OUT_FILE  # html信息
    intfil_object = codecs.open(INT_FILE, 'rb', 'utf-8')
    intfil_lines = intfil_object.read()

    soup = BeautifulSoup(intfil_lines, "html.parser")
    tsdls = soup.find_all("div", attrs={"class": "ts-d-l"})
    #print('1',tsdls)

    intfil_object.close()
    return tsdls


# 解析html获得投诉单信息
def getinfo(tsdls, sql):
    '''

    :param tsdls: beautifulsoup 对象
    :param sql: 插入语句
    :return:
    '''
    for tsdl in tsdls:

        outstr = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

        dats = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))  # 爬取时间

        outstr_tmp = tsdl.find("h1", attrs={"class": "article"}).text
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        bt = str_fs_db_b(str(outstr_tmp))  # 投诉标题

        tsqlist = tsdl.find("ul", attrs={"class": "ts-q-list"}).find_all("li")

        tmpstr = tsqlist[0].text
        outstr_tmp = ''
        if tmpstr.find('投诉编号：') >= 0:
            outstr_tmp = tmpstr.replace('投诉编号：', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        sn = str_fs_db_b(str(outstr_tmp))  # 投诉编号

        tmpstr = tsqlist[1].text
        outstr_tmp = ''
        if tmpstr.find('投诉对象：') >= 0:
            outstr_tmp = tmpstr.replace('投诉对象：', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        duixiang = str_fs_db_b(str(outstr_tmp))  # 投诉对象

        tmpstr = tsqlist[2].text
        outstr_tmp = ''
        if tmpstr.find('投诉问题：') >= 0:
            outstr_tmp = tmpstr.replace('投诉问题：', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        question = str_fs_db_b(str(outstr_tmp))  # 投诉问题

        tmpstr = tsqlist[3].text
        outstr_tmp = ''
        if tmpstr.find('投诉要求：') >= 0:
            outstr_tmp = tmpstr.replace('投诉要求：', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        appeal = str_fs_db_b(str(outstr_tmp))  # 投诉要求

        tmpstr = tsqlist[4].text
        outstr_tmp = ''
        if tmpstr.find('涉诉金额：') >= 0:
            outstr_tmp = tmpstr.replace('涉诉金额：', '').replace('元', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        repay = str_fs_db_b(str(outstr_tmp))  # 涉诉金额

        tmpstr = tsqlist[5].text
        outstr_tmp = ''
        if tmpstr.find('投诉进度：') >= 0:
            outstr_tmp = tmpstr.replace('投诉进度：', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        jindu = str_fs_db_b(str(outstr_tmp))  # 投诉进度

        tsdpj = tsdl.find("div", attrs={"class": "ts-d-pj"})
        if tsdpj != None:

            tsdpjs = tsdpj.find_all("li")

            tstr = tsdpjs[0]
            tmpstr = tstr.text
            tstr = str(tstr)
            outstr_tmp = ''
            if tmpstr.find('服务态度：') >= 0:
                if tstr.find('star5') >= 0:
                    outstr_tmp = '5'
                elif tstr.find('star4') >= 0:
                    outstr_tmp = '4'
                elif tstr.find('star3') >= 0:
                    outstr_tmp = '3'
                elif tstr.find('star2') >= 0:
                    outstr_tmp = '2'
                elif tstr.find('star1') >= 0:
                    outstr_tmp = '1'
                else:
                    outstr_tmp = '0'
            outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

            attitude = str_fs_db_b(str(outstr_tmp))

            tstr = tsdpjs[1]
            tmpstr = tstr.text
            tstr = str(tstr)
            outstr_tmp = ''
            if tmpstr.find('处理速度：') >= 0:
                if tstr.find('star5') >= 0:
                    outstr_tmp = '5'
                elif tstr.find('star4') >= 0:
                    outstr_tmp = '4'
                elif tstr.find('star3') >= 0:
                    outstr_tmp = '3'
                elif tstr.find('star2') >= 0:
                    outstr_tmp = '2'
                elif tstr.find('star1') >= 0:
                    outstr_tmp = '1'
                else:
                    outstr_tmp = '0'
            outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

            process = str_fs_db_b(str(outstr_tmp))

            tstr = tsdpjs[2]
            tmpstr = tstr.text
            tstr = str(tstr)
            outstr_tmp = ''
            if tmpstr.find('满意度：') >= 0:
                if tstr.find('star5') >= 0:
                    outstr_tmp = '5'
                elif tstr.find('star4') >= 0:
                    outstr_tmp = '4'
                elif tstr.find('star3') >= 0:
                    outstr_tmp = '3'
                elif tstr.find('star2') >= 0:
                    outstr_tmp = '2'
                elif tstr.find('star1') >= 0:
                    outstr_tmp = '1'
                else:
                    outstr_tmp = '0'
            outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

            manyi = str_fs_db_b(str(outstr_tmp))

            tmpstr = tsdpj.text
            outstr_tmp = ''
            outstr_tmp = tmpstr.replace('服务态度：', '').replace('处理速度：', '').replace('满意度：', '')
            outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

            evacont = str_fs_db_b(str(outstr_tmp))


        else:

            outstr = outstr + ',,,,'

            attitude = ''  # 服务态度
            process = ''  # 处理速度
            manyi = ''  # 满意度
            evacont = ''  # 投诉后评价

        outstr_tmp = tsdl.find("div", attrs={"class": "ts-d-question"}).find("span",
                                                                             attrs={"class": "u-date"}).text
        outstr_tmp = outstr_tmp.replace('发布于 ', '').replace('年', '/').replace('月', '/').replace('日', '')
        outstr = outstr + ',' + str_fs_db_b(str(outstr_tmp))

        tousutime = str_fs_db_b(str(outstr_tmp))  # 投诉时间

        #print('3',outstr)
        #print(tousutime)
        #print('------------------------------------------------------')

        commitinfo(sql, bt, sn, duixiang, question, appeal, repay, jindu, attitude, process, manyi, evacont, tousutime,
                   dats)



# 解析html获得投诉流程信息
def getstep(tsdls, sql):
    '''
    :param tsdls: beautifulsoup 对象
    :param sql: 插入语句
    :return:
    '''
    CTIME = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    CSTR_ALL = str(CTIME)

    for tsdl in tsdls:

        tsqlist = tsdl.find("ul", attrs={"class": "ts-q-list"}).find_all("li")
        tmpstr = tsqlist[0].text
        tsbh = ''
        if tmpstr.find('投诉编号：') >= 0:
            tsbh = tmpstr.replace('投诉编号：', '')

        tsditems = tsdl.find_all("div", attrs={"class": "ts-d-item"})
        for tsditem in tsditems:
            outstr5 = CSTR_ALL
            outstr5 = outstr5 + ',' + str_fs_db_b(str(tsbh))

            tousubh = str_fs_db_b(str(tsbh))  # 投诉编号

            outstr5_tmp = ''
            outstr5_none = tsditem.find("span", attrs={"class": "u-name"})
            if outstr5_none != None:
                outstr5_tmp = outstr5_none.text
            outstr5 = outstr5 + ',' + str_fs_db_b(str(outstr5_tmp))

            name = str_fs_db_b(str(outstr5_tmp))  # 流程操作人

            outstr5_tmp = ''
            outstr5_none = tsditem.find("span", attrs={"class": "u-status"})
            if outstr5_none != None:
                outstr5_tmp = outstr5_none.text
            outstr5 = outstr5 + ',' + str_fs_db_b(str(outstr5_tmp))

            status = str_fs_db_b(str(outstr5_tmp))  # 当前状态

            outstr5_tmp = ''
            outstr5_none = tsditem.find("span", attrs={"class": "u-date"})
            if outstr5_none != None:
                outstr5_tmp = outstr5_none.text
            outstr5 = outstr5 + ',' + str_fs_db_b(str(outstr5_tmp))

            udate = str_fs_db_b(str(outstr5_tmp))  # 流程时间
            timee = udate[6:14]  # 提取时间
            hour = udate[6:8]  # 小时

            outstr5_tmp = ''
            outstr5_none = tsditem.find("div", attrs={"class": "ts-d-cont"})
            if outstr5_none != None:
                outstr5_tmp = outstr5_none.text
            outstr5 = outstr5 + ',' + str_fs_db_b(str(outstr5_tmp))

            content = str_fs_db_b(str(outstr5_tmp))  # 流程内容

            #print(outstr5)

            commitlist(sql, tousubh, name, status, content, udate, timee, hour)

        #print('------------------------------------------------------')
