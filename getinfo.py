import os
import codecs
import requests
import urllib3
from bs4 import BeautifulSoup
import utils
import  datetime as dt
d2=dt.date.today().strftime("%Y%m%d")
d2=str(d2)
# 消除警告
urllib3.disable_warnings()

def run(conid):
    selfile = r'D:/yy/heimao_Response.txt'  # 抓包文件路径

    idfile = r'data/snid.txt'  # 投诉单id保存路径

    errorfile = r'data/error1.txt'  # 出错id保存位置

    # 建库/表语句
    sql = 'CREATE DATABASE if not exists heimaodata DEFAULT CHARACTER SET utf8'
    sql1 = 'create TABLE if not exists tousu_steplist (`sn投诉编号` varchar(255) DEFAULT NULL,`operator该流程操作人` varchar(255) DEFAULT NULL,`status该流程的当前状态` varchar(255) DEFAULT NULL,`operate_content该流程的操作内容` LONGTEXT DEFAULT NULL,`operate_time该流程的时间节点` varchar(255) DEFAULT NULL,`提取时间` varchar(255) DEFAULT NULL,`小时` varchar(255) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8'
    sql2 = 'create TABLE if not exists tousu_sina(`title投诉标题` varchar(255) DEFAULT NULL,`sn投诉编号` varchar(255) DEFAULT NULL,`cotitle投诉对象` varchar(255) DEFAULT NULL,`reason投诉问题` varchar(255) DEFAULT NULL,`appeal投诉要求` varchar(255) DEFAULT NULL,`request_repay_fee涉诉金额（元）` varchar(255) DEFAULT NULL,`status投诉进度` varchar(255) DEFAULT NULL,`attitude服务态度评分` varchar(255) DEFAULT NULL,`process处理速度评分` varchar(255) DEFAULT NULL,`satisfaction满意度评分` varchar(255) DEFAULT NULL,`evalContent处理后用户的评论` LONGTEXT DEFAULT NULL,`appeal_date投诉产生时间` varchar(255) DEFAULT NULL,`crawler_time爬取时间` varchar(255) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8'

    # 插入语句
    sql3 = 'insert into tousu_sina(`title投诉标题`,`sn投诉编号`,`cotitle投诉对象`,`reason投诉问题`,`appeal投诉要求`,`request_repay_fee涉诉金额（元）`,`status投诉进度`,`attitude服务态度评分`,`process处理速度评分`,`satisfaction满意度评分`,`evalContent处理后用户的评论`,`appeal_date投诉产生时间`,`crawler_time爬取时间`) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    sql4 = 'insert into tousu_steplist(`sn投诉编号`,`operator该流程操作人`,`status该流程的当前状态`,`operate_content该流程的操作内容`,`operate_time该流程的时间节点`,`提取时间`,`小时`) values(%s,%s,%s,%s,%s,%s,%s)'
    # 快递公司id
    # list = [5501611052, 6756505772, 5228185692, 2365681750, 2146783270, 3201585302, 1996777023, 3614813151]


    db = utils.sqlcd()
    cur1 = db.cursor()
    # 建库
    cur1.execute(sql)
    db.commit()
    utils.dbclose(db)

    db2 = utils.sqlct()
    cur2 = db2.cursor()
    # 建表..
    cur2.execute(sql1)
    db2.commit()
    cur2.execute(sql2)
    db2.commit()

    # 调用模拟滑动方法
    utils.selpath(selfile, 50, conid)  # 传入抓包保存文件以及滑动多少次

    # 调用方法解析文件获取投诉编号
    utils.getsnid(selfile, idfile)

    # 拼接投诉链接并请求
    tsdls = utils.reqinfo(idfile, errorfile)  # 返回beautifulsoup 对象

    # 解析投诉单信息
    print(sql3)
    utils.getinfo(tsdls, sql3)

    # 解析投诉流程信息
    utils.getstep(tsdls, sql4)

    utils.dbclose(db2)


    ################数据排重##################
    sql8 = 'drop table if exists tousu_sina_all_%s'%(d2)
    sql9 = 'drop table if exists tousu_steplist_all'
    sql5 = 'create table tousu_sina_all_%s as select * from (select * from tousu_sina) t1 group by `sn投诉编号` ORDER BY `cotitle投诉对象`,`appeal_date投诉产生时间`'%(d2)
    sql6 = 'create table tousu_steplist_all as select * from (select * from tousu_steplist) t group by `sn投诉编号`,`operate_time该流程的时间节点`,`status该流程的当前状态` ORDER BY `sn投诉编号`, `operate_time该流程的时间节点`'
    db2.ping(reconnect=True)
    # 投诉单数据去重
    cur2.execute(sql8)
    db2.commit()
    cur2.execute(sql5)
    db2.commit()

    # 投诉流程信息去重
    cur2.execute(sql9)
    db2.commit()
    cur2.execute(sql6)
    db2.commit()
    conid_dict = {'5501611052':'中国邮政','6756505772':'京东','5228185692':'顺丰','2365681750':'中通','2146783270':'圆通'
                 ,'3201585302':'申通','1996777023':'百世快递','3614813151':'韵达'}
    sql7 = '''select SUBSTRING(appeal_date投诉产生时间,1,10) as time ,count(distinct `sn投诉编号`) from tousu_sina_all_{} where cotitle投诉对象 like '%{}%' and DATE_SUB(CURDATE(), INTERVAL 9 DAY) <= date(SUBSTRING(appeal_date投诉产生时间,1,10))
                  group by SUBSTRING(appeal_date投诉产生时间,1,10) ORDER BY time desc'''.format(d2,conid_dict[str(conid)])
    cur2.execute(sql7)
    rs = cur2.fetchall()
    print(' {} 的数据库数据情况如下：'.format(conid_dict[str(conid)]))
    num = 1
    for r in rs:
        print(num,conid_dict[str(conid)],r['time'],r['count(distinct `sn投诉编号`)'])
        num += 1
    db2.commit()

    print('完成 {} 的数据库写入与排重！！'.format(conid_dict[str(conid)]))

if __name__ == '__main__':
    conids = [5501611052, 6756505772, 5228185692, 2365681750, 2146783270, 3201585302, 1996777023, 3614813151]
    # conids =  [5228185692, 2365681750, 2146783270, 3201585302, 1996777023, 3614813151]
    # conid = [6756505772,5228185692] # 京东物流 顺丰
    # conid = [2365681750,2146783270] # 中通 圆通
    # conid = [3201585302, 1996777023] # 申通 百世快递
    # conid = [3614813151] # 韵达
    for conid in conids:
        run(conid)
    print('完成所有竞品的爬数！')
