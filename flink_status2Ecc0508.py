#! /usr/python/env python
# -*- coding:utf-8 -*-
import pymysql
import sys
import requests
import datetime
import json

data_in_id = []
data_out = []
process_env = sys.argv[1]
#测试mysql连接信息
if process_env=='test':
    host = '10.3.6.24'
    port = 3306
    db = 'flink_monitor'
    user = 'root'
    passwd = 'root'
#生产连接信息10.0.36.77:6390/flink_monitor  username: bx_edwqy  password: 9tv3GDI9bGDZ9VfS
else:
    host = '172.16.102.25'
    port = 3306
    db = 'flink_monitor'
    user = 'root'
    passwd = 'root@1q2w'

if process_env == 'test':
    url = "http://10.4.85.241:8888/api/emi/"  ## 测试环境 - 如果ip不通，则换用： "http://172.20.20.218:8080/api/emi/"
else:
    url = "http://aim.it.bx/portal/api/warning/extern/third_party/add.do"## "http://10.0.48.5:8888/aoms/jobBatchMonitor/jobBatchMonitor" ##"http://em.it.bx:8888/api/emi/"  ## 生产环境

#url_yarn = "http://10.3.6.24:8088/ws/v1/cluster/apps?applicationTypes=Apache%20Flink&queue=bdp"
#url_yarn = "http://10.3.6.24:8088/ws/v1/cluster/apps?applicationTypes=Apache%20Flink&finalStatus=FAILED"

userAndperson = sys.argv[2]
collect = {}
def userAndPerson(parm):
    for uap in parm.split(","):
        collect[uap.split(":")[0]] = uap.split(":")[1]

def mutiUserData():
    print("1111")
    #url_string = "http://10.3.6.24:8088/ws/v1/cluster/apps?applicationTypes=Apache%20Flink&finalStatus=FAILED&user="
    url_string = "http://10.3.6.24:8088/ws/v1/cluster/apps?applicationTypes=Apache%20Flink&user="
    for k in collect.keys():
        url_string1 = url_string + k
        insertData(url_string1)


def reduceData(url_string):
    values = getData(url_string)
    cur, conn = getCursor(host, port, db, user, passwd)
    cur.execute("select * from flink_monitor.flink_app;")
    conn.commit()



    for data in cur.fetchall():
        #print(data)
        data_in_id.append(data[0])
    #print(data_in_id)

    for to_insert in values:
        if to_insert[0] not in data_in_id:
            data_out.append(to_insert)

    closeConnection(conn, cur)

    # print("data_out is :")
    # print(data_out)
    return data_out


def insertData(url_string):
    data_out = reduceData(url_string)
    cur, conn = getCursor(host, port, db, user, passwd)
    cur.executemany("insert into flink_monitor.flink_app values(%s, %s, %s, %s, %s, %s, %s, %s);", data_out)
    conn.commit()

    closeConnection(conn, cur)

def getCursor(host, port, db, user, passwd):
    conn = pymysql.connect(host=host,
                           port=port,
                           db=db,
                           user=user,
                           passwd=passwd,
                           charset='utf8'
                           )
    cur = conn.cursor()
    return cur, conn

def closeConnection(conn, cur):
    conn.close()
    cur.close()

def getData(url):
    cont = requests.get(url)
    data = json.loads(cont.text)
    #print(data)
    values = []
    #values_Id = []
    if data['apps'] == None:
        print("there is no data to process...because no FAILED flink job.")
        #sys.exit(1)
    else:
        for ids in data['apps']['app']:
            value = []
            appId = ids['id']
            appUser = ids['user']
            appName = ids['name']
            appQueue = ids['queue']
            appState = ids['state']
            appFinalStatus = ids['finalStatus']
            appApplicationType = ids['applicationType']

            #values_Id.append(appId)
            value.append(appId)
            value.append(appUser)
            value.append(appName)
            value.append(appQueue)
            value.append(appState)
            value.append(appFinalStatus)
            value.append(appApplicationType)
            value.append("0")  # 设置flag
            #print(value)
            values.append(value)

        #print("values is ")
        #print(values)
    return values


def send2ecc(job_name, alarm_time, user_name, main_user_name, alarm_level, duty_person):

    dataset = {
        'host': 'flink',  ## 主机名
        'checker': 'triangle调度平台任务',  ## 报警来源（平台特征）
        'alarmTime': str(alarm_time),  ##发生时间
        'className': 'EDW',  ##报警分类
        'object': '调度任务：'+ job_name,  ## 监控对象
        'alarmLevel': alarm_level,  ## 级别：MINOR次要，MAJOR主要，NOTICE通知
        'metrics': '优先联系:${user_name}，第二联系人:${duty_person}，备用联系人:${main_user_name}'
            .replace('${user_name}',user_name)
            .replace('${main_user_name}',main_user_name)
            .replace('${duty_person}',duty_person),  ## 监控指标名称
        'value': '3',  ##监控指标当前值
        'threshold': '3',  ##告警阈值
        'note': 'flink任务失败告警',  ## 补充说明
        'url': '',  ## 处理建议
        'AdminContacts': ','.join((user_name, duty_person, main_user_name))
    }
    headers = {'Content-type': 'application/json'}
    resp = requests.post(url, data=json.dumps(dataset), headers=headers)
    text = resp.text
    print(json.dumps(dataset, ensure_ascii=False))
    print("----- return:")
    print(text)


if __name__ == '__main__':

    alarm_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # main_user_name = "yanghaile"
    # duty_person = "yanghaile"
    alarm_level = "MINOR"

    userAndPerson(userAndperson)
    mutiUserData()

    # for per in collect.values():
    #     print(per)
    #     main_user_name = per
    #     duty_person = per
    for search in data_out:
        job_name = search[2]
        user_name = search[1]
        main_user_name = collect[user_name]
        duty_person = collect[user_name]
        #send2ecc(job_name, alarm_time, user_name, main_user_name, alarm_level, duty_person)
        print(search)


    #查找未设置flag的记录并添加到需要设置的列表里
    sql_str = 'select * from flink_monitor.flink_app where flag=0 ;'
    cur, conn = getCursor(host, port, db, user, passwd)
    cur.execute(sql_str)
    print(sql_str)
    conn.commit()
    for set_flag in cur.fetchall():
        data_out.append(set_flag)

    closeConnection(conn, cur)


    for to_set in data_out:
        sql_str = 'update flink_monitor.flink_app set flag=1 where appId = "${to_set}";'.replace("${to_set}", to_set[0])
        cur, conn = getCursor(host, port, db, user, passwd)
        cur.execute(sql_str)
        print(sql_str)
        conn.commit()
        closeConnection(conn, cur)