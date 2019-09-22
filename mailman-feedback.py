#! /usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import csv
import smtplib
import mimetypes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import parseaddr, formataddr
import base64
import time
import datetime
import argparse
import logging
import chardet
import random
import atexit
import httplib
import urllib
import json


# global variables
g_workdir = os.path.join(os.path.split(
    os.path.realpath(sys.argv[0]))[0], 'work')
g_logdir = os.path.join(os.path.split(
    os.path.realpath(sys.argv[0]))[0], 'log')
g_test = ''
g_file_task_config = ''
g_file_task_mails = ''
g_file_mail_result = ''
g_file_task_tmp1 = ''
g_task_config = ''
g_task_tmp1 = ''
g_file_task_tmp2 = ''
g_task_tmp2 = ''


# 解析传进来的参数
def parse_args():
    global g_workdir, g_logdir, g_test

    parser = argparse.ArgumentParser(description='邮件群发系统')
    parser.add_argument('--work', nargs='?', required=False,
                        help='工作目录,默认当前程序运行目录')
    parser.add_argument('--log', nargs='?', required=False,
                        help='日志目录,默认当前程序运行目录')
    # parser.add_argument('--test', nargs='?', required=False,
    #                     help='测试一封邮件，输入测试邮件收件地址')

    args = vars(parser.parse_args())
    if args['work'] != None:
        g_workdir = args['work']
    if args['log'] != None:
        g_logdir = args['log']
    # if args['test'] != None:
    #     g_test = args['test']


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]
def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


# 发反馈
def start_feedback():
    global g_workdir, g_logdir, g_test\
        ,g_file_task_config, g_file_task_mails, g_file_mail_result, g_file_task_tmp1\
        ,g_task_config, g_task_tmp1\
        ,g_file_task_tmp2, g_task_tmp2

    interval_arr = g_task_config['interval'].split(',')

    with open(g_file_mail_result, mode='r') as fp:
        csv_reader = csv.reader(fp, delimiter=',')
        row_count = 0
        conn = httplib.HTTPSConnection('mailman.sme.wang')
        for row in csv_reader:
            row_count += 1

            if row_count<=g_task_tmp2['mail_feedback_last_row']:
                continue
            else:
                g_task_tmp2['mail_feedback_last_row'] = row_count
                g_task_tmp2['mail_feedback_last_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if not row:
                continue

            print("第%s行 %s" % (str(row)))

            params = {
                "mail_id": row[0],
                "date_sent": row[1],
                "send_result": row[2],
                "send_error": row[3],
                }
            params = urllib.urlencode(params)
            conn.request("POST", "/edm/campaign/api_mail_feedback", params)
            response = conn.getresponse()
            content = response.read()
        conn.close()
        fp.close()




# 日志功能
def logs(level, msg):
    global g_logdir

    #msg_code = chardet.detect(msg)
    #
    #if msg_code["encoding"] == "GB2312":
    #    msg = msg.decode('gbk').encode("utf-8")

    LEVEL = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    # print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if level == 0:
        fhd = open(g_logdir + '/debug.log', 'ab+')
    elif level == 3:
        fhd = open(g_logdir + '/stderr.log', 'ab+')
    else:
        fhd = open(g_logdir + '/stdout.log', 'ab+')

    fhd.write("%s %-8s %s\n" %
              (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), LEVEL[level], msg))

    fhd.close()
    #log.log(level, msg)
    # if level == "debug":
    #     logger.debug(msg)
    # elif level == "err":
    #     logger.error(msg)
    # else:
    #     logger.info(msg)

def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def get_file_content(file):
    content = False
    if (os.path.isfile(file)):
        with open(file, 'r') as fp:
            content = fp.read().strip()
        fp.close()
    return content

def set_file_content(file, content, append=False):
    mode = 'w' if not append else 'a'
    with open(file, mode) as fp:
        # fp.write(content) #TypeError: expected a string or other character buffer object
        fp.write(str(content))
    fp.close()

def set_csv_content(file, row, append=False):
    mode = 'w' if not append else 'a'
    with open(file, mode) as fp:
        csvwriter = csv.writer(fp)
        csvwriter.writerow(row)
    fp.close()

'''把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12'''
def TimeStampToTime(timestamp):
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S',timeStruct)

'''获取文件的大小,结果保留两位小数，单位为MB'''
def get_FileSize(filePath):
    filePath = unicode(filePath,'utf8')
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024)
    return round(fsize,2)

'''获取文件的访问时间'''
def get_FileAccessTime(filePath):
    filePath = unicode(filePath,'utf8')
    t = os.path.getatime(filePath)
    return TimeStampToTime(t)

'''获取文件的创建时间'''
def get_FileCreateTime(filePath):
    filePath = unicode(filePath,'utf8')
    t = os.path.getctime(filePath)
    return TimeStampToTime(t)

'''获取文件的修改时间'''
def get_FileModifyTime(filePath):
    filePath = unicode(filePath,'utf8')
    t = os.path.getmtime(filePath)
    return TimeStampToTime(t)




# 主程序入口
def main():
    global g_workdir, g_logdir, g_test\
        ,g_file_task_config, g_file_task_mails, g_file_mail_result, g_file_task_tmp1\
        ,g_task_config, g_task_tmp1\
        ,g_file_task_tmp2, g_task_tmp2

    parse_args()

    # if not os.path.isdir(g_workdir):
    #     os.mkdir(g_workdir)
    # if not os.path.isdir(g_logdir):
    #     os.mkdir(g_logdir)

    g_file_task_config = 'task.config.txt'
    g_file_task_mails = 'task.mails.csv'
    g_file_mail_result = 'task.mail_result.csv'
    g_file_task_tmp1 = 'task.tmp1.txt'
    g_file_task_tmp2 = 'task.tmp2.txt'

    g_file_task_config = os.path.join(g_workdir, g_file_task_config)
    g_file_task_mails = os.path.join(g_workdir, g_file_task_mails)
    g_file_mail_result = os.path.join(g_workdir, g_file_mail_result)
    g_file_task_tmp1 = os.path.join(g_workdir, g_file_task_tmp1)
    g_file_task_tmp2 = os.path.join(g_workdir, g_file_task_tmp2)

    #检查是否在运行
    # pid = os.getpid()
    # pid_file = os.path.join(g_workdir, 'pid')
    # if os.path.isfile(pid_file):
    #     pid_in_file = get_file_content(pid_file)
    #     if (check_pid(pid_in_file)):
    #         print '程序运行中，退出程序'
    #         os._exit()
    #     else:
    #         os.remove(pid_file)
    # set_file_content(pid_file, pid)

    # 获取配置
    # if not(os.path.isfile(g_file_task_config)):
    #     conn = httplib.HTTPSConnection('mailman.sme.wang')
    #     conn.request("GET", "/edm/campaign/api_get_task")
    #     response = conn.getresponse()
    #     task_config_str = response.read()
    #     g_task_config = json.loads(task_config_str)
    #     if g_task_config['task_id']==0:
    #         print '当前没有任务可接'
    #         sys.exit()
    #     else:
    #         set_file_content(g_file_task_config, task_config_str)
    #     conn.close()
    # else:
    #     g_task_config = json.loads(get_file_content(g_file_task_config))

    # 检查mail_result文件
    if not(os.path.isfile(g_file_mail_result)):
        print 'mail_result文件不存在'
        sys.exit()

    # 检查tmp2文件
    if not(os.path.isfile(g_file_task_tmp2)):
        print 'tmp2文件不存在'
        sys.exit()

    # 获取tmp2
    g_task_tmp2 = json.loads(get_file_content(g_file_task_tmp2))

    # 检查时间
    result_mtime = get_FileModifyTime(g_file_mail_result)
    if g_task_tmp2['mail_feedback_last_time']>result_mtime:
        print 'mail_result文件没有新数据，无须处理'
        sys.exit()

    # 开始工作
    start_feedback()

def all_done():
    global g_workdir, g_logdir, g_test\
        ,g_file_task_config, g_file_task_mails, g_file_mail_result, g_file_task_tmp1\
        ,g_task_config, g_task_tmp1\
        ,g_file_task_tmp2, g_task_tmp2

    # 保存tmp2
    if g_task_tmp2:
        set_file_content(g_file_task_tmp2, json.dumps(g_task_tmp2))

# 程序起始
if __name__ == '__main__':
    atexit.register(all_done)
    main()
