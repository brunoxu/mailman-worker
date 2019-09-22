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
    parser.add_argument('--test', nargs='?', required=False,
                        help='测试一封邮件，输入测试邮件收件地址')

    args = vars(parser.parse_args())
    if args['work'] != None:
        g_workdir = args['work']
    if args['log'] != None:
        g_logdir = args['log']
    if args['test'] != None:
        g_test = args['test']


# 发邮件
def start_send():
    global g_workdir, g_logdir, g_test\
        ,g_file_task_config, g_file_task_mails, g_file_mail_result, g_file_task_tmp1\
        ,g_task_config, g_task_tmp1

    interval_arr = g_task_config['interval'].split(',')

    with open(g_file_task_mails, mode='r', encoding="utf-8") as fp:
        csv_reader = csv.reader(fp, delimiter=',')
        row_count = 0
        for row in csv_reader:
            row_count += 1

            if row_count<=g_task_tmp1['mail_send_last_row']:
                continue
            else:
                g_task_tmp1['mail_send_last_row'] = row_count

            if not row:
                continue

            to_mail = row[3].strip()
            if len(to_mail) == 0:
                continue

            delay_seconds = random.randrange(int(interval_arr[0]), int(interval_arr[1]))
            time.sleep(float(delay_seconds))

            # logs(1, "row %s, to:%s, delay:%s" % (row_count, to_mail, delay_seconds))
            print("第%s行 to:%s, delay:%s" % (row_count, to_mail, delay_seconds))

            # 拼接sendmail用的message，包含header和content
            msg = MIMEMultipart()
            if g_task_config['from_alias']:
                msg['From'] = formataddr(
                    (Header(g_task_config['from_alias'], 'utf-8').encode(), g_task_config['from']))
            else:
                msg['From'] = g_task_config['from']
            if len(g_test)==0:
                msg['To'] = to_mail
            else:
                msg['To'] = g_test
            msg['Subject'] = Header(row[4], charset='UTF-8')
            txt = MIMEText(row[5], _subtype='html', _charset='UTF-8')
            msg.attach(txt)

            # 发送邮件
            if len(g_test)==0:
                send_result, send_error = sendmail(g_task_config['from'], to_mail, msg
                    , g_task_config['smtp_server'], g_task_config['smtp_account']
                    , g_task_config['smtp_password'], g_task_config['smtp_port'])

                # 记录发送结果
                mail_id = row[0]
                date_sent = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                row_arr = [mail_id, date_sent, send_result, send_error]
                set_csv_content(g_file_mail_result, row_arr, True)
            else:
                send_result, send_error = sendmail(g_task_config['from'], g_test, msg
                    , g_task_config['smtp_server'], g_task_config['smtp_account']
                    , g_task_config['smtp_password'], g_task_config['smtp_port'])
                print('发送测试邮件 send_result:%s, send_error:%s' % (send_result, send_error))
                sys.exit()
        fp.close()

        # 记录完成
        g_task_tmp1['mail_send_finished'] = 1
        g_task_tmp1['mail_send_finish_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 任务完成反馈
        conn = httplib.HTTPSConnection('mailman.sme.wang')
        params = {
            "task_id": g_task_config['task_id']
            }
        params = urllib.urlencode(params)
        conn.request("POST", "/edm/campaign/api_task_done", params)
        response = conn.getresponse()
        content = response.read()
        conn.close()




## 发送邮件主程序 ##
# from_email: 发件人地址
# to_email: 收件人地址
# message: 邮件内容
# s_server: 发件服务器
# s_account: 发件账号
# s_passwd: 发件密码
# s_port: 发件端口
def sendmail(from_email, to_email, message, s_server='localhost', s_account='', s_passwd='', s_port=''):
    send_result = 1
    send_error = ''
    try:
        logs(1, "connect to smtp server %s" % s_server)
        if not s_port:
            s_port = 25
        #print s_port
        if int(s_port) == 465 or int(s_port) == 587:
            #print '1111'
            #sys.exit()
            # smtp = smtplib.SMTP_SSL('%s' % (s_server), s_port)
            smtp = smtplib.SMTP('%s' % (s_server), s_port)
            smtp.starttls()
        else:
            #print '2222'
            #sys.exit()
            smtp = smtplib.SMTP('%s' % (s_server), s_port)
        smtp.set_debuglevel(0)
        if s_account:
            smtp.login(s_account, s_passwd)
        logs(1, "start to send email to %s" % to_email)
        smtp.sendmail(from_email, to_email, message.as_string())
        smtp.quit()
        logs(1, "%s to %s success" % (from_email, to_email))
    # except smtplib.SMTPResponseException, e:
    #     errcode = getattr(e, 'smtp_code', -1)
    #     errmsg = getattr(e, 'smtp_error', 'ignore')
    #     logs(0, "%d, %s" % (errcode, errmsg))
    #     logs(3, "%s to %s failed" % (from_email, to_email))
    # except smtplib.SMTPSenderRefused, e:
    #     errcode = getattr(e, 'smtp_code', -1)
    #     errmsg = getattr(e, 'smtp_error', 'ignore')
    #     sender = getattr(e, 'sender', 'None')
    #     logs(0, "%d, %s, %s" % (errcode, errmsg, sender))
    #     logs(3, "%s to %s failed" % (from_email, to_email))
    # except smtplib.SMTPRecipientsRefused, e:
    #     recipients = getattr(e, "recipients", "None")
    #     logs(0, "%s was refused" % recipients)
    #     logs(3, "%s to %s failed" % (from_email, to_email))
    except Exception as e:
        # if hasattr(e, 'message'):
        #     logs(0, e.message)
        # else:
        #     logs(0, e)
        logs(0, e)
        logs(3, "%s to %s failed" % (from_email, to_email))
        send_result = 0
        send_error = str(e)
    return send_result, send_error




# 主程序入口
def main():
    global g_workdir, g_logdir, g_test\
        ,g_file_task_config, g_file_task_mails, g_file_mail_result, g_file_task_tmp1\
        ,g_task_config, g_task_tmp1\
        ,g_file_task_tmp2, g_task_tmp2

    parse_args()

    if not os.path.isdir(g_workdir):
        os.mkdir(g_workdir)
    if not os.path.isdir(g_logdir):
        os.mkdir(g_logdir)

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
    pid = os.getpid()
    pid_file = os.path.join(g_workdir, 'pid')
    if os.path.isfile(pid_file):
        pid_in_file = get_file_content(pid_file)
        if (check_pid(pid_in_file)):
            print '程序运行中，退出程序'
            os._exit()
        else:
            os.remove(pid_file)
    set_file_content(pid_file, pid)

    # 获取配置
    if not(os.path.isfile(g_file_task_config)):
        conn = httplib.HTTPSConnection('mailman.sme.wang')
        conn.request("GET", "/edm/campaign/api_get_task")
        response = conn.getresponse()
        task_config_str = response.read()
        g_task_config = json.loads(task_config_str)
        if g_task_config['task_id']==0:
            print '当前没有任务可接'
            sys.exit()
        else:
            set_file_content(g_file_task_config, task_config_str)
        conn.close()
    else:
        g_task_config = json.loads(get_file_content(g_file_task_config))

    # 处理和检查from
    if not g_task_config['from']:
        if g_task_config['smtp_account'].find('@')>-1:
            g_task_config['from'] = g_task_config['smtp_account']
    else:
        if g_task_config['smtp_account'].find('@')>-1 and g_task_config['smtp_account']<>g_task_config['from']:
            g_task_config['from'] = g_task_config['smtp_account']
    if not g_task_config['from']:
        print '发件人邮箱为空'
        sys.exit()
    if g_task_config['from'].find('@')==-1:
        print '发件人邮箱格式不对'
        sys.exit()

    # 获取邮件
    if not(os.path.isfile(g_file_task_mails)):
        # import wget
        # wget.download('https://mailman.sme.wang/edm/campaign/api_get_mails', g_file_task_mails)
        # 线上服务器Python 2.7.15+和本地的Python 3.6.0，都提示No module named 'wget'    换其他方法
        # import httplib，本地python3报错，No module named 'httplib'，查资料 https://stackoverflow.com/questions/13778252/import-httplib-importerror-no-module-named-httplib 说，换成http.client了
        conn = httplib.HTTPSConnection('mailman.sme.wang')
        ind = 0
        while ind<100:
            params = {
                "task_id": g_task_config['task_id'],
                }
            # headers = {
            #     'User-Agent': 'python',
            #     'Content-Type': 'application/x-www-form-urlencoded',
            #     }
            params = urllib.urlencode(params)
            # conn.request("POST", uri, params, headers)
            conn.request("GET", "/edm/campaign/api_get_mails", params)
            response = conn.getresponse()
            content = response.read()
            if content:
                if ind==0:
                    set_file_content(g_file_task_mails, content)
                else:
                    set_file_content(g_file_task_mails, content, True)
            else:
                if ind==0:
                    print '当前任务没有邮件要发'
                    sys.exit()
                break
            ind += 1
        conn.close()

    # 获取tmp1
    if not(os.path.isfile(g_file_task_tmp1)):
        g_task_tmp1 = {
            "mail_send_last_row": 0,
            'mail_send_finished': 0,
            "mail_send_finish_time": '',
            }
        set_file_content(g_file_task_tmp1, json.dumps(g_task_tmp1))
    else:
        g_task_tmp1 = json.loads(get_file_content(g_file_task_tmp1))

    # 检查时间
    mails_mtime = get_FileModifyTime(g_file_task_mails)
    if g_task_tmp1['mail_send_finished'] and g_task_tmp1['mail_send_finish_time']>mails_mtime:
        print 'mails文件没有新数据，无须处理'
        sys.exit()

    # 初始化tmp2
    if not(os.path.isfile(g_file_task_tmp2)):
        g_task_tmp2 = {
            "mail_feedback_last_row": 0,
            "mail_feedback_last_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        set_file_content(g_file_task_tmp2, json.dumps(g_task_tmp2))

    # info
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info('==================================================')
    logger.info('g_workdir:     %s' % g_workdir)
    logger.info('g_logdir:      %s' % g_logdir)
    logger.info('g_test:        %s' % g_test)
    logger.info('task_id:       %s' % g_task_config['task_id'])
    logger.info('task_name:     %s' % g_task_config['task_name'])
    logger.info('domian:        %s' % g_task_config['domian'])
    logger.info('from:          %s' % g_task_config['from'])
    logger.info('from_alias:    %s' % g_task_config['from_alias'])
    logger.info('interval:      %s' % g_task_config['interval'])
    logger.info('smtp_server:   %s' % g_task_config['smtp_server'])
    logger.info('smtp_port:     %s' % g_task_config['smtp_port'])
    logger.info('==================================================')

    # 开始发送
    start_send()

# 程序起始
if __name__ == '__main__':
    atexit.register(all_done)
    main()

def all_done():
    global g_workdir, g_logdir, g_test\
        ,g_file_task_config, g_file_task_mails, g_file_mail_result, g_file_task_tmp1\
        ,g_task_config, g_task_tmp1

    # 删除pid文件
    pid_file = os.path.join(g_workdir, 'pid')
    os.remove(pid_file)

    # 保存tmp1
    if g_task_tmp1:
        set_file_content(g_file_task_tmp1, json.dumps(g_task_tmp1))




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
        fp.write(content)
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
