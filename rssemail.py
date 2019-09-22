#! /usr/bin/env python
# -*- coding:utf-8 -*-

import csv
import sys
import os
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
from jinja2 import Environment, FileSystemLoader


# global variables
g_varfile = ''
g_contentfile = ''
g_titefile = ''
g_delaytime = ''
#g_from = 'root@localhost'
g_from = ''
g_alias = ''
g_smtp = ''
g_account = ''
g_passwd = ''
g_port = 25
g_columns = ''
g_log_folder = os.path.join(os.path.split(
    os.path.realpath(sys.argv[0]))[0], 'log')
g_replaces = ''
g_test = ''


# 解析传进来的参数
def parse_args():
    global g_varfile, g_contentfile, g_titlefile, g_delaytime, g_from, g_alias, g_smtp, g_account, g_passwd, g_port, g_columns, g_log_folder, g_replaces, g_test

    parser = argparse.ArgumentParser(description='邮件群发系统')
    parser.add_argument('--file', nargs='?', required=True, help='变量文件')
    parser.add_argument('--content', nargs='?', required=True, help='邮件内容文件')
    parser.add_argument('--title', nargs='?', required=True, help='邮件标题文件')
    parser.add_argument('-t', nargs='?', required=False,
                        help='每封邮件间隔时间,如-t 30,60')
    parser.add_argument('--from', nargs='?', required=False, help='发件人邮箱')
    parser.add_argument('--alias', nargs='?', required=False,
                        help='发件人别名，如果为空,默认使用邮件地址')
    parser.add_argument('--smtp', nargs='?', required=True, help='发件服务器')
    parser.add_argument('--account', nargs='?',
                        required=False, help='发件账号')
    parser.add_argument('--passwd', nargs='?',
                        required=False, help='发件密码')
    parser.add_argument('--port', nargs='?',
                        required=False, help='发件端口')
    parser.add_argument('-c', nargs='?', required=False,
                        help='变量所在列,0列为接收人邮件地址')
    parser.add_argument('--log', nargs='?', required=False,
                        help='日志存储位置,默认当前程序运行目录')
    parser.add_argument('--replaces', nargs='?', required=False,
                        help='替换内容，格式如"key1=value1&&key2=value2&&key3=value3"')
    parser.add_argument('--test', nargs='?', required=False,
                        help='测试一封邮件，输入测试邮件收件地址')

    args = vars(parser.parse_args())
    if args['file'] != None:
        g_varfile = args['file']
    if args['content'] != None:
        g_contentfile = args['content']
    if args['title'] != None:
        g_titlefile = args['title']
    if args['t'] != None:
        g_delaytime = args['t']
    if args['from'] != None:
        g_from = args['from']
    if args["alias"] != None:
        g_alias = args["alias"]
    if args['smtp'] != None:
        g_smtp = args['smtp']
    if args['account'] != None:
        g_account = args['account']
    if args['passwd'] != None:
        g_passwd = args['passwd']
    if args['port'] != None:
        g_port = args['port']
    if args['c'] != None:
        g_columns = args['c']
    if args['log'] != None:
        g_log_folder = args['log']
    if args['replaces'] != None:
        g_replaces = args['replaces']
    if args['test'] != None:
        g_test = args['test']

    if not g_from:
        if g_account.find('@')>-1:
            g_from = g_account
    else:
        if g_account.find('@')>-1 and g_account<>g_from:
            g_from = g_account

    if not g_from:
        print '发件人邮箱为空'
        sys.exit()
    if g_from.find('@')==-1:
        print '发件人邮箱格式不对'
        sys.exit()


# 读取变量文件，获取收件人地址和所有变量
def parse_var_file():
    global g_varfile, g_columns, g_from, g_alias, g_smtp, g_account, g_passwd, g_port, g_replaces, g_test

    dic_replaces = {}
    if len(g_replaces)>0:
        replaces_arr = g_replaces.split('&&')
        for str in replaces_arr:
            str_arr = str.split('=')
            dic_replaces[str_arr[0]] = str_arr[1].decode('utf-8')

    columns_array = g_columns.split(',')

    #with open(g_varfile) as fp:
    import io
    with io.open(g_varfile, "r", encoding="utf-8") as fp:
        for line in fp:
            time.sleep(float(generate_random_sleeptime()))  # 休眠间隔时间

            line_array = line.split(',')
            #dic_vars = {}
            dic_vars = dic_replaces

            to_mail = line_array[0].strip()
            if len(to_mail) == 0:
                continue
            dic_vars['__email__'] = to_mail

            # 设置变量对应的字段，用来替换模板文件中对应的变量位置
            for i in columns_array:
                #msg_code = chardet.detect(line_array[int(i)])
                #if msg_code["encoding"] == "GB2312":
                #    dic_vars[
                #        'var' + i] = line_array[int(i)].decode('gbk')
                #elif msg_code["encoding"] == "utf-8":
                #    dic_vars[
                #        'var' + i] = line_array[int(i)].decode('utf-8')
                #else:
                #    dic_vars['var' + i] = line_array[int(i)]
                dic_vars['var' + i] = line_array[int(i)]

            email_title = create_title(dic_vars)
            email_message = create_message(dic_vars)

            # print "email_tile: %s" % email_title
            # print "email_message: %s" % email_message

            # 拼接sendmail用的message，包含header和content
            msg = MIMEMultipart()
            if g_alias:
                msg['From'] = formataddr(
                    (Header(g_alias, 'utf-8').encode(), g_from))
            else:
                msg['From'] = g_from
            if len(g_test)==0:
                msg['To'] = to_mail
            else:
                msg['To'] = g_test
            msg['Subject'] = Header(email_title, charset='UTF-8')

            # 添加邮件内容
            #txt = MIMEText(email_message, _subtype='plain', _charset='UTF-8')
            # 添加html的邮件内容
            #txt = MIMEText(email_message, _subtype='html', _charset='UTF-8')
            email_message_lower = email_message.lower()
            if email_message_lower.find('<html')==-1 and email_message_lower.find('<p')==-1 and email_message_lower.find('<br')==-1:
                email_message = email_message.replace('\n','<br>\n')
            txt = MIMEText(email_message, _subtype='html', _charset='UTF-8')
            msg.attach(txt)

            # 准备发送邮件
            if len(g_test)==0:
                sendmail(g_from, to_mail, msg, g_smtp, g_account, g_passwd, g_port)
            else:
                sendmail(g_from, g_test, msg, g_smtp, g_account, g_passwd, g_port)
                sys.exit()


# 从模板文件生成邮件主题
def create_title(vars):
    global g_titlefile

    THIS_DIR = os.path.dirname(g_titlefile)
    This_file = os.path.basename(g_titlefile)

    # Create the jinja2 environment.
    # Notice the use of trim_blocks, which greatly helps control whitespace.
    j2_env = Environment(loader=FileSystemLoader(THIS_DIR),
                         trim_blocks=True)

    # print vars
    return j2_env.get_template(This_file).render(vars)


# 从模板文件生成邮件内容
def create_message(vars):
    global g_contentfile

    THIS_DIR = os.path.dirname(g_contentfile)
    This_file = os.path.basename(g_contentfile)

    # Create the jinja2 environment.
    # Notice the use of trim_blocks, which greatly helps control whitespace.
    j2_env = Environment(loader=FileSystemLoader(THIS_DIR),
                         trim_blocks=True)
    msg = j2_env.get_template(This_file).render(vars)
    return msg
    # return base64.b64encode(msg)


# 从给定发送邮件间隔时间随机生成具体的间隔秒数
def generate_random_sleeptime():
    global g_delaytime

    delay_array = g_delaytime.split(',')

    return random.randrange(int(delay_array[0]), int(delay_array[1]))


# 日志功能
def logs(level, msg):
    global g_log_folder

    #msg_code = chardet.detect(msg)
    #
    #if msg_code["encoding"] == "GB2312":
    #    msg = msg.decode('gbk').encode("utf-8")

    LEVEL = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    # print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if level == 0:
        fhd = open(g_log_folder + '/debug.log', 'ab+')
    elif level == 3:
        fhd = open(g_log_folder + '/stderr.log', 'ab+')
    else:
        fhd = open(g_log_folder + '/stdout.log', 'ab+')

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


## 发送邮件主程序 ##
# from_email: 发件人地址
# to_email: 收件人地址
# message: 邮件内容
# s_server: 发件服务器
# s_account: 发件账号
# s_passwd: 发件密码
# s_port: 发件端口
def sendmail(from_email, to_email, message, s_server='localhost', s_account='', s_passwd='', s_port=''):
    global g_log_folder

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
        if s_account or s_passwd:
            smtp.login(s_account, s_passwd)
        logs(1, "start to send email to %s" % to_email)
        smtp.sendmail(from_email, to_email, message.as_string())
        smtp.close()
        logs(1, "%s to %s success\n" % (from_email, to_email))
    # except smtplib.SMTPResponseException, e:
    #     errcode = getattr(e, 'smtp_code', -1)
    #     errmsg = getattr(e, 'smtp_error', 'ignore')
    #     logs(0, "%d, %s" % (errcode, errmsg))
    #     logs(3, "%s to %s faild" % (from_email, to_email))
    # except smtplib.SMTPSenderRefused, e:
    #     errcode = getattr(e, 'smtp_code', -1)
    #     errmsg = getattr(e, 'smtp_error', 'ignore')
    #     sender = getattr(e, 'sender', 'None')
    #     logs(0, "%d, %s, %s" % (errcode, errmsg, sender))
    #     logs(3, "%s to %s faild" % (from_email, to_email))
    # except smtplib.SMTPRecipientsRefused, e:
    #     recipients = getattr(e, "recipients", "None")
    #     logs(0, "%s was refused" % recipients)
    #     logs(3, "%s to %s faild" % (from_email, to_email))
    except Exception as e:
        # if hasattr(e, 'message'):
        #     logs(0, e.message)
        # else:
        #     logs(0, e)
        logs(0, e)
        logs(3, "%s to %s faild" % (from_email, to_email))


# 主程序入口
def main():
    global g_varfile, g_contentfile, g_titlefile, g_delaytime, g_log_folder, g_from, g_smtp, g_passwd, g_columns

    parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info('==================================================')
    logger.info('变量文件:       %s' % g_varfile)
    logger.info('变量所在列:      %s' % g_columns)
    logger.info('邮件主题文件:     %s' % g_titlefile)
    logger.info('邮件内容文件:     %s' % g_contentfile)
    logger.info('邮件间隔时间:     %s' % g_delaytime)
    logger.info('发送邮件服务器地址:  %s' % g_smtp)
    logger.info('发送邮件服务器端口:  %s' % g_port)
    logger.info('log folder: %s' % g_log_folder)
    logger.info('==================================================')

    if not os.path.isdir(g_log_folder):
        os.mkdir(g_log_folder)

    parse_var_file()


# 程序起始
if __name__ == '__main__':
    main()
