## Prepare Environment
    pip install -r require.txt

## Usage
    usage: rssemail.py [-h] --file [FILE] --content [CONTENT] --title [TITLE]
                   [-t [T]] [--alias [ALIAS]] [--from [FROM]] [--smtp [SMTP]]
                   [--passwd [PASSWD]] [--port [PORT]] [-c [C]] [--log [LOG]]

    邮件群发系统

    optional arguments:
      -h, --help           show this help message and exit
      --file [FILE]        变量文件
      --content [CONTENT]  邮件内容文件
      --title [TITLE]      邮件标题文件
      -t [T]               每封邮件间隔时间,如-t 30,60
      --alias [ALIAS]      发件人别名，如果为空,默认使用邮件地址
      --from [FROM]        发件人邮件地址
      --smtp [SMTP]        发件邮件服务器地址
      --passwd [PASSWD]    发件邮件服务器登录密码
      --port [PORT]        发件邮件服务器端口
      -c [C]               变量所在列,0列为接收人邮件地址
      --log [LOG]          日志存储位置,默认当前程序运行目录


## Example
    python rssemail.py --file ~/Program/tmp/testfile1.csv \
    --title ~/Program/tmp/testtitle.txt \
    --content ~/Program/tmp/testmsg.txt \
    -t 10,40 -c 2,5,3 --from noreply@mail.xxxx.cn \
    --smtp smtp.xxxx.com --passwd xxxxxxxx --port 465
