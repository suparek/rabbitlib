#encoding=utf-8
# -*- coding: utf-8 -*-
import os
import sys
import click
import huoutil
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import json
import pika
from mq import MessageQueue

in_data = {
    "content": "测试一下消息队列发送",
}

data_str = json.dumps(in_data, ensure_ascii=False)

myqueue = MessageQueue(host='myhost', port='', mq_username='myuser', mq_password='mypasswd')
myqueue.send(data_str, 'noti_chan', 'content_info')
