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
import time
from random import randint
from mq import MessageQueue
"""测试发送延时信息
"""

myqueue = MessageQueue(host='myhost', port='', mq_username='myuser', mq_password='mypasswd')

for nn in range(10):
    seed = randint(0, 5) * 1000
    in_data = {"content": "测试一下消息队列发送_" + str(nn) + '_' + str(seed)}
    data_str = json.dumps(in_data, ensure_ascii=False)
    myqueue.send(data_str, 'noti_chan_delay', 'content_info', delay=seed)
