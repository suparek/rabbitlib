#encoding=utf-8
import os
import sys
import time
import logging
import requests
# import gevent
import json
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from mq import MessageQueue


def process(body):
    in_data = json.loads(body)
    content = in_data.get('content', '')
    print(content)


def main():
    myqueue = MessageQueue(host='myhost', port='', mq_username='myuser', mq_password='mypasswd')
    myqueue.declare_delay_exchange('noti_chan_delay', exchange_type='direct')
    myqueue.declare_queue('noti_qu_delay')
    myqueue.bind_exchange_queue('noti_qu_delay', 'noti_chan_delay', binding_key="content_info")
    suke = myqueue.consume('noti_qu_delay')
    for method, properties, body in suke:
        try:
            process(body)
        except Exception as e:
            print(e)
            print("error")
        else:
            myqueue.ack_message(method)
            # myqueue.rej_message(method)
    myqueue.run()


if __name__ == "__main__":
    main()
