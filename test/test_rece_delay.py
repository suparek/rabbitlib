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
"""测试监听接收延时信息
"""


def _notify_dingding(content, ding_urls):
    if not ding_urls:
        return None
    data = {
        'msgtype': 'text',
        'text': {
            'content': content,
        },
    }
    for ding_url in ding_urls:
        try:
            start = time.time()
            resp = requests.post(ding_url, json=data, timeout=5)
        except requests.exceptions.RequestException as ex:
            logging.exception("push dingtalk error: %s, params: %s", ex, data)
            return
        finally:
            end = time.time()
            logging.info("push dingtalk cost: %f, params: %s", end - start, data)
        ret = resp.json()
        logging.info("push dingtalk result: %s", resp.content)
        if ret['errcode'] != 0:
            logging.error("push dingtalk failed, response: %s, params: %s", resp.content, data)


def process(body):
    in_data = json.loads(body)
    content = in_data.get('content', '')
    ding_urls = in_data.get('ding_urls', ())
    print(content, ding_urls)
    # _notify_dingding(content, ding_urls)


def main():
    myqueue = MessageQueue(host='myhost', port='', mq_username='myuser', mq_password='mypasswd')
    myqueue.declare_delay_exchange('noti_chan_delay', exchange_type='direct')
    myqueue.declare_queue('noti_qu_delay')
    myqueue.bind_exchange_queue('noti_qu_delay', 'noti_chan_delay', binding_key="content_info")
    while 1:
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
                import random
                if random.random() <= 0.1:
                    myqueue.create_kk = 1
                    myqueue.connection.close()
                    myqueue.create_kk = 0
        print("结束了！")
        myqueue.run()


if __name__ == "__main__":
    main()
