#encoding=utf-8
import time
import logging
import requests
import pika
import json
from pika.exceptions import StreamLostError, ChannelWrongStateError, ChannelClosedByBroker
from .credentials import AliyunCredentialsProvider

# https://github.com/suparek/RabbitMQClient/blob/master/rabbitmqclient.py


class MyQueue(object):
    def __init__(self, queue_name):
        self.queue_name = queue_name

    def get_items(self, num, status):
        raise NotImplementedError

    def ack(self, id, status):
        raise NotImplementedError

    def add(self, status, payload, priority=0):
        raise NotImplementedError


class RabbitQueue(MyQueue):
    """用rabbitmq生成的queue
    """

    __default_routing_key = "#"
    __default_exchange_type = "direct"

    def __init__(self,
                 host,
                 port,
                 mq_username="",
                 mq_password="",
                 virtualHost="",
                 accessKey="",
                 accessSecret="",
                 instanceId="",
                 heartbeat=None):
        self.connect_params = self.getConnectionParam(host, port, mq_username, mq_password, virtualHost, accessKey,
                                                      accessSecret, instanceId, heartbeat)
        self.create_connection()

    def __getattribute__(self, attr):
        """每次使用connection，channel的时候都要检查一下是否已经断开连接了
        """
        if attr in ("connection", "channel"):
            try:
                connection = object.__getattribute__(self, "connection")
                channel = object.__getattribute__(self, "channel")
                connect_params = object.__getattribute__(self, "connect_params")
                try:
                    connection.process_data_events()
                except (StreamLostError, ChannelWrongStateError, ValueError, TypeError):
                    logging.debug("队列连接已断开或发生错误！")
                if connection.is_closed or channel.is_closed:
                    new_con = pika.BlockingConnection(connect_params)
                    new_chan = new_con.channel()
                    object.__setattr__(self, "connection", new_con)
                    object.__setattr__(self, "channel", new_chan)
                    logging.debug("重新连接了队列！")
            except AttributeError:
                pass
        return object.__getattribute__(self, attr)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def create_connection(self):
        self.connection = pika.BlockingConnection(self.connect_params)
        self.channel = self.connection.channel()

    def getConnectionParam(self,
                           host,
                           port,
                           mq_username="",
                           mq_password="",
                           virtualHost="",
                           accessKey="",
                           accessSecret="",
                           instanceId="",
                           heartbeat=None):
        if accessKey:
            provider = AliyunCredentialsProvider(accessKey, accessSecret, instanceId)
            credentials_param = pika.PlainCredentials(
                provider.get_username(), provider.get_password(), erase_on_connect=True)
            params = {"host": host, "credentials": credentials_param, "heartbeat": heartbeat}
            if virtualHost:
                params['virtual_host'] = virtualHost
            if port:
                params['port'] = port
            return pika.ConnectionParameters(**params)
        else:
            params = {"host": host, "heartbeat": heartbeat}
            if mq_username:
                credentials_param = pika.PlainCredentials(mq_username, mq_password, erase_on_connect=True)
                params["credentials"] = credentials_param
            if port:
                params['port'] = port
            return pika.ConnectionParameters(**params)

    def close_connection(self):
        self.connection.close()

    def declare_exchange(self, exchange, **kwargs):
        exchange_type = kwargs.get('exchange_type', 'direct')
        durable = kwargs.get('durable', True)
        x_delayed_type = kwargs.get('x-delayed-type')
        arguments = {}
        arguments = self.arg_set('x-delayed-type', x_delayed_type, arguments)
        self.channel.exchange_declare(
            exchange=exchange, exchange_type=exchange_type, durable=durable, arguments=arguments)

    def declare_delay_exchange(self, exchange, **kwargs):
        """
        延时队列声明，兼容 rabbitmq_delayed_message_exchange 和 阿里云
        如果要使用延时消息发送，请使用 这个来声明队列
        """
        exchange_type = kwargs.get('exchange_type', 'direct')
        durable = kwargs.get('durable', True)
        arguments = {}
        arguments = self.arg_set('x-delayed-type', exchange_type, arguments)
        try:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type='x-delayed-message', durable=durable, arguments=arguments)
        except ChannelClosedByBroker as e:
            logging.warning(e)
            self.create_connection()
            self.channel.exchange_declare(
                exchange=exchange, exchange_type=exchange_type, durable=durable, arguments=arguments)

    def delete_exchange(self, exchange):
        self.channel.exchange_delete(exchange=exchange)

    def arg_set(self, key, value, arguments):
        if value:
            arguments[key] = value
        return arguments

    def declare_queue(self, queue, **kwargs):
        durable = kwargs.get('durable', True)
        priority = kwargs.get('priority', 10)
        ttl_milseconds = kwargs.get('ttl_milseconds')
        dead_letter_exchange = kwargs.get('dead_letter_exchange')
        dead_letter_routing_key = kwargs.get('dead_letter_routing_key')
        arguments = {'x-max-priority': priority}
        arguments = self.arg_set('x-message-ttl', ttl_milseconds, arguments)
        arguments = self.arg_set('x-dead-letter-exchange', dead_letter_exchange, arguments)
        arguments = self.arg_set('x-dead-letter-routing-key', dead_letter_routing_key, arguments)
        self.channel.queue_declare(queue=queue, durable=durable, arguments=arguments)

    def delete_queue(self, queue):
        self.channel.queue_delete(queue=queue)

    def bind_exchange_queue(self, queue, exchange, binding_key=__default_routing_key):
        self.channel.queue_bind(queue=queue, exchange=exchange, routing_key=binding_key)

    def send(self, message, exchange, routing_key, **kwargs):
        """
        kwargs 参数说明：
        message_id是用户指定的消息id，为空则使用系统自动生成的。
        delay是延时消息参数，单位是ms
        priority是消息优先级
        expiration是消息生存周期，与delay相冲突，如果使用了expiration，delay就无效了
        """
        message_id = kwargs.get('message_id')
        expiration = kwargs.get('expiration')
        close_connection = kwargs.get('close_connection', False)
        priority = kwargs.get('priority', 0)
        delay = kwargs.get('delay')
        property_params = {"content_type": "application/json"}
        headers = {}
        property_params = self.arg_set('message_id', message_id, property_params)
        if not delay:
            # 如果要发送延时信息，就不要使用 生存周期了
            property_params = self.arg_set('expiration', expiration, property_params)
        property_params = self.arg_set('priority', priority, property_params)

        # delay 是 aliyun，x-delay是 rabbitmq_delayed_message_exchange ，所以都加上了
        headers = self.arg_set('delay', delay, headers)
        headers = self.arg_set('x-delay', delay, headers)

        property_params = self.arg_set('headers', headers, property_params)
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(**property_params))
        if close_connection:
            self.close_connection()

    def consume(self, queue, auto_ack=False, inactivity_timeout=None, prefetch_count=None):
        if prefetch_count:
            self.channel.basic_qos(prefetch_count=prefetch_count)
        result = self.channel.consume(queue, auto_ack, inactivity_timeout=inactivity_timeout)
        for nn in result:
            yield nn

    def run(self):
        try:
            self.channel.start_consuming()
        finally:
            self.channel.stop_consuming()
            self.close_connection()

    def ack_message(self, method):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def rej_message(self, method, requeue=False):
        self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=requeue)
