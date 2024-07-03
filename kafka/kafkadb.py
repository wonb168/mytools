# -*- coding: utf-8 -*-
 
 
import json
import logging
 
from confluent_kafka import Consumer, Producer
from confluent_kafka import TopicPartition, KafkaError
 
log = logging.getLogger(__name__)
 
 
class KProducer(object):
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic
        self.settings = {
            #'bootstrap.servers': 'alikafka-pre-cn-0pp0wshvu00b-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-3-vpc.alikafka.aliyuncs.com:9092',
            "bootstrap.servers": "{0}:{1}".format(self.host, self.port),
            "compression.type": "gzip"
            #"value.serializer": lambda m: json.dumps(m).encode("ascii")
        }
        self.p = Producer(self.settings)
 
    def acked(self, err, msg):
        if err is not None:
            log.error("Failed to deliver message: %s", err)
        #else:
        #    print('Message produced: %s', msg.value().decode('utf-8'))
 
    def send_msg(self, data):
        try:
            #self.p.produce(self.topic, json.dumps(data).encode('utf-8'), callback=self.acked)
            self.p.produce(self.topic, json.dumps(data), callback=self.acked)
            self.p.flush(timeout=30)
        except Exception as e:
            log.error("Kafka send message fail: {}.".format(e))
 
 
class PConsumer(object):
    def __init__(self, host, port, topic, group_id, partition_count):
        self.host = host
        self.port = port
        self.topic = 'PRE_PS_C_PRO'
        self.group_id = 'GID_LANGZHONG_TEST'
        self.partition_count = partition_count
        conf = {
            'bootstrap.servers': 'alikafka-pre-cn-0pp0wshvu00b-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-3-vpc.alikafka.aliyuncs.com:9092',
            #"bootstrap.servers": "{0}:{1}".format(self.host, self.port),
            #"client.id": self.client_id,
            "group.id": self.group_id,
            #"enable.auto.commit": False,
            "enable.auto.commit": True,
            "heartbeat.interval.ms": 3000,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 30000,
            #"max.poll.interval.ms": 86400000,
            #"auto.offset.reset": "latest",
            "auto.offset.reset": "smallest",
            "compression.type": "gzip",
            "message.max.bytes": 10485760
        }
        self.cons = Consumer(conf)
        #self.cons.subscribe([self.topic])
        partition = [TopicPartition(self.topic, x) for x in range(int(self.partition_count))]
        self.cons.assign(partition)
 
    def consumerMessage(self):
        while True:
            msg = self.cons.poll(timeout=1.0)
            if msg == None:
                continue
            if not msg.error() is None:
                log.error("msg error: {}".format(msg.error()))
                continue
            else:
                try:
                    value = json.loads(msg.value())
                    #self.cons.commit(message=msg,asynchronous=True)
                except Exception as e:
                    log.error("consumerMessage error: {0}, {1}".format(e, e.__traceback__.tb_lineno))
 
    def close(self):
        try:
            self.cons.close()
        except Exception as e:
            log.error("close error: {0}, {1}".format(e, e.__traceback__.tb_lineno))

    