#%%
from kafka import KafkaConsumer as Consumer

kafka_servers = ["alikafka-pre-cn-0pp0wshvu00b-1-vpc.alikafka.aliyuncs.com:9092",
                 "alikafka-pre-cn-0pp0wshvu00b-2-vpc.alikafka.aliyuncs.com:9092",
                 "alikafka-pre-cn-0pp0wshvu00b-3-vpc.alikafka.aliyuncs.com:9092"]

topics = ['PRE_PS_C_PRO']

group_id = "GID_LANGZHONG_TEST"

consumer = Consumer(*topics,
                         group_id=group_id,
                         bootstrap_servers=kafka_servers)

# 从Kafka拉取消息
for message in consumer:
    #print(message.value)
    print(f"Key: {message.key}, Value: {message.value}")
# %%
from confluent_kafka import Consumer, KafkaException

# Kafka集群配置
conf = {
    'bootstrap.servers': 'alikafka-pre-cn-0pp0wshvu00b-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-3-vpc.alikafka.aliyuncs.com:9092',
    'group.id': 'GID_LANGZHONG_TEST',
    'auto.offset.reset': 'earliest'
}

# 创建Kafka Consumer实例
consumer = Consumer(conf)

# 订阅特定的topic
consumer.subscribe(['PRE_PS_C_PRO'])

try:
    while True:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                print('%% %s [%d] reached end at offset %d\n' %
                      (message.topic(), message.partition(), message.offset()))
            elif message.error():
                raise KafkaException(message.error())
        else:
            # Process the message
            print('Received message: {}'.format(message.value().decode('utf-8')))

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
# %%
