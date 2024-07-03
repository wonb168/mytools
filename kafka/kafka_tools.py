"""
-------------------------------------------------
    File Name:      kafka_tools.py
    Description:
    Author:         yingshengfeng
    date:           2000年01月01日
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import json
import asyncio
import pandas

from confluent_kafka import Consumer, KafkaException, KafkaError

tenant_prefix = 'tenant_fastfish_'

sql_params = {
    'tenant_prefix': tenant_prefix,
    'table': 'null',
}

kafka_servers_host = [
    '172.16.8.9:9092',
    '172.16.8.10:9092',
    '172.16.8.11:9092',
]

# Kafka集群配置
kafka_conf = {
    'bootstrap.servers': ','.join(kafka_servers_host),
    'group.id': 'GID_LANGZHONG_PRO',
    'auto.offset.reset': 'earliest',  # 生产环境应该设置为none模式，避免重复消费
    # 禁用自动提交偏移量, 手动控制偏移量提交，确保在消息成功处理后才提交偏移量。
    'enable.auto.commit': 'false',  # false + earliest 完全重新开始消费
}

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
 
async def analysis(data, consumer):
    
    id, sql, sql2, type = json2sql(msg, columns)
    if type == "UPDATE":
        conn_cursor.execute(f"""
            SELECT 1 FROM {sql_params['tenant_prefix']}ods.{sql_params['table']} WHERE id = {id};
        """)
        result = conn_cursor.fetchone()
        if not result:
            print('not exists...', flush=True)
            sql = sql2
    print(sql, flush=True)
    conn_cursor.execute(sql)
    conn.commit()
    # 手动提交偏移量
    consumer.commit()

def get_column(conn_cursor):
    column_sql = """
        select
            column_name 
        from information_schema.columns
        where table_name = '${table}'
            and table_schema = '${tenant_prefix}ods'
    """.replace('${', '{').format(**sql_params)
    conn_cursor.execute(column_sql)
    res = conn_cursor.fetchall()
    # print(cur.description)
    return pandas.DataFrame(res, columns=list(map(lambda _: _.name, conn_cursor.description)))


def column_filter(data_dict: dict, columns):
    return {k: v for k, v in data_dict.items() if k.lower() in columns}


def kafka_consumer(conn, topic, conf):
    conn_cursor = conn.cursor()

    # columns = get_column(conn_cursor)['column_name'].apply(lambda _: _.lower()).to_list()

    # print('table columns list: ', columns, flush=True)

    # 创建Kafka Consumer实例
    consumer = Consumer(conf)
    # 订阅特定的topic
    # if type(topic) is not list:
    #     topic = [str(topic)]

    print(topic, flush=True)
    consumer.subscribe(topic)
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    print('%% %s [%d] reached end at offset %d\n' % (
                        message.topic(),
                        message.partition(),
                        message.offset()
                    ), flush=True)
                elif message.error():
                    raise KafkaException(message.error())
            else:
                # Process the message
                msg = message.value().decode('utf-8')
                print(msg, flush=True)
                await analysis(msg,consumer)

    except KeyboardInterrupt as e:
        print(e, flush=True)

    finally:
        consumer.close()


def json2sql(json_data, columns):
    # data = json.loads(json_data.replace('"[', '[').replace(']"', ']').replace('"{', '{').replace('}"', '}'))
    # data = json.loads(r"{0}".format(json_data))
    data = json.loads(json_data)
    table = '{tenant_prefix}ods.'.replace('${', '{').format(**sql_params) + data['table']

    condition = data['shardingKey']
    data_filter = column_filter(data['data'], columns)
    print('filter_dict:', data_filter, flush=True)
    data_type = data['type']
    print('type:', data_type, flush=True)
    sql_query2 = ""

    if len(data_filter) == 0:
        return condition, 'select 1;', 'select 1;', data_type

    if data_type == 'INSERT':
        insert_values = data_filter
        all_values = list(insert_values.values())
        # 生成SQL语句
        sql_columns = ', '.join(insert_values.keys())
        sql_values = "$lz_kafka$, $lz_kafka$".join(str(value) for value in all_values)
        # sql_values = "$lz_kafka$, $lz_kafka$".join(str(insert_values.values()))

        sql_query = "DELETE FROM {} WHERE ID = $lz_kafka${}$lz_kafka$;".format(table, condition)
        sql_query += "\n" + "INSERT INTO {} ({}) VALUES ($lz_kafka${}$lz_kafka$);".format(table, sql_columns, sql_values)

    elif data_type == 'UPDATE':
        update_values = data_filter
        old_values = column_filter(data['oldData'], columns)

        # 生成SQL语句
        sql_query = "UPDATE {} SET ".format(table)

        update_list = ["{} = $lz_kafka${}$lz_kafka$".format(key, value) for key, value in update_values.items() if value != old_values.get(key)]
        set_values = ', '.join(update_list)
        sql_query += set_values + " WHERE ID = $lz_kafka${}$lz_kafka$;".format(condition)

        if len(update_list) == 0:
            sql_query = 'select 1;'

        sql_columns = ', '.join(update_values.keys())
        # sql_values = "$lz_kafka$, $lz_kafka$".join(update_values.values())
        all_values = list(update_values.values())
        sql_values = "$lz_kafka$, $lz_kafka$".join(str(value) for value in all_values)

        sql_query2 = "INSERT INTO {} ({}) VALUES ($lz_kafka${}$lz_kafka$);".format(table, sql_columns, sql_values)

    elif data_type == 'DELETE':
        sql_query = "DELETE FROM {} WHERE ID = $lz_kafka${}$lz_kafka$;".format(table, condition)

    return condition, sql_query, sql_query2, data_type
