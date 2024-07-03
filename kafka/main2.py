import asyncio
import uvloop
import json
import copy

from kafkadb import KProducer, PConsumer
import psycopg2

conn=psycopg2.connect(database="postgres", user="linezone", password="123456", host="127.0.0.1", port="5432")
 
def json2sql(json_data):

    data = json.loads(json_data.replace('"[', '[').replace(']"', ']').replace('"{', '{').replace('}"', '}'))

    table = data['table']
    # 提取数据
    table = data['table']
    type=data['type']
    print('type:',type)
    sql_query2=""
    if type=='INSERT':
        insert_values = data['data']
        all_values = list(insert_values.values())
        # 生成SQL语句
        sql_columns = ', '.join(insert_values.keys())
        sql_values = "', '".join(str(value) for value in all_values)
        #sql_values = "', '".join(str(insert_values.values()))

        sql_query = "INSERT INTO {} ({}) VALUES ('{}');".format(table, sql_columns, sql_values)

    elif type=='UPDATE':
        update_values = data['data']
        old_values = data['oldData']
        condition = data['shardingKey']

        # 生成SQL语句
        sql_query = "UPDATE {} SET ".format(table)
        set_values = ', '.join(["{} = '{}'".format(key, value) for key, value in update_values.items() if value != old_values.get(key)])
        sql_query += set_values + " WHERE ID = '{}';".format(condition)
        
        sql_columns = ', '.join(update_values.keys())
        #sql_values = "', '".join(update_values.values())
        all_values = list(update_values.values())
        sql_values = "', '".join(str(value) for value in all_values)

        sql_query2 = "INSERT INTO {} ({}) VALUES ('{}');".format(table, sql_columns, sql_values)
    
    elif type=='DELETE':
        condition = data['shardingKey']
        sql_query = "DELETE FROM {} WHERE ID = '{}';".format(table, condition)
    
    return condition,sql_query,sql_query2,type 

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
 
async def analysis(protocol, rules, data, kproducer):
    # Perform analysis
    result = "Some result"
    print("result1111111-----------------------: ", data)
    id,sql,sql2,ttype=json2sql(data)
    with conn.cursor() as cur:
        if ttype=='UPDATE':
            cur.execute(f"SELECT 1 FROM ps_c_pro WHERE id = {id};")
            result = cur.fetchone()
        # 检查结果
        if not result:
            print('not exist')
            sql=sql2
        cur.execute(sql)
        # 提交事务
        conn.commit()

    kproducer.send_msg(copy.deepcopy(result))

async def start():
    # Initialization steps
    #kproducer = KProducer()
    pconsumers = PConsumer(1,2,'PRE_PS_C_PRO','GID_LANGZHONG_TEST',4)
    protocol = "some_protocol"
    rules = "some_rules"

    while True:
        message = pconsumers.cons.poll(timeout=1.0)

        if message is None:
            continue

        if message.error() is not None:
            log.error(f"Message error: {message.error()}")
            continue

        data = json.loads(message.value())
        await analysis(protocol, rules, data, kproducer)

def main():
    loop.run_until_complete(start())

if __name__ == "__main__":
    main()