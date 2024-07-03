import asyncio
import uvloop
import json
 
from kafkadb import PConsumer
import psycopg2

conn=psycopg2.connect(database="postgres", user="linezone", password="123456", host="127.0.0.1", port="5432")
 
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
#loop = asyncio.get_event_loop()
loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)


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
 
 
#@asyncio.coroutine
def analysis(protocol, rules, data, kproducer):
    # samething
    id,sql1,sql2,type=json2sql(data)
    with conn.cursor as cur:
        cur.execute(sql1)
        conn.commit()
    print("result1111111-----------------------: ", result)
    kproducer.send_msg(copy.deepcopy(result))
 
async def start(self):
    # samething
    while True:
        message = self.pconsumers.cons.poll(timeout=1.0)
 
        if message == None:
            continue
 
        if not message.error() is None:
            log.error("Message error: {}".format(message.error()))
            continue
 
        data = json.loads(message.value())
        await analysis(protocol, rules, data, self.kproducer)
 
def main():
    loop.run_until_complete(start())
 
 
if __name__ == "__main__":
    main()