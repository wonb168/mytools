import psycopg2

from project_code.ods.kafka_tools import sql_params
from project_code.ods.kafka_tools import kafka_conf
from project_code.ods.kafka_tools import kafka_consumer

conn = psycopg2.connect(database="mdmaster_fastfish_dev", user="etl_fastfish_dev", password="oFkRt1CkVk5fJ6R8",
                        host="10.1.11.62", port="2345")


sql_params['table'] = 'sc_b_stock'
# 配置主题
topic = ['SC_B_STOCK']
kafka_conf['enable.auto.commit'] = 'false'

kafka_consumer(conn, topic, kafka_conf)
