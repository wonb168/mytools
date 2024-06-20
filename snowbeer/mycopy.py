#%%
import psycopg2
import time

#"postgresql://ur_0_uown_crb_edw_scp:UOwn_scp2023%40$!@10.207.64.41:2345/crb_edw_scp" 
#%%
def connect_db(db_name, user, password, host, port):
    return psycopg2.connect(database=db_name, user=user, password=password, host=host, port=port)

def pg2csv(pg_conn, sql, pg_table, csv_file):
    cur = pg_conn.cursor()
    with open(f'{pg_table}.csv', 'w') as f:
        cur.copy_expert((f"COPY ({sql}) TO STDOUT WITH CSV HEADER"), f)

def csv2pg(pg_conn, csv_file, pg_table):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"TRUNCATE TABLE {pg_table}")
    with open(f'{pg_table}.csv', 'r') as f:
        pg_cursor.copy_expert((f"COPY {pg_table} FROM STDIN WITH CSV HEADER"), f)
    pg_conn.commit()
    # with open(csv_file, 'r') as file:
    #     pg_cursor.copy_from(file, pg_table, sep=',')
    #     pg_conn.commit()

#%%
def pg2pg(pg_conn_src, pg_conn_dest, sql, pg_table):
    t=time.time()
    pg_cursor_src = pg_conn_src.cursor()
    pg_cursor_dest = pg_conn_dest.cursor()
    pg2csv(pg_conn_src, sql, pg_table,f'{pg_table}.csv')
    csv2pg(pg_conn_dest, f'{pg_table}.csv', pg_table)
    print(f"{pg_table} copied in {round(time.time()-t,2)} seconds")
# %%
pg_conn_src=connect_db('crb_edw_scp', 'ur_0_uown_crb_edw_scp', 'UOwn_scp2023@$!', '10.207.64.41', 2345)
pg_conn_dest=connect_db('sop_demo', 'demo', 'lzsj2015', '192.168.200.101', 2345)
#%%
#单表测试
sql="select * from tenant_snowbeer_adm.adm_dim_sku limit 10"
pg_table="tenant_snowbeer_adm.adm_dim_sku"
pg2pg(pg_conn_src, pg_conn_dest, sql, pg_table)

# %%
# 读取 csv 的表清单