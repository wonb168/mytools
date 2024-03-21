
# %%
%%time
import psycopg
import time
# 核心命令
# with psycopg.connect(dsn_src) as conn1, psycopg.connect(dsn_tgt) as conn2:
#     with conn1.cursor().copy("COPY tenant_snowbeer_ods.dmf_op_customer_salesplan_share_mi TO STDOUT (FORMAT BINARY)") as copy1:
#         with conn2.cursor().copy("COPY tenant_snowbeer_ods.dmf_op_customer_salesplan_share_mi FROM STDIN (FORMAT BINARY)") as copy2:
#             for data in copy1:
#                 copy2.write(data)
#%%                    
def sync_table(table,dsn_src,dsn_tgt,batch_ids=''):
    if batch_ids=='':
        sql_copy=f"COPY {table} TO STDOUT (FORMAT BINARY)"
    else:
        sql_copy=f"""
copy (select * from {table} 
{"where batch_id in(" +batch_ids+ ")" if batch_ids else ""}) TO STDOUT (FORMAT BINARY)"""  
        # print(sql_copy)
    with psycopg.connect(dsn_src) as conn1, psycopg.connect(dsn_tgt) as conn2:
        conn2.cursor().execute(f"truncate {table}")
        with conn1.cursor().copy(sql_copy) as copy1:
            with conn2.cursor().copy(f"COPY {table} FROM STDIN (FORMAT BINARY)") as copy2:
                for data in copy1:
                    copy2.write(data)                     
#%%
def sync_tables(dsn_src,dsn_tgt,schema,black_list,batch_ids=''):
    black_list_str = "','".join(black_list)
    if black_list_str:
        black_list_str = "'" + black_list_str + "'"
    sql=f"""with b as(select table_schema, table_name,1 batch_id 
FROM information_schema.columns
WHERE column_name = 'batch_id' and data_type='integer' AND table_schema='{schema}') 
select a.schemaname||'.'||a.relname tb ,coalesce (b.batch_id,0) batch_id
from pg_stat_user_tables a
left join b on a.schemaname=b.table_schema and a.relname=b.table_name
where a.schemaname='{schema}' and a.relname not like '%_1_prt_%' --去分区子表
--and a.relname>='dmf_op_customer_salesplan_share_mi'
and a.relname not like '%_bak'
{"and a.relname not in(" +black_list_str+ ")" if black_list_str else ""} --黑名单
order by 1
"""
    print(sql)
    with psycopg.connect(dsn_src) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for row in cur:
                t=time.time()
                table_name=row[0]
                print(table_name, 'begin...')
                if row[1]>0:
                    #print('copy table with batch_id')
                    sync_table(table_name,dsn_src,dsn_tgt,batch_ids)
                else:
                    #print('copy table without batch_id')
                    sync_table(table_name,dsn_src,dsn_tgt,'')
                
                print("cost: ",round(time.time()-t))
#%%    
#密码包含@要转成%40                                    
dsn_src="postgresql://ur_0_uown_crb_edw_scp:UOwn_scp2023%40$!@10.204.128.93:10086/crb_edw_scp"
dsn_tgt="postgresql://ur_0_uown_crb_edw_scp:UOwn_scp2023%40$!@10.207.64.41:2345/crb_edw_scp"  
#%%
%%time
schema="tenant_snowbeer_ods"
black_list = ["fct_stock_move", "ods_ocms_product_invoiced_quantity_v","特殊发酵液"]
sync_tables(dsn_src,dsn_tgt,schema,black_list)
# %%            
# %%time
# schema="tenant_snowbeer_biz"
# black_list = ["users_record", "sp_demand_plan_detail",""]
# sync_tables(dsn_src,dsn_tgt,schema,black_list,batch_ids='161')

# %%
# 单表copy
# t="tenant_lansheng5_biz.rst_ra_skc_org_detail"
# sql_copy=f"COPY (select * from {t} where day_date='2024-03-20') TO STDOUT (FORMAT BINARY)"
# print(sql_copy)
# with psycopg.connect(dsn_src) as conn1, psycopg.connect(dsn_tgt) as conn2:
#     with conn1.cursor().copy(sql_copy) as copy1:
#         with conn2.cursor().copy(f"COPY {t} FROM STDIN (FORMAT BINARY)") as copy2:
#             for data in copy1:
#                 copy2.write(data)