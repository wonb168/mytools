
# %%
%%time
import psycopg
import time
# 核心命令 UOwn_scp2023@$!
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
def get_tables(dsn_src,dsn_tgt,schema,black_list,batch_ids=''):
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
and a.relname not like '%_bak' 
and a.relname not like '%_tmp' 
and a.relname not like 'tmp%'
{"and a.relname not in(" +black_list_str+ ")" if black_list_str else ""} --黑名单
order by 1
"""
    print(sql)
    with psycopg.connect(dsn_src) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
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
--and a.relname>='fct_sku_stock_onroad'--'dmf_op_customer_salesplan_share_mi'
and a.relname not like '%_bak'
 and a.relname not like '%_tmp'
 and a.relname not like 'tmp%'
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
dsn_src="postgresql://ur_0_uown_crb_edw_scp:UOwn_scp2023%40$!@10.207.64.41:2345/crb_edw_scp"  
dsn_tgt="postgresql://demo:lzsj2015@192.168.200.101:2345/sop_demo"  
#%%
#%%
"""
tenant_snowbeer_adm
tenant_snowbeer_biz
tenant_snowbeer_dm
tenant_snowbeer_edw
tenant_snowbeer_ods --最先同步，其他等批次
tenant_snowbeer_ods_std
tenant_snowbeer_sys
"""
schema="tenant_snowbeer_ods" # 
#schema="public"
black_list = ["fct_stock_move", "ods_ocms_product_invoiced_quantity_v","特殊发酵液","users_record", "sp_demand_plan_detail"]
# schema="tenant_snowbeer_adm"
rs=get_tables(dsn_src,dsn_tgt,schema,black_list)
print(rs)
#%% 

#%%
batch_ids='194' # 支持多个，逗号隔开：'194,195'

#%%
for row in rs:
    t=time.time()
    table_name=row[0]
    print(table_name, 'begin...')
    # if row[0]<=f'{schema}.maint_production_switch_time' :
    #     print('skip')
    #     continue 
    if row[1]>0:
        print('copy table with batch_id')
        sync_table(table_name,dsn_src,dsn_tgt,batch_ids)
    else:
        print('copy table without batch_id')
        sync_table(table_name,dsn_src,dsn_tgt,'')
    print("cost: ",round(time.time()-t))
print('end ............................................')

# %%
