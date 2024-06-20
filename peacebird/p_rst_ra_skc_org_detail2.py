#%%
import duckdb 
import time
dd=duckdb.connect('p_rst_ra_skc_org_detail.duckdb')
#dd=duckdb.connect()
dd.execute("SET pg_experimental_filter_pushdown=true;")
def exesql(sql): # plpython 用
   t=time.time()
   #print(sql)
   print('\n'.join(sql.split('\n')[:2]))
   dd.execute(sql)
   print('cost:',round(time.time()-t,2))
#%%
# ATTACH 'dbname=mdmaster_peacebird_uat2 user=gpadmin host=127.0.0.1 port=2345' AS gp (TYPE postgres);
# rv = plpy.execute("SELECT current_database() as db")
# dbname=rv[0]['db']
# plpy.notice(dbname)
day_date='2024-04-29'
dbname="mdmaster_peacebird_uat2"	#后面改动态
tenant=dbname.split('_')[1]
dburl=f"dbname={dbname} user=gpadmin host=127.0.0.1 port=2345"
sql=f"load postgres;ATTACH '{dburl}' AS gp (TYPE postgres);"
exesql(sql)	

#%%

sql=f"""   
SELECT GROUP_CONCAT(skc_order_id, ',') from tmp_distinct_order_id
--id from (select * FROM tmp_distinct_order_id limit 3)t;    
"""
ids=dd.execute(sql).fetchall()[0][0]
#%%
sql=f"""update gp.tenant_peacebird_biz.rst_ra_sku_org_detail a 
set	compute_status='0'
    from tmp_distinct_order_id b
    where a.skc_order_id=b.skc_order_id
    and a.day_date = '{day_date}' and a.is_deleted = '0'
    ;"""
day_date='2024-04-29'
sql=f"""call postgres_execute('gp',$$
create temp table tmp_distinct_order_id as
select unnest(string_to_array('{ids}',',')) skc_order_id;
update tenant_peacebird_biz.rst_ra_sku_org_detail a 
    set  compute_status='0'
    from tmp_distinct_order_id b
    where a.skc_order_id=b.skc_order_id
    and a.day_date = '2024-04-29' and a.is_deleted = '0';
$$);"""
exesql(sql)	
# dd.execute(sql)
#%%  
sql="""
create temp table gp.tmp_distinct_order_id as
select * from tmp_distinct_order_id"""
#dd.execute(sql)
