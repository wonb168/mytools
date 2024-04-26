import duckdb




sql="select * from '/home/gpadmin/public.kpi.parquet'"
dd.sql(sql)
sql2=f"call postgres_execute('gp',$$create view public.test_view as {sql}$$)"
print(sql2)
dd.execute(sql2)
# # 连服务器
ssh gpadmin@192.168.200.75
# # 进python
python3.9
# # duckdb
import duckdb
dd=duckdb.connect('parquet.duckdb')
dbname="mdmaster_hggp7_dev"	#后面改动态
tenant=dbname.split('_')[1]
dburl=f"dbname={dbname} user=gpadmin host=127.0.0.1 port=2345"
sql=f"load postgres;ATTACH '{dburl}' AS gp (TYPE postgres);SET pg_experimental_filter_pushdown=true;"
dd.execute(sql)
# # 测试

sql="select * from duckdb_databases"
dd.sql(sql)
# # exesql
from icecream import ic
import time
def exesql(sql):
   t=time.time()
   print(sql)
   dd.execute(sql)
   print('cost:',round(time.time()-t,2))
# # 大表对比测试
day_date='2024-04-23'
sql=f"""
    drop table if exists tmp_ra_sku_org_data_pre;
    create table tmp_ra_sku_org_data_pre as
    select  a.*
    from gp.tenant_hggp7_biz.rst_ra_sku_org_data a
    where a.day_date = '{day_date}'
    ;"""
exesql(sql)	