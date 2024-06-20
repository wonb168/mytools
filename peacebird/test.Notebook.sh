ssh root@10.211.55.3 -p 23422
# password
Lanzhong@p3
# gpadmin
su - gpadmin
# sudo python3 -m harlequin "p_rst_ra_skc_org_detail.duckdb"
# python
python3 -V
sudo python3
import duckdb
dd=duckdb.connect("p_rst_ra_skc_org_detail.duckdb")
# update gp.tenant_peacebird_biz.rst_ra_sku_org_detail a set
# compute_status='0'
# from tmp_distinct_order_id b
# where a.skc_order_id=b.skc_order_id
# and a.day_date = '{day_date}' and a.is_deleted = '0'
day_date='2024-04-29'
sql=f"select skc_order_id,count(*) from tmp_distinct_order_id a where a.day_date = '{day_date}' and a.is_deleted = '0' group by skc_order_id having count(*)>1"
sql
dd.sql(sql)
dd.sql("select * from duckdb_tables()")