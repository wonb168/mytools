
drop table if exists tmp_base_update_org;
create temp table tmp_base_update_org as
select operating_unit_sk,org_sk
from gp.public.base_update_org
where day_date='2024-03-28'
;


--获取组织-商品skc标签表
drop table if exists tmp_base_org_skc_tag;
create temp table tmp_base_org_skc_tag as --184s
select a.*
from gp.tenant_{tenant}_edw.base_org_skc_tag a
inner join tmp_base_update_org b
on a.operating_unit_sk=b.operating_unit_sk
and a.org_sk=b.org_sk
;

--获取组织标签表
drop table if exists tmp_base_org_tag;
create temp table tmp_base_org_tag as
select a.*
from tenant_{tenant}_edw.base_org_tag a
inner join tmp_base_update_org b
on a.operating_unit_sk=b.operating_unit_sk
and a.org_sk=b.org_sk
;
# # 进远程duckdb cli
# 
dd.execute("detach gp")
ssh gpadmin@192.168.200.75
./duckdb
install postgres;

load postgres;
ATTACH 'postgresql://etl_hg_dev:j5qwqc1rwi9cxEVf@192.168.200.75:2345/mdmaster_hg_dev' AS gp (TYPE postgres);

# # 执行sql
create table tmp_sumary as
UNPIVOT monthly_sales
ON COLUMNS(* EXCLUDE (empid, dept))
INTO
    NAME month
    VALUE sales;
ssh gpadmin@192.168.200.101
ssh gpadmin@192.168.200.82
ssh gpadmin@192.168.200.82 #centos8

./duckdb
install postgres;


load postgres;
ATTACH 'postgresql://gpadmin:lzsj2021@192.168.200.101:2345/mdmaster_hg_test' AS gp (TYPE postgres);


# 计时
.timer on
create temp table tmp_base_update_org as
select operating_unit_sk,org_sk
from gp.public.base_update_org
where day_date=current_date
and org_flag=3
;--0.1s
select count(*) from tmp_base_update_org;
create temp table tmp_gto_skc_store_step_kpi_summary as
select
	a.store_skc_action_model_after_available_qty ,
	a.store_skc_action_model_before_available_qty ,
	a.store_skc_action_model_after_target_fill_count ,
	a.store_skc_action_model_before_target_fill_count ,
	a.store_skc_action_model_after_residual_size ,
	a.store_skc_action_model_before_residual_size ,
	a.store_skc_action_model_after_full_size ,
	a.store_skc_action_model_before_full_size ,
	a.store_skc_action_model_after_have_stock ,
	a.store_skc_action_model_before_have_stock ,
	a.store_skc_action_model_after_stock ,
	a.store_skc_action_model_before_stock ,
	a.store_skc_action_model_net_in_qty ,
	a.store_skc_action_model_allot_in_qty ,
	a.store_skc_action_model_allot_out_qty ,
	a.store_skc_action_model_replenish_distribution_qty ,
	a.step_id ,
	a.solution_id ,
	a.skc_sk ,
	a.store_sk ,
	a.operating_unit_sk ,
	a.day_date ,
	d.store_skc_14_0_have_sales ,
	d.store_skc_7_0_have_sales ,
	d.store_skc_last_14_7_day_sale_qty ,
	d.store_skc_last_7_0_day_sale_qty ,
	e.org_sk ,
	e.org_name as solution_org_hierarchy_name,
	e.step_name ,
	e.solution_name ,
	f.display_total_qty ,
	f.display_skc_qty ,
	f.clerk_count ,
	f.store_level_order ,
	f.org_flag ,
	f.parent_org_sk ,
	f.org_order ,
	f.is_custom ,
	f.org_hierarchy_order ,
	f.is_operating_unit ,
	f.org_type ,
	f.org_long_hierarchy_code ,
	f.supervisor ,
	f.goods_supervisor ,
	f.store_manager ,
	f.franchisee_code ,
	f.channel_type ,
	f.rent_type ,
	f.city_level ,
	f.region ,
	f.store_format ,
	f.store_category ,
	f.area_level ,
	f.physical_warehouse ,
	f.temperature_zone ,
	f.lng ,
	f.lat ,
	f.address ,
	f.area ,
	f.city ,
	f.province ,
	f.biz_district ,
	f.store_level ,
	f.business_type ,
	f.status ,
	f.org_long_name ,
	f.org_long_code ,
	f.org_long_sk ,
	f.org_long_hierarchy ,
	f.org_hierarchy_name ,
	f.org_hierarchy_code ,
	f.org_hierarchy_type_name ,
	f.org_hierarchy_type_code ,
	f.org_name ,
	f.org_code ,
	f.org_tree_name ,
	f.org_tree_code ,
	f.close_time ,
	f.opening_time ,
	f.rent_cost ,
	f.warehouse_area ,
	f.selling_area ,
	g.affiliation_year ,
	g.product_year ,
	g.product_sk ,
	g.display_pole_number ,
	g.develop_type ,
	g.designer ,
	g.order_level ,
	g.age_group ,
	g.price_band ,
	g.color_system ,
	g.virtual_suit ,
	g.affiliation_quarter ,
	g.sale_level_category ,
	g.sale_type ,
	g.suit ,
	g.gender ,
	g.band ,
	g.tiny_class ,
	g.mid_class ,
	g.big_class ,
	g.class_long_type ,
	g.class_long_code ,
	g.product_range ,
	g.color_name ,
	g.color_code ,
	g.product_quarter ,
	g.product_name ,
	g.product_code ,
	g.skc_name ,
	g.skc_code ,
	g.brand_name ,
	g.brand_code ,
	g.pull_off_date ,
	g.put_on_date ,
	g.tag_price ,
	g.cost_price ,
	c.store_skc_action_have_target_count ,
	b.store_action_model_send_package ,
	b.store_action_model_receive_package --select count(*) --4552932 select distinct day_date
from
	gp.tenant_{tenant}_adm.model_skc_store_step_kpi a  --按照组织覆盖
inner join gp.tenant_{tenant}_adm.gto_model_store_step_kpi b on a.operating_unit_sk = b.operating_unit_sk
	and a.store_sk = b.store_sk and a.step_id = b.step_id --and b.day_date='${day_date}'
inner join gp.tenant_{tenant}_adm.gto_skc_store_step_kpi c on a.operating_unit_sk = c.operating_unit_sk
	and a.store_sk = c.store_sk and a.skc_sk = c.skc_sk and a.step_id = c.step_id --and c.day_date='${day_date}'
inner join gp.tenant_{tenant}_adm.gto_skc_store_fct_kpi d on a.operating_unit_sk = d.operating_unit_sk and a.store_sk = d.store_sk
  and a.skc_sk = d.skc_sk and a.day_date = d.day_date --and d.day_date='${day_date}'
inner join gp.tenant_{tenant}_adm.gto_solution_info e on a.step_id = e.step_id --and e.day_date='${day_date}'
inner join gp.tenant_{tenant}_dm.dim_org_integration f on a.store_sk = f.org_sk and a.operating_unit_sk = f.operating_unit_sk
inner join gp.tenant_{tenant}_dm.dim_skc g on a.skc_sk = g.skc_sk
inner join tmp_base_update_org h on a.operating_unit_sk = h.operating_unit_sk and e.org_sk=h.org_sk
;--49s
drop table if exists tmp_dim;
create temp table tmp_dim as 
select operating_unit_sk,store_sk,skc_sk -- select count(*)
from tmp_gto_skc_store_step_kpi_summary 
group by operating_unit_sk,store_sk,skc_sk
;-- 0.1s

--获取组织-商品skc标签表
drop table if exists tmp_base_org_skc_tag;
create temp table tmp_base_org_skc_tag as
select b.*,a.tag_code,a.tag_value -- select *
from gp.tenant_{tenant}_edw.base_org_skc_tag a
inner join tmp_dim b
on a.operating_unit_sk=b.operating_unit_sk and a.org_sk=b.store_sk and a.skc_sk=b.skc_sk
;-- 908541 6.6s
--获取组织标签表
drop table if exists tmp_base_org_tag;
create temp table tmp_base_org_tag as
select b.*,a.tag_code,a.tag_value
from gp.tenant_{tenant}_edw.base_org_tag a
inner join tmp_dim b
on a.operating_unit_sk=b.operating_unit_sk and a.org_sk=b.store_sk
;-- 4799389 0.4s
--获取商品skc标签表
drop table if exists tmp_base_skc_tag;
create temp table tmp_base_skc_tag as
select b.*,a.tag_code,a.tag_value -- select *  
from gp.tenant_{tenant}_edw.base_skc_tag a
inner join tmp_dim b on a.skc_sk=b.skc_sk
;-- 8259984 0.9s
-- 合并所有标签
drop table if exists tmp_tag_all;
create temp table tmp_tag_all as
select * from tmp_base_skc_tag
union all
select * from tmp_base_org_tag
union all
select * from tmp_base_org_skc_tag
;-- Updated Rows	14514075
select count(*) from tmp_tag_all;--0.2s
select * from tmp_tag_all limit 3;
select count(*) from tmp_tag_all
-- tag 行转列(14514075行大表做行列转换)
drop table if exists tmp_tags;
create temp table tmp_tags as -- 
PIVOT tmp_tag_all
ON tag_code
USING max(tag_value) AS tag_value
group by operating_unit_sk, store_sk, skc_sk
;--1.7s

create table gp.tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck as 
select a.*,b.* exclude (operating_unit_sk, store_sk, skc_sk)
from tmp_gto_skc_store_step_kpi_summary a
left join tmp_tags b on a.operating_unit_sk=b.operating_unit_sk and a.store_sk=b.store_sk and a.skc_sk=b.skc_sk
;--392s

copy (select a.*,b.* exclude (operating_unit_sk, store_sk, skc_sk)
from tmp_gto_skc_store_step_kpi_summary a
left join tmp_tags b on a.operating_unit_sk=b.operating_unit_sk and a.store_sk=b.store_sk and a.skc_sk=b.skc_sk)
to 'tmp_gto_skc_store_step_kpi_summary.parquet' (format 'parquet');
copy gp.tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck2 from 'tmp_gto_skc_store_step_kpi_summary.parquet';
copy gp.tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck3 from 'tmp_gto_skc_store_step_kpi_summary.parquet';
insert into gp.tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck3 
select a.*,b.* exclude (operating_unit_sk, store_sk, skc_sk)
from tmp_gto_skc_store_step_kpi_summary a
left join tmp_tags b on a.operating_unit_sk=b.operating_unit_sk and a.store_sk=b.store_sk and a.skc_sk=b.skc_sk
;
select tag,memory_usage_bytes/1024/1024 from duckdb_memory();

ddl=dd.execute("select sql from duckdb_tables where table_name='t'").fetchone()[0]
print(ddl)
dd.execute('create table t(id int)')
ddl.replace('CREATE TABLE ', 'CREATE TABLE hg.').replace(';','dist')
ssh gpadmin@192.168.200.82 # B8ezntr0fiivl6GA
sudo /opt/python3.11/bin/python3.11
import duckdb as dd
print(dd.__version__)
dd.execute("install postgres")

dd.execute("detach gp")
dd.execute("load postgres_scanner;ATTACH 'postgresql://gpadmin:8ntK0Ppkct8sm45Z@192.168.200.82:2345/mdmaster_{tenant}_prod' AS gp (TYPE postgres);")
dd.execute("load postgres;ATTACH 'postgresql://gpadmin:8ntK0Ppkct8sm45Z@192.168.200.82:2345/mdmaster_{tenant}_prod' AS gp (TYPE postgres);")

dd.execute("load postgres;ATTACH 'dbname=mdmaster_hg_test user=gpadmin host=127.0.0.1 port=2345' AS gp (TYPE postgres);")

import time
t=time.time()
day_date='2024-04-03'
sql=f"""
create temp table tmp_base_update_org as
select operating_unit_sk,org_sk
from gp.public.base_update_org
where day_date='{day_date}'
and org_flag=3
;"""

dd.execute(sql)
dd.sql('select count(*) from tmp_base_update_org').fetchone()[0]
print(round(time.time()-t,0))
t=time.time()
tenant='hg'
sql=f"""
create temp table tmp_gto_skc_store_step_kpi_summary as
select
	a.store_skc_action_model_after_available_qty ,
	a.store_skc_action_model_before_available_qty ,
	a.store_skc_action_model_after_target_fill_count ,
	a.store_skc_action_model_before_target_fill_count ,
	a.store_skc_action_model_after_residual_size ,
	a.store_skc_action_model_before_residual_size ,
	a.store_skc_action_model_after_full_size ,
	a.store_skc_action_model_before_full_size ,
	a.store_skc_action_model_after_have_stock ,
	a.store_skc_action_model_before_have_stock ,
	a.store_skc_action_model_after_stock ,
	a.store_skc_action_model_before_stock ,
	a.store_skc_action_model_net_in_qty ,
	a.store_skc_action_model_allot_in_qty ,
	a.store_skc_action_model_allot_out_qty ,
	a.store_skc_action_model_replenish_distribution_qty ,
	a.step_id ,
	a.solution_id ,
	a.skc_sk ,
	a.store_sk ,
	a.operating_unit_sk ,
	a.day_date ,
	d.store_skc_14_0_have_sales ,
	d.store_skc_7_0_have_sales ,
	d.store_skc_last_14_7_day_sale_qty ,
	d.store_skc_last_7_0_day_sale_qty ,
	e.org_sk ,
	e.org_name as solution_org_hierarchy_name,
	e.step_name ,
	e.solution_name ,
	f.display_total_qty ,
	f.display_skc_qty ,
	f.clerk_count ,
	f.store_level_order ,
	f.org_flag ,
	f.parent_org_sk ,
	f.org_order ,
	f.is_custom ,
	f.org_hierarchy_order ,
	f.is_operating_unit ,
	f.org_type ,
	f.org_long_hierarchy_code ,
	f.supervisor ,
	f.goods_supervisor ,
	f.store_manager ,
	f.franchisee_code ,
	f.channel_type ,
	f.rent_type ,
	f.city_level ,
	f.region ,
	f.store_format ,
	f.store_category ,
	f.area_level ,
	f.physical_warehouse ,
	f.temperature_zone ,
	f.lng ,
	f.lat ,
	f.address ,
	f.area ,
	f.city ,
	f.province ,
	f.biz_district ,
	f.store_level ,
	f.business_type ,
	f.status ,
	f.org_long_name ,
	f.org_long_code ,
	f.org_long_sk ,
	f.org_long_hierarchy ,
	f.org_hierarchy_name ,
	f.org_hierarchy_code ,
	f.org_hierarchy_type_name ,
	f.org_hierarchy_type_code ,
	f.org_name ,
	f.org_code ,
	f.org_tree_name ,
	f.org_tree_code ,
	f.close_time ,
	f.opening_time ,
	f.rent_cost ,
	f.warehouse_area ,
	f.selling_area ,
	g.affiliation_year ,
	g.product_year ,
	g.product_sk ,
	g.display_pole_number ,
	g.develop_type ,
	g.designer ,
	g.order_level ,
	g.age_group ,
	g.price_band ,
	g.color_system ,
	g.virtual_suit ,
	g.affiliation_quarter ,
	g.sale_level_category ,
	g.sale_type ,
	g.suit ,
	g.gender ,
	g.band ,
	g.tiny_class ,
	g.mid_class ,
	g.big_class ,
	g.class_long_type ,
	g.class_long_code ,
	g.product_range ,
	g.color_name ,
	g.color_code ,
	g.product_quarter ,
	g.product_name ,
	g.product_code ,
	g.skc_name ,
	g.skc_code ,
	g.brand_name ,
	g.brand_code ,
	g.pull_off_date ,
	g.put_on_date ,
	g.tag_price ,
	g.cost_price ,
	c.store_skc_action_have_target_count ,
	b.store_action_model_send_package ,
	b.store_action_model_receive_package --select count(*) --4552932 select distinct day_date
from
	gp.tenant_{tenant}_adm.model_skc_store_step_kpi a  --按照组织覆盖
inner join gp.tenant_{tenant}_adm.gto_model_store_step_kpi b on a.operating_unit_sk = b.operating_unit_sk
	and a.store_sk = b.store_sk and a.step_id = b.step_id 
inner join gp.tenant_{tenant}_adm.gto_skc_store_step_kpi c on a.operating_unit_sk = c.operating_unit_sk
	and a.store_sk = c.store_sk and a.skc_sk = c.skc_sk and a.step_id = c.step_id 
inner join gp.tenant_{tenant}_adm.gto_skc_store_fct_kpi d on a.operating_unit_sk = d.operating_unit_sk and a.store_sk = d.store_sk
  and a.skc_sk = d.skc_sk and a.day_date = d.day_date 
inner join gp.tenant_{tenant}_adm.gto_solution_info e on a.step_id = e.step_id 
inner join gp.tenant_{tenant}_dm.dim_org_integration f on a.store_sk = f.org_sk and a.operating_unit_sk = f.operating_unit_sk
inner join gp.tenant_{tenant}_dm.dim_skc g on a.skc_sk = g.skc_sk
inner join tmp_base_update_org h on a.operating_unit_sk = h.operating_unit_sk and e.org_sk=h.org_sk
;"""
dd.execute(sql)
print(round(time.time()-t,0))


t=time.time()
sql="""
drop table if exists tmp_dim;
create temp table tmp_dim as 
select operating_unit_sk,store_sk,skc_sk -- select count(*)
from tmp_gto_skc_store_step_kpi_summary 
group by operating_unit_sk,store_sk,skc_sk
;"""
dd.execute(sql)
sql=f"""
--获取组织-商品skc标签表
drop table if exists tmp_base_org_skc_tag;
create temp table tmp_base_org_skc_tag as
select b.*,a.tag_code,a.tag_value -- select *
from gp.tenant_{tenant}_edw.base_org_skc_tag a
inner join tmp_dim b
on a.operating_unit_sk=b.operating_unit_sk and a.org_sk=b.store_sk and a.skc_sk=b.skc_sk
;"""
dd.execute(sql)
sql=f"""
--获取组织标签表
drop table if exists tmp_base_org_tag;
create temp table tmp_base_org_tag as
select b.*,a.tag_code,a.tag_value
from gp.tenant_{tenant}_edw.base_org_tag a
inner join tmp_dim b
on a.operating_unit_sk=b.operating_unit_sk and a.org_sk=b.store_sk
;"""
dd.execute(sql)
sql=f"""
--获取商品skc标签表
drop table if exists tmp_base_skc_tag;
create temp table tmp_base_skc_tag as
select b.*,a.tag_code,a.tag_value -- select *  
from gp.tenant_{tenant}_edw.base_skc_tag a
inner join tmp_dim b on a.skc_sk=b.skc_sk
;"""
dd.execute(sql)
sql="""
-- 合并所有标签
drop table if exists tmp_tag_all;
create temp table tmp_tag_all as
select * from tmp_base_skc_tag
union all
select * from tmp_base_org_tag
union all
select * from tmp_base_org_skc_tag
;"""
dd.execute(sql)
sql="""
-- tag 行转列
drop table if exists tmp_tags;
create temp table tmp_tags as -- 
PIVOT tmp_tag_all
ON tag_code
USING max(tag_value) AS tag_value
group by operating_unit_sk, store_sk, skc_sk
;"""
dd.execute(sql)

sql="""
drop table if exists tmp_rst;
create table tmp_rst as 
select a.*,b.* exclude (operating_unit_sk, store_sk, skc_sk)
from tmp_gto_skc_store_step_kpi_summary a
left join tmp_tags b on a.operating_unit_sk=b.operating_unit_sk and a.store_sk=b.store_sk and a.skc_sk=b.skc_sk
;"""
dd.execute(sql)

print(time.time()-t)
dd.execute("drop table tmp_rst")
sql="""
drop table if exists tmp_rst;
create table tmp_rst as 
select a.*,b.* exclude (operating_unit_sk, store_sk, skc_sk)
from tmp_gto_skc_store_step_kpi_summary a
left join tmp_tags b on a.operating_unit_sk=b.operating_unit_sk and a.store_sk=b.store_sk and a.skc_sk=b.skc_sk
;"""
dd.execute(sql)
import time
t=time.time()
# 写入 gp tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck
sql=f"""
insert into gp.tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck
select * from tmp_rst
;"""
print(sql)
dd.execute(sql)
print(time.time()-t)
dd.execute("select * from tmp_rst limit 1").fetchone()#[0]
dd.execute("")

# 获取 rst 表的 ddl 并在 gp 中创建
sql="""select sql from duckdb_tables() where table_name like 'tmp_rst' and schema_name = 'main';"""
ddl=dd.execute(sql).fetchone()[0]
print(ddl)
ddl=ddl.replace('CREATE TABLE tmp_rst', f'CREATE TABLE tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck').replace('DOUBLE', 'numeric').replace(';',"""WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (skc_sk);""")
print(f"drop table if exists tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck;"+ddl)
dd.execute("force install postgres_scanner")
dd.execute("load postgres_scanner")
select count(*) from 'tmp_gto_skc_store_step_kpi_summary.parquet';
dd.__version__
dd.execute("CALL postgres_execute('gp', ddl);")
dd.execute("load postgres_scanner;ATTACH 'postgresql://gpadmin:8ntK0Ppkct8sm45Z@192.168.200.82:2345/mdmaster_{tenant}_prod' AS gp (TYPE postgres);")
import time
t=time.time()
# 写入 gp tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck
sql="""
insert into gp.tenant_{tenant}_adm.gto_skc_store_step_kpi_summary_duck
select * from tmp_rst
;"""
dd.execute(sql)
print(time.time()-t)