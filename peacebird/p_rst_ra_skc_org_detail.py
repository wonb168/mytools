#%%
import duckdb 
import time
t0=time.time()
dd=duckdb.connect('p_rst_ra_skc_org_detail.duckdb')
#dd=duckdb.connect()
dd.execute("SET pg_experimental_filter_pushdown=true;")
def exesql(sql): # plpython 用
   t=time.time()
   #print(sql)
   print('\n'.join(sql.split('\n')[:3]))
   dd.execute(sql)
   print('cost:',round(time.time()-t,2))
#%%

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
#--0：未修改；1：已保存；2：已提交； 3：集货
sql=f"""
select * from postgres_query('gp',
$$select day_date
from tenant_peacebird_biz.rst_ra_sku_org_detail a
where a.day_date = '{day_date}' and a.is_deleted = '0'
and compute_status in ('1','2','3') limit 1 
;$$)"""
# 如果修改记录数=0则返回1，否则执行逻辑
rst=dd.execute(sql).fetchone()
if not rst:
   print('no data')
   exit()
   #return 'no data'

#%%
# day_date='2024-04-28'
sql=f"""
-- tmp_rst_ra_sku_org_detail 取sku表
drop table if exists tmp_rst_ra_sku_org_detail;
create  table tmp_rst_ra_sku_org_detail as
select * from postgres_query('gp',$$
select a.skc_order_id,
      a.model_allot_out_org_sk,a.model_allot_in_org_sk,
      a.human_allot_out_org_sk,a.human_allot_in_org_sk,
      a.scene_code,a.scene_name,a.remark,a.brand_code,
      a.ra_source,a.commit_status,a.modify_status,a.is_effective,a.reserved1,a.reserved9,
      a.skc_sk,
      a.sku_sk,
      a.size_code,
      a.commit_user_name,
      a.model_ra_qty,
      a.human_ra_qty,
      a.biz_action_template_code,
      a.biz_action_template_name,
      a.org_sk,
      a.job_id, --任务id
      a.task_status, --任务状态
      a.document_code, --需求单号
      a.batch_id,
      a.step_id, --步骤id
      a.day_date,
      a.compute_status
from tenant_peacebird_biz.rst_ra_sku_org_detail a
where a.day_date = '{day_date}' and a.is_deleted = '0'
;$$)"""
exesql(sql)	
# %%
dd.execute("select count(*) from tmp_rst_ra_sku_org_detail").fetchone()
# %%
sql=f"""--筛选目标sku
drop table if exists tmp_org_sku_distinct;
create table tmp_org_sku_distinct as
select distinct coalesce(human_allot_out_org_sk,'111') as human_allot_out_org_sk,coalesce(human_allot_in_org_sk,'111') as human_allot_in_org_sk,skc_sk
from tmp_rst_ra_sku_org_detail
where compute_status in ('1','2','3') --0：未修改；1：已保存；2：已提交； 3：集货
;"""
exesql(sql)	
# %%
sql=f"""--取出被更改状态的单据id 最后需要状态还原为0
drop table if exists tmp_distinct_order_id;
create table tmp_distinct_order_id as
select distinct skc_order_id
from tmp_rst_ra_sku_org_detail
where compute_status in ('1','2','3') --0：未修改；1：已保存；2：已提交； 3：集货
;"""
exesql(sql)	
# %%
#%%   
sql=f"""
    --tmp_org_skc_union 所需款店
    drop table if exists tmp_org_skc_union;
    create table tmp_org_skc_union as
    select org_sk,skc_sk
    from (
        select human_allot_out_org_sk as org_sk,skc_sk
        from tmp_org_sku_distinct
        union all
        select human_allot_in_org_sk as org_sk,skc_sk
        from tmp_org_sku_distinct
    ) aa
    group by org_sk,skc_sk
    ;"""
exesql(sql)	

# %%
sql=f"""
    --tmp_skc_union 所需款
    drop table if exists tmp_skc_union;
    create table tmp_skc_union as
    select skc_sk
    from tmp_org_skc_union
    group by skc_sk
    ;"""
exesql(sql)	

#%%
sql=f"""   --tmp_sku_target 获取相同款店的单据 --相同款店的单据都需要更新
    drop table if exists tmp_sku_target;
    create table tmp_sku_target as
    select a.skc_order_id,
      a.model_allot_out_org_sk,a.model_allot_in_org_sk,
      a.human_allot_out_org_sk,a.human_allot_in_org_sk,
      a.scene_code,a.scene_name,a.remark,a.brand_code,
      a.ra_source,a.commit_status,a.modify_status,a.is_effective,a.reserved1,a.reserved9,
      a.skc_sk,
      a.sku_sk,
      a.size_code,
      a.commit_user_name,
      a.model_ra_qty,
      a.human_ra_qty,
      a.biz_action_template_code,
      a.biz_action_template_name,
      a.org_sk,
      a.job_id, --任务id
      a.task_status, --任务状态
      a.document_code, --需求单号
      a.batch_id,
      a.step_id, --步骤id
      a.day_date
    from tmp_rst_ra_sku_org_detail a
    where exists ( select 1 from tmp_org_skc_union b where
    	(a.human_allot_out_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    	or (a.human_allot_in_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    	or (a.model_allot_out_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    	or (a.model_allot_in_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    )
--     inner join tmp_skc_union b on a.skc_sk=b.skc_sk
--     and a.day_date='{day_date}' --and a.ra_source in ('0','2')
--     and a.is_deleted = '0'"""
exesql(sql)	

#%%
sql=f"""    --获取人工目标款店 skc-org
    drop table if exists tmp_target_human_skc_org;
    create table tmp_target_human_skc_org as
    select org_sk,skc_sk
    from (
        select human_allot_out_org_sk as org_sk,skc_sk
        from tmp_sku_target
        union all
        select human_allot_in_org_sk as org_sk,skc_sk
        from tmp_sku_target
        union all
        select model_allot_out_org_sk as org_sk,skc_sk
        from tmp_sku_target
        union all
        select model_allot_in_org_sk as org_sk,skc_sk
        from tmp_sku_target
    ) aa
    group by org_sk,skc_sk
    ;"""
exesql(sql)	

#%%
sql=f"""--获取skc1-门店C --该部分skc-org组合使用biz.skc指标，skc1-门店A、B库存指标重新计算
    drop table if exists tmp_skc_org_store_c; --作为后续门店C指标主表
    create table tmp_skc_org_store_c as
    select a.skc_sk,a.org_sk
    from tmp_target_human_skc_org a --与a表A、B门店匹配不上
    left join tmp_org_skc_union b on a.skc_sk=b.skc_sk and a.org_sk=b.org_sk
    where b.org_sk is null
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_rst_sku;
    create table tmp_rst_sku as
    select distinct c.sku_sk::text sku_sk,c.skc_sk::text skc_sk,c.size_code,c.size_group_code
    ,c.virtual_suit as virtual_suit_code,c.age_group as reserved9,c.order_level as reserved10
    from gp.tenant_peacebird_dm.dim_sku c
    where exists (select 1 from tmp_sku_target t where c.skc_sk::int =t.skc_sk::int)
    ;"""
exesql(sql)	

#%%
sql=f"""   --补录sku数据
    drop table if exists tmp_sku_target_group ;
    create table tmp_sku_target_group as
    select  a.skc_order_id
      ,a.human_allot_out_org_sk,a.human_allot_in_org_sk
      ,a.skc_sk,a.scene_code,a.ra_source
    from tmp_sku_target a
    group by a.skc_order_id,a.human_allot_out_org_sk,a.human_allot_in_org_sk,a.skc_sk,a.scene_code,a.ra_source
    ;"""
exesql(sql)	

#%%
sql=f""" --tmp_target_order_id_size 目标单据补全尺码
    drop table if exists tmp_target_order_id_size;
    create table tmp_target_order_id_size as
    select  a.skc_order_id,
      a.human_allot_out_org_sk,a.human_allot_in_org_sk
      ,a.skc_sk,scene_code,ra_source
      ,b.size_code
    from tmp_sku_target_group a
    left join tmp_rst_sku b on a.skc_sk=b.skc_sk
    ;"""
exesql(sql)	

#%%
sql=f"""--数据汇总
    drop table if exists tmp1;
    create table tmp1 as
    select a.skc_order_id,
      a.human_allot_out_org_sk,a.human_allot_in_org_sk,
      a.skc_sk,
      a.size_code,
      sum(b.model_ra_qty) as model_ra_qty,sum(b.human_ra_qty) as human_ra_qty
    from tmp_target_order_id_size a
    left join tmp_sku_target b on a.skc_order_id=b.skc_order_id and a.size_code=b.size_code --and b.day_date ='{day_date}'
    group by grouping sets((1,2,3,4,5),(1,2,3,4))
    ;"""
exesql(sql)	

#%%
# json_object_agg 不支持
sql=f"""    --tmp2 将补调量组合成json格式
    drop table if exists tmp2 ;
    create table tmp2 as
    select a.skc_order_id,
      a.human_allot_out_org_sk,human_allot_in_org_sk,
      a.skc_sk,
      json_group_array(json_object('key', coalesce(a.size_code,'total'), 'value', model_ra_qty)) as model_ra_qty,
      json_group_array(json_object('key', coalesce(a.size_code,'total'), 'value', human_ra_qty)) as human_ra_qty
    from tmp1 a
    group by 1,2,3,4
    ;"""
exesql(sql)	

#%%
sql=f"""
    --增加过滤器 --处理到org+skc+场景+来源 后续也按这个粒度更新，如相同款店组合场景+来源分别多对一更新
    drop table if exists tmpx;
    create table tmpx as
    select org_sk,skc_sk,size_code,scene_code,ra_source,0 as is_url
    from
    (
    select human_allot_out_org_sk org_sk,skc_sk,size_code,scene_code,ra_source
    from tmp_target_order_id_size
    union
    select human_allot_in_org_sk org_sk,skc_sk,size_code,scene_code,ra_source
    from tmp_target_order_id_size
    ) t
    group by 1,2,3,4,5
    ;"""
exesql(sql)	

#%%
sql=f"""--tmp_sku_org_distinct sku-org组合 用于过滤
    drop table if exists tmp_sku_org_distinct;
    create table tmp_sku_org_distinct as
    select distinct org_sk,sku_sk,t1.size_code
    from tmpx t1
    inner join tmp_rst_sku t2 on t1.skc_sk=t2.skc_sk and t1.size_code=t2.size_code
    ;"""
exesql(sql)	

#%% # 2.66
sql=f"""--获取stock库存数据
    drop table if exists tmp_ra_sku_org_stock;
    create table tmp_ra_sku_org_stock as
    select a.skc_sk,a.org_sk,b.size_code,a.committed_onorder_out_qty,a.forecast_available_stock_qty
    from gp.tenant_peacebird_biz.rst_ra_sku_org_stock a
    inner join tmp_sku_org_distinct b on a.sku_sk=b.sku_sk and a.org_sk=b.org_sk
    where a.day_date='{day_date}'
    ;"""

sql2=f"""--获取stock库存数据
    drop table if exists tmp_ra_sku_org_stock;
    create table tmp_ra_sku_org_stock as
    with a as(select * from postgres_query('gp',$$
    select a.sku_sk,a.skc_sk,a.org_sk,a.committed_onorder_out_qty,a.forecast_available_stock_qty
    from tenant_peacebird_biz.rst_ra_sku_org_stock a
    where a.day_date='{day_date}'$$)
    )select a.*,b.size_code
     from a
    inner join tmp_sku_org_distinct b on a.sku_sk=b.sku_sk and a.org_sk=b.org_sk
    ;"""
exesql(sql)	   

#%%
sql=f"""--获取实时库存数据
    drop table if exists tmp_ra_sku_org_stock_realtime;
    create table tmp_ra_sku_org_stock_realtime as
    select a.skc_sk,a.org_sk,b.size_code
    ,a.realtime_forecast_available_stock_qty
    ,a.realtime_before_ra_stock_qty
    ,a.realtime_onorder_out_stock_qty
    ,a.realtime_onorder_in_stock_qty
    ,a.realtime_onroad_stock_qty
    ,a.reserved16
    from gp.tenant_peacebird_biz.rst_ra_sku_org_stock_realtime a
    inner join tmp_sku_org_distinct b on a.sku_sk=b.sku_sk and a.org_sk=b.org_sk
    where a.day_date='{day_date}'
    ;"""
exesql(sql)	

#%%
sql=f"""--获取实时库存数据 聚合到skc_sk+org_sk
    drop table if exists tmp_ra_sku_org_stock_realtime_skc;
    create table tmp_ra_sku_org_stock_realtime_skc as
    select a.skc_sk,a.org_sk
    from tmp_ra_sku_org_stock_realtime a
    group by a.skc_sk,a.org_sk
    ;"""
exesql(sql)	

#%%
sql=f"""    -- tmp_target_skc_org 获取目标skc-org用于过滤
    drop table if exists tmp_target_skc_org;
    create  table tmp_target_skc_org as
    select skc_sk,human_allot_out_org_sk as org_sk
    from tmp_rst_ra_sku_org_detail
    group by skc_sk,human_allot_out_org_sk
    union
    select skc_sk,human_allot_in_org_sk as org_sk
    from tmp_rst_ra_sku_org_detail
    group by skc_sk,human_allot_in_org_sk
    ;"""
exesql(sql)	

#%%
sql=f"""drop table if exists tmp_target_skc_org_filter;
create  table tmp_target_skc_org_filter as
    select skc_sk,org_sk
    from tmp_target_skc_org
    where org_sk is not null
    group by skc_sk,org_sk
    ;"""
exesql(sql)	

#%% # 6.64s
sql=f"""    drop table if exists tmp_ra_sku_org_data;
    create  table tmp_ra_sku_org_data as
    select a.*
    from gp.tenant_peacebird_biz.rst_ra_sku_org_data a
    inner join tmp_target_skc_org_filter b on a.skc_sk=b.skc_sk and a.org_sk=b.org_sk and a.day_date = '{day_date}'
    """
exesql(sql)	

#%%
sql=f"""
    --tmp_ra_sku_org_data_filter 过滤有过销售款店 --用于计算款店销售
    drop table if exists tmp_ra_sku_org_data_filter;
    create  table tmp_ra_sku_org_data_filter as
    select  a.*
    from tmp_ra_sku_org_data a
    inner join tmp_sku_org_distinct b on a.sku_sk=b.sku_sk and a.org_sk=b.org_sk
    ;"""
exesql(sql)	

#%%
sql=f""" --tmp_ra_sku_org_data_filter_sales 过滤有过销售款店 --用于计算粗粒度销售，不能过滤款店
    drop table if exists tmp_ra_sku_org_data_filter_sales;
    create  table tmp_ra_sku_org_data_filter_sales as
    select  *
    from tmp_ra_sku_org_data
    where total_sales_qty>0
    ;"""
exesql(sql)	


#%%
sql=f"""  --从data表当中读取计算的相关数据指标
    drop table  if exists tmp3;
    create table tmp3 as
    select c.org_sk stockorg_sk,c.skc_sk ,a.history_first_distribution_date,a.first_distribution_date,a.sales_level_code sales_level
    ,c.scene_code,c.ra_source
    ,c.size_code
    ,sum(a.last_7days_sales_qty) last_7days_sales_qty
    ,sum(a.last_7_14days_sales_qty) last_7_14days_sales_qty
    ,sum(a.total_sales_qty) total_sales_qty
    --,sum(case when c.scene_code in ('9','12') and c.ra_source in ('1','2') then coalesce(a.realtime_before_ra_stock_qty,a.before_ra_stock_qty) else a.before_ra_stock_qty end ) as before_ra_stock_qty
    --1 补调单来源-非实时 ->非实时库存
    --2 补调单来源-实时 1)有实时接口 ->实时库存,补0 2)无实时接口 ->非实时库存
    --3 补调单来源-人工新增/规则数据 有实时用实时，否则用非实时
    --4 其他-有实时用实时，否则用非实时
    ,sum(case when c.ra_source in ('0') then a.before_ra_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_before_ra_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.before_ra_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_before_ra_stock_qty,a.before_ra_stock_qty)
    else coalesce(a.realtime_before_ra_stock_qty,a.before_ra_stock_qty) end ) as before_ra_stock_qty
    ,sum(case when c.ra_source in ('0') then a.before_ra_sub_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_before_ra_sub_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.before_ra_sub_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_before_ra_sub_stock_qty,a.before_ra_sub_stock_qty)
    else coalesce(a.realtime_before_ra_sub_stock_qty,a.before_ra_sub_stock_qty) end ) as before_ra_sub_stock_qty
    ,sum(case when c.ra_source in ('0') then a.after_model_ra_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_after_model_ra_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.after_model_ra_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_after_model_ra_stock_qty,a.after_model_ra_stock_qty)
    else coalesce(a.realtime_after_model_ra_stock_qty,a.after_model_ra_stock_qty) end ) as after_model_ra_stock_qty --'模型补调后库存'
    ,sum(coalesce(a.after_model_ra_stock_qty,0)+coalesce(a.onroad_stock_qty,0)) as after_model_ra_onroad_stock_qty --模型补调后库存（含在途）
    ,sum(case when c.ra_source in ('0') then a.after_ra_sub_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_after_ra_sub_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.after_ra_sub_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_after_ra_sub_stock_qty,a.after_ra_sub_stock_qty)
    else coalesce(a.realtime_after_ra_sub_stock_qty,a.after_ra_sub_stock_qty) end ) as after_ra_sub_stock_qty --补调后库存（减在单出）
    ,sum(case when c.ra_source in ('0') then a.after_model_ra_include_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_after_model_ra_include_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.after_model_ra_include_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_after_model_ra_include_stock_qty,a.after_model_ra_include_stock_qty)
    else coalesce(a.realtime_after_model_ra_include_stock_qty,a.after_model_ra_include_stock_qty) end ) as after_model_ra_include_stock_qty --'模型补调后库存（含在单在途）'
    ,sum(case when c.ra_source in ('0') then a.onroad_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(e.realtime_onroad_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.onroad_stock_qty
    when c.ra_source in ('2','3') then coalesce(e.realtime_onroad_stock_qty,a.onroad_stock_qty)
    else coalesce(e.realtime_onroad_stock_qty,a.onroad_stock_qty) end ) as onroad_stock_qty
    ,sum(case when c.ra_source in ('0') then a.onorder_in_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(e.realtime_onorder_in_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.onorder_in_stock_qty
    when c.ra_source in ('2','3') then coalesce(e.realtime_onorder_in_stock_qty,a.onorder_in_stock_qty)
    else coalesce(e.realtime_onorder_in_stock_qty,a.onorder_in_stock_qty) end ) as onorder_in_stock_qty
    ,sum(case when c.ra_source in ('0') then a.onorder_out_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(e.realtime_onorder_out_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.onorder_out_stock_qty
    when c.ra_source in ('2','3') then coalesce(e.realtime_onorder_out_stock_qty,a.onorder_out_stock_qty)
    else coalesce(e.realtime_onorder_out_stock_qty,a.onorder_out_stock_qty) end ) as onorder_out_stock_qty
    --,sum(d.committed_onorder_out_qty) as committed_onorder_out_qty
    --已提交按来源区分 d.committed_onorder_out_qty e.reserved16
    ,sum(case when c.ra_source in ('0') then coalesce(d.committed_onorder_out_qty,0)
    when c.ra_source in ('1') and c.is_url=1 then coalesce(e.reserved16,0)
    when c.ra_source in ('1') and c.is_url=0 then coalesce(d.committed_onorder_out_qty,0)
    --when c.ra_source in ('2','3') then coalesce(e.reserved16,coalesce(d.committed_onorder_out_qty,0))
    --else coalesce(e.reserved16,coalesce(d.committed_onorder_out_qty,0)) end ) as committed_onorder_out_qty
    when c.ra_source in ('2','3') and f.skc_sk is not null then coalesce(e.reserved16,0)
    when c.ra_source in ('2','3') and f.skc_sk is null then coalesce(d.committed_onorder_out_qty,0)
    else coalesce(d.committed_onorder_out_qty,0) end ) as committed_onorder_out_qty
    ,sum(case when c.ra_source in ('0') then d.forecast_available_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(e.realtime_forecast_available_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then d.forecast_available_stock_qty
    when c.ra_source in ('2','3') then coalesce(e.realtime_forecast_available_stock_qty,d.forecast_available_stock_qty)
    else coalesce(e.realtime_forecast_available_stock_qty,d.forecast_available_stock_qty) end ) as forecast_available_stock_qty
    ,sum(case when c.ra_source in ('0') then a.before_ra_onroad_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_before_ra_onroad_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.before_ra_onroad_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_before_ra_onroad_stock_qty,a.before_ra_onroad_stock_qty)
    else coalesce(a.realtime_before_ra_onroad_stock_qty,a.before_ra_onroad_stock_qty) end ) as before_ra_onroad_stock_qty
    ,sum(case when c.ra_source in ('0') then a.before_ra_include_stock_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_before_ra_include_stock_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.before_ra_include_stock_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_before_ra_include_stock_qty,a.before_ra_include_stock_qty)
    else coalesce(a.realtime_before_ra_include_stock_qty,a.before_ra_include_stock_qty) end ) as before_ra_include_stock_qty
    ,sum(case when c.ra_source in ('0') then a.after_ra_wh_sub_stock_total_qty
    when c.ra_source in ('1') and c.is_url=1 then coalesce(a.realtime_after_ra_wh_sub_stock_total_qty,0)
    when c.ra_source in ('1') and c.is_url=0 then a.after_ra_wh_sub_stock_total_qty
    when c.ra_source in ('2','3') then coalesce(a.realtime_after_ra_wh_sub_stock_total_qty,a.after_ra_wh_sub_stock_total_qty)
    else coalesce(a.realtime_after_ra_wh_sub_stock_total_qty,a.after_ra_wh_sub_stock_total_qty) end ) as after_ra_wh_sub_stock_total_qty

    from tmpx c
    left join tmp_ra_sku_org_data_filter a on c.org_sk=a.org_sk and c.skc_sk=a.skc_sk and c.size_code=a.size_code 
    left join tmp_ra_sku_org_stock d on c.org_sk=d.org_sk and c.skc_sk=d.skc_sk and c.size_code=d.size_code
    left join tmp_ra_sku_org_stock_realtime e on c.org_sk=e.org_sk and c.skc_sk=e.skc_sk and c.size_code=e.size_code
    left join tmp_ra_sku_org_stock_realtime_skc f on c.org_sk=f.org_sk and c.skc_sk=f.skc_sk
    group by
    grouping sets((1,2,3,4,5,6,7,8),(1,2,3,4,5,6,7))
    ;"""
exesql(sql)	


#%%

sql=f"""    --将计算的数据指标整合成json
    drop table if exists tmp4;
    create table tmp4 as
    select a.stockorg_sk ,a.scene_code,a.ra_source,a.skc_sk,
    min(a.history_first_distribution_date) as history_first_distribution_date ,min(a.first_distribution_date) as first_distribution_date ,
    max(a.sales_level) as sales_level,
    json_group_array(json_object('key', size_code, 'value', last_7days_sales_qty)) AS last_7days_sales_qty,
    json_group_array(json_object('key', size_code, 'value', last_7_14days_sales_qty)) AS last_7_14days_sales_qty,
    json_group_array(json_object('key', size_code, 'value', total_sales_qty)) AS total_sales_qty,
    --
    json_group_array(json_object('key', size_code, 'value', before_ra_stock_qty)) AS before_ra_stock_qty,
    json_group_array(json_object('key', size_code, 'value', before_ra_onroad_stock_qty)) AS before_ra_onroad_stock_qty,
    json_group_array(json_object('key', size_code, 'value', before_ra_sub_stock_qty)) AS before_ra_sub_stock_qty,
    json_group_array(json_object('key', size_code, 'value', before_ra_include_stock_qty)) AS before_ra_include_stock_qty,
    --
    json_group_array(json_object('key', size_code, 'value', after_model_ra_stock_qty)) AS after_model_ra_stock_qty,
    json_group_array(json_object('key', size_code, 'value', after_model_ra_onroad_stock_qty)) AS after_model_ra_onroad_stock_qty,
    json_group_array(json_object('key', size_code, 'value', after_ra_sub_stock_qty)) AS after_ra_sub_stock_qty,
    json_group_array(json_object('key', size_code, 'value', after_model_ra_include_stock_qty)) AS after_model_ra_include_stock_qty,
    --
    json_group_array(json_object('key', size_code, 'value', onroad_stock_qty)) AS onroad_stock_qty,
    json_group_array(json_object('key', size_code, 'value', onorder_in_stock_qty)) AS onorder_in_stock_qty,
    json_group_array(json_object('key', size_code, 'value', onorder_out_stock_qty)) AS onorder_out_stock_qty,
    json_group_array(json_object('key', size_code, 'value', committed_onorder_out_qty)) AS committed_onorder_out_qty,
    json_group_array(json_object('key', size_code, 'value', forecast_available_stock_qty)) AS forecast_available_stock_qty,
    json_group_array(json_object('key', size_code, 'value', after_ra_wh_sub_stock_total_qty)) AS after_ra_wh_sub_stock_total_qty

   
    from tmp3 a
    group by 1,2,3,4
    ;"""
exesql(sql)	

#%%
sql=f""" 
    --tmp_skc_info skc信息
    drop table if exists tmp_skc_info;
    create table tmp_skc_info as
    select b.skc_sk::text skc_sk
    ,b.suit as suit_id
    ,b.color_name
    ,b.skc_code
    ,b.product_code
    ,b.color_code
    ,b.brand_code
    ,b.brand_name
    ,b.product_year
    ,b.product_quarter
    ,b.product_range
    ,b.class_long_code as class_longcode
    ,b.class_long_type as class_longtype
    ,b.band
    ,b.tag_price
    ,b.sale_type as reserved2
    ,b.product_name
    ,b.big_class
    ,b.mid_class
    ,b.tiny_class
    ,b.gender
    from gp.tenant_peacebird_dm.dim_skc b
    where exists
    (select 1 from tmp_sku_target a where a.skc_sk::int=b.skc_sk::int
    )
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_replenish_transfer_level 补调组织单位
    drop table if exists tmp_replenish_transfer_level;
    create  table tmp_replenish_transfer_level as
    select * from postgres_query('gp',$$
    with template_info as (
    select id::text as biz_action_template_code
    ,parameter_table_id
    from tenant_peacebird_biz.gto_biz_action_template
    where is_deleted = 0
    ), parameter_table_info as (
    select a.id,b.biz_action_template_code
    from tenant_peacebird_biz.base_parameter_table as a
    inner join template_info as b
    on a.id = b.parameter_table_id
    where a.is_deleted = 0
    )
    select biz_action_template_code
    ,min(id) as id,min(parameter_value_id) as parameter_value_id,min(parameter_code) as parameter_code
    ,min(parameter_value::text) as parameter_value
    ,coalesce(min(replenish_transfer_level::text),'operating_unit') as replenish_transfer_level
    from (
    select a.biz_action_template_code
    ,a.id, b.id as parameter_value_id, b.parameter_code, b.parameter_value
    , b.parameter_value ->> 'gto.replenish_transfer_level' as replenish_transfer_level
    from parameter_table_info as a
    inner join tenant_peacebird_biz.base_parameter_value as b
    on a.id = b.parameter_table_id
    where b.parameter_code = 'gto.replenish_transfer_level' and b.is_deleted = 0
    union all
    select distinct id::varchar as biz_action_template_code
    ,null::int as id,null::int as parameter_value_id,null as parameter_code,null::jsonb as parameter_value
    ,null as replenish_transfer_level
    from tenant_peacebird_biz.gto_biz_action_template
    where is_deleted='0' and is_enable='1'
    ) aa
    group by biz_action_template_code
    $$)"""
exesql(sql)	

#%%
sql=f"""    --tmp_replenish_transfer_level 补调组织单位
    drop table if exists tmp_replenish_transfer_level2;
    create  table tmp_replenish_transfer_level2 as
    with template_info as (
    select id::text as biz_action_template_code
    ,parameter_table_id
    from gp.tenant_peacebird_biz.gto_biz_action_template
    where is_deleted = 0
    ), parameter_table_info as (
    select a.id,b.biz_action_template_code
    from gp.tenant_peacebird_biz.base_parameter_table as a
    inner join template_info as b
    on a.id = b.parameter_table_id
    where a.is_deleted = 0
    )
    select biz_action_template_code
    ,min(id) as id,min(parameter_value_id) as parameter_value_id,min(parameter_code) as parameter_code
    ,min(parameter_value::text) as parameter_value
    ,coalesce(min(replenish_transfer_level::text),'operating_unit') as replenish_transfer_level
    from (
    select a.biz_action_template_code
    ,a.id, b.id as parameter_value_id, b.parameter_code, b.parameter_value
    , b.parameter_value ->> 'gto.replenish_transfer_level' as replenish_transfer_level
    from parameter_table_info as a
    inner join gp.tenant_peacebird_biz.base_parameter_value as b
    on a.id = b.parameter_table_id
    where b.parameter_code = 'gto.replenish_transfer_level' and b.is_deleted = 0
    union all
    select distinct id::varchar as biz_action_template_code
    ,null::int as id,null::int as parameter_value_id,null as parameter_code,null as parameter_value
    ,null as replenish_transfer_level
    from gp.tenant_peacebird_biz.gto_biz_action_template
    where is_deleted='0' and is_enable='1'
    ) aa
    group by biz_action_template_code
    """
#exesql(sql)	

#%%
sql=f"""    --tmp_org_hierarchy 组织层级
    drop table if exists tmp_org_hierarchy;
    create  table tmp_org_hierarchy as
    select org_hierarchy_code,org_hierarchy_name
    from gp.tenant_peacebird_edw.dim_org_hierarchy
    where day_date='{day_date}'::timestamp
    group by org_hierarchy_code,org_hierarchy_name
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_org_integration 模板的补调组织单位 --业务动作模板编码+组织sk+区域sk
    drop table if exists tmp_org_integration;
    create  table tmp_org_integration as
    --select * from postgres_query('gp',$$
    select distinct b.biz_action_template_code --业务动作模板编码
    ,a.org_sk --组织sk
    ,a.org_code --组织编码
    ,a.org_name --组织名称
    ,b.replenish_transfer_level
    ,c.org_hierarchy_name
    ,a.org_long_hierarchy
,case when a.org_flag='2' then a.parent_org_sk::text else json_object(string_to_array(a.org_long_hierarchy,':'),string_to_array(a.org_long_sk,':'))->> c.org_hierarchy_name end as manager_org_sk --区域sk
,case when a.org_flag='2' then d.org_code else json_object(string_to_array(a.org_long_hierarchy,':'),string_to_array(a.org_long_code,':'))->> c.org_hierarchy_name end as manager_org_code --区域编码
,case when a.org_flag='2' then d.org_name else json_object(string_to_array(a.org_long_hierarchy,':'),string_to_array(a.org_long_name,':'))->> c.org_hierarchy_name end as manager_org_name --区域名称
    ,a.org_code as stockorg_code
    ,a.org_name as stockorg_name
    -- ,a.org_code as manager_org_code
    -- ,a.org_name as manager_org_name
    ,a.store_level
    ,a.status
    ,a.city
    --,a.org_sk as store_sk
    ,a.org_sk::text as store_sk
    ,a.org_flag
    ,case when a.org_flag='2' then null else a.store_manager end as reserved4
    --,d.brand_code

    from gp.tenant_peacebird_dm.dim_org_integration a
    cross join tmp_replenish_transfer_level b
    inner join tmp_org_hierarchy c on b.replenish_transfer_level=c.org_hierarchy_code
    --inner join tenant_peacebird_edw.dim_brand_mapping d on a.operating_unit_sk=d.org_sk
    left  join gp.tenant_peacebird_dm.dim_org_integration d --获取仓库的父级组织
    on a.parent_org_sk = d.org_sk
    where a.status = '正常' and a.org_type=2
    --and org_sk='10054792' --同一家门店在不同模板管理区域不同
    --$$)
    """
exesql(sql)	

#%%
sql=f"""    --tmp_national_skc 全国+skc 销售指标 --排序
    drop table if exists tmp_national_skc;
    create table tmp_national_skc as
    select  a.skc_sk
    ,a.size_code
    ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
    ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
    ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    group by
    grouping sets((1,2),(1))
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_national_skc_json 全国+skc拼接为json字符串
    drop table if exists tmp_national_skc_json;
    create table tmp_national_skc_json as
    select  a.skc_sk,
    json_group_array(json_object('key', size_code, 'value', last_7days_sales_qty)) AS last_7days_sales_qty,
    json_group_array(json_object('key', size_code, 'value', last_7_14days_sales_qty)) AS last_7_14days_sales_qty,
    json_group_array(json_object('key', size_code, 'value', total_sales_qty)) AS total_sales_qty
    from tmp_national_skc a
    group by a.skc_sk
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_national_skc;

    --tmp_dim_org 组织架构维表
    drop table if exists tmp_dim_org;
    create  table tmp_dim_org as
    select *
    from gp.tenant_peacebird_edw.dim_org a
    where a.day_date='{day_date}'::timestamp
    and a.org_type=2
    ;"""
exesql(sql)	

#%%

sql=f"""   -- tmp_org_info org信息
    drop table if exists tmp_org_info;
    create  table tmp_org_info as
    select distinct b.org_code as store_code
            ,b.org_name as store_name
            ,a.org_code as manager_org_code
            ,a.org_name as manager_org_name
            ,b.store_level
            ,b.status
            ,b.city
            ,b.org_sk::text as store_sk
            ,b.org_flag
            ,b.store_manager as reserved4
            ,c.brand_code
    from gp.tenant_peacebird_edw.dim_org_integration b
    left join tmp_dim_org a on b.parent_org_sk = a.org_sk
    inner join gp.tenant_peacebird_edw.dim_brand_mapping c on b.operating_unit_sk=c.org_sk
    where b.status = '正常' and a.org_type=2
       and a.day_date='{day_date}' and b.day_date='{day_date}'
    ;"""
exesql(sql)	

#%%
sql=f"""    -- tmp_manager_org_skc 区域+skc 销售指标 --排序
    drop table if exists tmp_manager_org_skc;
    create  table tmp_manager_org_skc as
    select  c.manager_org_code
        ,c.biz_action_template_code
        ,a.skc_sk
        ,a.size_code
        ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
        ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
        ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    --inner join tmp_org_info c on a.org_sk=c.store_sk and a.brand_code=c.brand_code
    inner join tmp_org_integration c on a.org_sk=c.store_sk --and a.brand_code=c.brand_code
    group by
    grouping sets((1,2,3,4),(1,2,3))
    ;"""
exesql(sql)	

#%%
sql=f"""    -- tmp_manager_org_skc_json 区域+skc拼接为json字符串
    drop table if exists tmp_manager_org_skc_json;
    create  table tmp_manager_org_skc_json as
    select  a.manager_org_code
          ,a.biz_action_template_code
          ,a.skc_sk,
json_group_array(json_object('key', a.size_code, 'value', a.last_7days_sales_qty)) AS last_7days_sales_qty,
    json_group_array(json_object('key', a.size_code, 'value', a.last_7_14days_sales_qty)) AS last_7_14days_sales_qty,
    json_group_array(json_object('key', a.size_code, 'value', a.total_sales_qty)) AS total_sales_qty
    from tmp_manager_org_skc as a
    group by a.manager_org_code,a.biz_action_template_code,a.skc_sk
    ;"""
exesql(sql)	

#%%
sql=f"""     -- tmp_sku_skc
    drop table if exists tmp_sku_skc;
    create  table tmp_sku_skc as
    select  sku_sk::text as sku_sk
        ,skc_sk::text as skc_sk
        ,size_code
        ,size_name
        ,size_order
        ,suit as suit_id
        ,product_code
        ,color_code
    from gp.tenant_peacebird_dm.dim_sku
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_org_suit 门店+套装 销售指标 --排序
    drop table if exists tmp_org_suit;
    create table tmp_org_suit as
    select  a.org_sk
    ,b.suit_id
    ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
    ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
    ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    inner join tmp_sku_skc b on a.sku_sk=b.sku_sk
    group by a.org_sk,b.suit_id
    ;"""
exesql(sql)	

#%%
sql=f"""    -- tmp_manager_org_suit 区域+套装 销售指标 --排序
    drop table if exists tmp_manager_org_suit;
    create  table tmp_manager_org_suit as
    select  c.manager_org_code
        ,c.biz_action_template_code
        ,b.suit_id
        ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
        ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
        ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    inner join tmp_sku_skc b on a.sku_sk=b.sku_sk
    --inner join tmp_org_info c on a.org_sk=c.store_sk and a.brand_code=c.brand_code
    inner join tmp_org_integration c on a.org_sk=c.store_sk --and a.brand_code=c.brand_code
    group by c.manager_org_code,c.biz_action_template_code,b.suit_id
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_national_suit 全国+套装 销售指标 --排序
    drop table if exists tmp_national_suit;
    create table tmp_national_suit as
    select  b.suit_id
    ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
    ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
    ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    inner join tmp_sku_skc b on a.sku_sk=b.sku_sk
    group by b.suit_id
    ;"""
exesql(sql)	

#%%
sql=f"""
    --tmp_org_sales_qty 门店 销售指标 --排序
    drop table if exists tmp_org_sales_qty;
    create table tmp_org_sales_qty as
    select  a.org_sk
    ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
    ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
    ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    group by a.org_sk
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_org_spu 门店+spu 销售指标
    drop table if exists tmp_org_spu;
    create table tmp_org_spu as
    select  a.org_sk
    ,b.product_code
    ,sum(a.last_7days_sales_qty) as last_7days_sales_qty --近7天销量'
    ,sum(a.last_7_14days_sales_qty) as last_7_14days_sales_qty --'近7-14天销量'
    ,sum(a.total_sales_qty) as total_sales_qty --'累计销量'
    from tmp_ra_sku_org_data_filter_sales a
    inner join tmp_sku_skc b on a.sku_sk=b.sku_sk
    group by a.org_sk,b.product_code
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_gto_action_record;
    create table tmp_gto_action_record as
    select b.batch_id,b.org_sk::text org_sk,b.biz_action_template_id::int biz_action_template_id
    from gp.tenant_peacebird_biz.gto_action_record b
    where b.day_date = '{day_date}'::timestamp
    ;"""
exesql(sql)	

#%%
sql=f"""     --tmp_sku_org_detail skc_order_id+属性字段
    drop table if exists tmp_sku_org_detail;
    create table tmp_sku_org_detail as
    select skc_order_id,b.batch_id,max(commit_user_name) as commit_user_name
    ,min(day_date) as day_date
    ,max(biz_action_template_code) as biz_action_template_code
    ,max(biz_action_template_name) as biz_action_template_name
    ,max(a.org_sk) as org_sk
    ,max(step_id) as step_id
    --,max(batch_id) as batch_id
    ,max(job_id) as job_id --任务id
    ,max(task_status) as task_status --任务状态
    ,max(model_allot_out_org_sk) as model_allot_out_org_sk
    ,max(model_allot_in_org_sk) as model_allot_in_org_sk
    ,max(scene_code) as scene_code,max(scene_name) as scene_name,max(remark) as remark,max(brand_code) as brand_code,max(document_code) as document_code
    ,max(ra_source) as ra_source,max(commit_status) as commit_status,max(modify_status) as modify_status,max(is_effective) as is_effective,max(reserved1) as reserved1,max(reserved9) as reserved9
    from tmp_sku_target a
    left join tmp_gto_action_record b on a.biz_action_template_code::int=b.biz_action_template_id and a.org_sk=b.org_sk
    group by skc_order_id,b.batch_id
    ;"""
exesql(sql)	

#%%
sql=f"""   --关联指标数据
    drop table if exists tmp2_kpi;
    create table tmp2_kpi as
    select a.skc_order_id,
      d.model_allot_out_org_sk,d.model_allot_in_org_sk,
      a.human_allot_out_org_sk,human_allot_in_org_sk,
      d.scene_code,d.scene_name,
      d.remark,
      d.brand_code,
      d.ra_source,a.skc_sk,d.commit_status,d.modify_status,d.is_effective,d.reserved1,d.reserved9,
      a.model_ra_qty,
      a.human_ra_qty

    ,f.color_name --'颜色名称'

    ,i.total_sales_qty as national_skc_total_sales_qty --'全国SKC累计销量'
    ,i.last_7days_sales_qty as national_skc_last_7days_sales_qty	--'全国SKC近7天销量'
    ,i.last_7_14days_sales_qty as national_skc_last_7_14days_sales_qty	--'全国SKC近7-14天销量'

    ,j.total_sales_qty as allot_out_manager_org_skc_total_sales_qty --'调出区域skc累计销量'
    ,k.total_sales_qty as allot_in_manager_org_skc_total_sales_qty --'调入区域skc累计销量'
    ,j.last_7days_sales_qty as allot_out_manager_org_skc_last_7days_sales_qty --'调出区域skc近7天销量'
    ,k.last_7days_sales_qty as allot_in_manager_org_skc_last_7days_sales_qty --'调入区域skc近7天销量'
    ,j.last_7_14days_sales_qty as allot_out_manager_org_skc_last_7_14days_sales_qty --'调出区域skc近7-14天销量'
    ,k.last_7_14days_sales_qty as allot_in_manager_org_skc_last_7_14days_sales_qty --'调入区域skc近7-14天销量'

    ,l.total_sales_qty as national_suit_total_sales_qty --'全国套装累计销量'
    ,l.last_7days_sales_qty as national_suit_last_7days_sales_qty --'全国套装近7天销量'
    ,l.last_7_14days_sales_qty as national_suit_last_7_14days_sales_qty --'全国套装近7-14天销量'

    ,o.last_7days_sales_qty as allot_out_org_suit_last_7days_sales_qty --'调出门店套装近7天销量'
    ,p.last_7days_sales_qty as allot_in_org_suit_last_7days_sales_qty --'调入门店套装近7天销量'
    ,o.last_7_14days_sales_qty as allot_out_org_suit_last_7_14days_sales_qty --'调出门店套装近7-14天销量'
    ,p.last_7_14days_sales_qty as allot_in_org_suit_last_7_14days_sales_qty --'调入门店套装近7-14天销量'
    ,o.total_sales_qty as allot_out_org_suit_total_sales_qty --'调出门店套装累计销量'
    ,p.total_sales_qty as allot_in_org_suit_total_sales_qty --'调入门店套装累计销量'

    ,m.last_7days_sales_qty as allot_out_manager_org_suit_last_7days_sales_qty --'调出区域套装近7天销量'
    ,n.last_7days_sales_qty as allot_in_manager_org_suit_last_7days_sales_qty --'调入区域套装近7天销量'
    ,m.last_7_14days_sales_qty as allot_out_manager_org_suit_last_7_14days_sales_qty --'调出区域套装近7-14天销量'
    ,n.last_7_14days_sales_qty as allot_in_manager_org_suit_last_7_14days_sales_qty --'调入区域套装近7-14天销量'
    ,m.total_sales_qty as allot_out_manager_org_suit_total_sales_qty --'调出区域套装累计销量'
    ,n.total_sales_qty as allot_in_manager_org_suit_total_sales_qty --'调入区域套装累计销量'

    ,q.last_7days_sales_qty as allot_out_org_all_last_7days_sales_qty --'调出门店近7天销量'
    ,r.last_7days_sales_qty as allot_in_org_all_last_7days_sales_qty --'调入门店近7天销量'
    ,q.last_7_14days_sales_qty as allot_out_org_all_last_7_14days_sales_qty --'调出门店近7-14天销量'
    ,r.last_7_14days_sales_qty as allot_in_org_all_last_7_14days_sales_qty --'调入门店近7-14天销量'

    ,s.total_sales_qty as allot_out_org_spu_total_sales_qty
    ,s.last_7days_sales_qty as allot_out_org_spu_last_7days_sales_qty
    ,s.last_7_14days_sales_qty as allot_out_org_spu_last_7_14days_sales_qty
    ,t.total_sales_qty as allot_in_org_spu_total_sales_qty
    ,t.last_7days_sales_qty as allot_in_org_spu_last_7days_sales_qty
    ,t.last_7_14days_sales_qty as allot_in_org_spu_last_7_14days_sales_qty

    ,d.biz_action_template_code
    ,d.document_code
    ,d.batch_id

    from tmp2 a

    inner join tmp_sku_org_detail d on a.skc_order_id=d.skc_order_id
    left join tmp_org_integration b on a.human_allot_out_org_sk=b.store_sk and d.biz_action_template_code=b.biz_action_template_code
    left join tmp_org_integration c on a.human_allot_in_org_sk=c.store_sk and d.biz_action_template_code=c.biz_action_template_code

    left join tmp_skc_info f on a.skc_sk=f.skc_sk

    left join tmp_national_skc_json i on a.skc_sk=i.skc_sk

    left join tmp_manager_org_skc_json j on a.skc_sk=j.skc_sk
      and b.manager_org_code=j.manager_org_code and d.biz_action_template_code=j.biz_action_template_code
    left join tmp_manager_org_skc_json k on a.skc_sk=k.skc_sk
      and c.manager_org_code=k.manager_org_code and d.biz_action_template_code=k.biz_action_template_code

    left join tmp_national_suit l on f.suit_id=l.suit_id

    left join tmp_manager_org_suit m on f.suit_id=m.suit_id
      and b.manager_org_code=m.manager_org_code and d.biz_action_template_code=m.biz_action_template_code
    left join tmp_manager_org_suit n on f.suit_id=n.suit_id
      and c.manager_org_code=n.manager_org_code and d.biz_action_template_code=n.biz_action_template_code

    left join tmp_org_suit o on f.suit_id=o.suit_id and a.human_allot_out_org_sk=o.org_sk
    left join tmp_org_suit p on f.suit_id=p.suit_id and a.human_allot_in_org_sk=p.org_sk

    left join tmp_org_sales_qty q on a.human_allot_out_org_sk=q.org_sk
    left join tmp_org_sales_qty r on a.human_allot_in_org_sk=r.org_sk

    left join tmp_org_spu s on f.product_code=s.product_code and a.human_allot_out_org_sk=s.org_sk
    left join tmp_org_spu t on f.product_code=t.product_code and a.human_allot_in_org_sk=t.org_sk
    ;"""
exesql(sql)	

#%%
sql="""
    --商品类别处理
    --筛选skc
    drop table if exists tmp_skc;
    create table tmp_skc as
    select b.*
    from tmp_skc_info b
    ;"""
exesql(sql)	

#%%
sql=f"""    --商品长编码 --class_longcode 第三层级
    drop table if exists tmp6_1;
    create table tmp6_1 as
    select a.brand_code,
    skc_sk,
    split_part(a.class_longcode,':',3) as class_longtype,
    split_part(a.class_longtype,':',1)||'#'||split_part(a.class_longcode,':',1)||'&'|| split_part(a.class_longtype,':',2)||'#'||split_part(a.class_longcode,':',2)||'&'|| split_part(a.class_longtype,':',3)||'#'||split_part(a.class_longcode,':',3) class_longcode,
    max(array_length(string_to_array(class_longcode,':'),'1')) leg,
    '4'::int as act_length
    from tmp_skc a
    group by 1,2,3,4
    ;"""
exesql(sql)	

#%%
sql=f"""    --商品长编码 --class_longcode 第二层级
    drop table if exists tmp6_2;
    create table tmp6_2 as
    select a.brand_code,
    skc_sk,
    split_part(a.class_longcode,':',2) as class_longtype,
    split_part(a.class_longtype,':',1)||'#'||split_part(a.class_longcode,':',1)||'&'|| split_part(a.class_longtype,':',2)||'#'||split_part(a.class_longcode,':',2) class_longcode,
    max(array_length(string_to_array(class_longcode,':'),'1')) leg,
    '3'::int as act_length
    from tmp_skc a
    group by 1,2,3,4
    ;"""
exesql(sql)	

#%%
sql=f"""    --商品长编码 --class_longcode 第一层级
    drop table if exists tmp6_3;
    create table tmp6_3 as
    select a.brand_code,
    skc_sk,
    split_part(a.class_longcode,':',1) as class_longtype,
    split_part(a.class_longtype,':',1)||'#'||split_part(a.class_longcode,':',1) class_longcode,
    max(array_length(string_to_array(class_longcode,':'),'1')) leg,
    '2'::int as act_length
    from tmp_skc a
    group by 1,2,3,4
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_skc;
    --汇总筛选 只取当前最大length-1
    drop table if exists tmp6;
    create table tmp6 as
    select *
    from tmp6_1
    where act_length=leg
    union
    select *
    from tmp6_2
    where act_length=leg
    union
    select *
    from tmp6_3
    where act_length=leg
    ;"""
exesql(sql)	

#%%
sql=f"""  
    drop table if exists tmp_all_biz_rst_ra_skc_org_detail;
    create  table tmp_all_biz_rst_ra_skc_org_detail as
    select
     a.human_allot_out_org_sk
    ,a.human_allot_in_org_sk
    ,a.model_allot_out_org_sk
    ,a.model_allot_in_org_sk
    ,a.skc_sk
    ,a.order_id
    ,a.id
    ,a.human_exception_type
    ,a.model_exception_type
    ,a.biz_action_template_code
    ,a.batch_id
    ,a.in_org_send_priority_score
    ,a.in_org_receive_priority_score
    ,a.in_org_target_stock
    ,a.template_in_org_before_model_ra_stock_qty
    ,a.template_in_org_before_model_ra_onroad_stock_qty
    ,a.template_in_org_before_model_ra_sub_stock_qty
    ,a.template_in_org_before_model_ra_include_stock_qty
    ,a.template_in_org_after_model_ra_stock_qty
    ,a.template_in_org_after_model_ra_onroad_stock_qty
    ,a.template_in_org_after_model_ra_sub_stock_qty
    ,a.template_in_org_after_model_ra_include_stock_qty
    ,a.template_in_org_before_human_ra_stock_qty
    ,a.template_in_org_before_human_ra_onroad_stock_qty
    ,a.template_in_org_before_human_ra_sub_stock_qty
    ,a.template_in_org_before_human_ra_include_stock_qty
    ,a.template_in_org_after_human_ra_stock_qty
    ,a.template_in_org_after_human_ra_onroad_stock_qty
    ,a.template_in_org_after_human_ra_sub_stock_qty
    ,a.template_in_org_after_human_ra_include_stock_qty
    ,a.in_org_after_human_ra_stock_qty
    ,a.in_org_after_human_ra_onroad_stock_qty
    ,a.in_org_after_human_ra_sub_stock_qty
    ,a.in_org_after_human_ra_include_stock_qty
    ,a.out_org_send_priority_score
    ,a.out_org_receive_priority_score
    ,a.out_org_target_stock
    ,a.template_out_org_before_model_ra_stock_qty
    ,a.template_out_org_before_model_ra_onroad_stock_qty
    ,a.template_out_org_before_model_ra_sub_stock_qty
    ,a.template_out_org_before_model_ra_include_stock_qty
    ,a.template_out_org_after_model_ra_stock_qty
    ,a.template_out_org_after_model_ra_onroad_stock_qty
    ,a.template_out_org_after_model_ra_sub_stock_qty
    ,a.template_out_org_after_model_ra_include_stock_qty
    ,a.template_out_org_before_human_ra_stock_qty
    ,a.template_out_org_before_human_ra_onroad_stock_qty
    ,a.template_out_org_before_human_ra_sub_stock_qty
    ,a.template_out_org_before_human_ra_include_stock_qty
    ,a.template_out_org_after_human_ra_stock_qty
    ,a.template_out_org_after_human_ra_onroad_stock_qty
    ,a.template_out_org_after_human_ra_sub_stock_qty
    ,a.template_out_org_after_human_ra_include_stock_qty
    ,a.out_org_after_human_ra_stock_qty
    ,a.out_org_after_human_ra_onroad_stock_qty
    ,a.out_org_after_human_ra_sub_stock_qty
    ,a.out_org_after_human_ra_include_stock_qty
    from  gp.tenant_peacebird_biz.rst_ra_skc_org_detail a
    where a.day_date = '{day_date}' and a.is_deleted = '0'
    ;"""
exesql(sql)	

#%%
sql=f"""  --
    drop table if exists tmp_biz_rst_ra_skc_org_detail;
    create  table tmp_biz_rst_ra_skc_org_detail as
    select a.*
    from tmp_all_biz_rst_ra_skc_org_detail a
    where exists ( select 1 from tmp_org_skc_union b where
    	(a.human_allot_out_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    	or (a.human_allot_in_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    	or (a.model_allot_out_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    	or (a.model_allot_in_org_sk=b.org_sk and a.skc_sk=b.skc_sk)
    )"""
exesql(sql)	

#%%
sql=f""" 
    -- tmp_rst_ra_skc_org_detail 取skc表
    drop table if exists tmp_rst_ra_skc_org_detail;
    create  table tmp_rst_ra_skc_org_detail as
    select skc_sk,store_sk,biz_action_template_code,batch_id
      ,min(send_priority_score) as send_priority_score
      ,min(receive_priority_score) as receive_priority_score
      ,min(target_stock::text)::json as target_stock
      ,min(template_before_model_ra_stock_qty::text) as template_before_model_ra_stock_qty
      ,min(template_before_model_ra_onroad_stock_qty::text) as template_before_model_ra_onroad_stock_qty
      ,min(template_before_model_ra_sub_stock_qty::text) as template_before_model_ra_sub_stock_qty
      ,min(template_before_model_ra_include_stock_qty::text) as template_before_model_ra_include_stock_qty
      ,min(template_after_model_ra_stock_qty::text) as template_after_model_ra_stock_qty
      ,min(template_after_model_ra_onroad_stock_qty::text) as template_after_model_ra_onroad_stock_qty
      ,min(template_after_model_ra_sub_stock_qty::text) as template_after_model_ra_sub_stock_qty
      ,min(template_after_model_ra_include_stock_qty::text) as template_after_model_ra_include_stock_qty
      --模板调入方人工补调前/后库存
      ,min(template_before_human_ra_stock_qty::text) as template_before_human_ra_stock_qty
      ,min(template_before_human_ra_onroad_stock_qty::text) as template_before_human_ra_onroad_stock_qty
      ,min(template_before_human_ra_sub_stock_qty::text) as template_before_human_ra_sub_stock_qty
      ,min(template_before_human_ra_include_stock_qty::text) as template_before_human_ra_include_stock_qty
      ,min(template_after_human_ra_stock_qty::text) as template_after_human_ra_stock_qty
      ,min(template_after_human_ra_onroad_stock_qty::text) as template_after_human_ra_onroad_stock_qty
      ,min(template_after_human_ra_sub_stock_qty::text) as template_after_human_ra_sub_stock_qty
      ,min(template_after_human_ra_include_stock_qty::text) as template_after_human_ra_include_stock_qty
      --人工补调后
      ,min(after_human_ra_stock_qty::text) as after_human_ra_stock_qty
      ,min(after_human_ra_onroad_stock_qty::text) as after_human_ra_onroad_stock_qty
      ,min(after_human_ra_sub_stock_qty::text) as after_human_ra_sub_stock_qty
      ,min(after_human_ra_include_stock_qty::text) as after_human_ra_include_stock_qty
    from (
    select a.skc_sk
      ,a.human_allot_in_org_sk as store_sk
      ,a.biz_action_template_code
      ,a.batch_id
      ,a.in_org_send_priority_score as send_priority_score
      ,a.in_org_receive_priority_score as receive_priority_score
      ,a.in_org_target_stock as target_stock
      ,a.template_in_org_before_model_ra_stock_qty as template_before_model_ra_stock_qty
      ,a.template_in_org_before_model_ra_onroad_stock_qty as template_before_model_ra_onroad_stock_qty
      ,a.template_in_org_before_model_ra_sub_stock_qty as template_before_model_ra_sub_stock_qty
      ,a.template_in_org_before_model_ra_include_stock_qty as template_before_model_ra_include_stock_qty
      ,a.template_in_org_after_model_ra_stock_qty as template_after_model_ra_stock_qty
      ,a.template_in_org_after_model_ra_onroad_stock_qty as template_after_model_ra_onroad_stock_qty
      ,a.template_in_org_after_model_ra_sub_stock_qty as template_after_model_ra_sub_stock_qty
      ,a.template_in_org_after_model_ra_include_stock_qty as template_after_model_ra_include_stock_qty
      --in_human 模板调入方人工补调前/后库存
      ,a.template_in_org_before_human_ra_stock_qty as template_before_human_ra_stock_qty
      ,a.template_in_org_before_human_ra_onroad_stock_qty as template_before_human_ra_onroad_stock_qty
      ,a.template_in_org_before_human_ra_sub_stock_qty as template_before_human_ra_sub_stock_qty
      ,a.template_in_org_before_human_ra_include_stock_qty as template_before_human_ra_include_stock_qty

      ,a.template_in_org_after_human_ra_stock_qty as template_after_human_ra_stock_qty
      ,a.template_in_org_after_human_ra_onroad_stock_qty as template_after_human_ra_onroad_stock_qty
      ,a.template_in_org_after_human_ra_sub_stock_qty as template_after_human_ra_sub_stock_qty
      ,a.template_in_org_after_human_ra_include_stock_qty as template_after_human_ra_include_stock_qty
      --人工补调后
      ,a.in_org_after_human_ra_stock_qty as after_human_ra_stock_qty
      ,a.in_org_after_human_ra_onroad_stock_qty as after_human_ra_onroad_stock_qty
      ,a.in_org_after_human_ra_sub_stock_qty as after_human_ra_sub_stock_qty
      ,a.in_org_after_human_ra_include_stock_qty as after_human_ra_include_stock_qty
    from tmp_biz_rst_ra_skc_org_detail a
    union
    select a.skc_sk
      ,a.human_allot_out_org_sk as store_sk
      ,a.biz_action_template_code
      ,a.batch_id
      ,a.out_org_send_priority_score as send_priority_score
      ,a.out_org_receive_priority_score as receive_priority_score
      ,a.out_org_target_stock as target_stock
      ,a.template_out_org_before_model_ra_stock_qty as template_before_model_ra_stock_qty
      ,a.template_out_org_before_model_ra_onroad_stock_qty as template_before_model_ra_onroad_stock_qty
      ,a.template_out_org_before_model_ra_sub_stock_qty as template_before_model_ra_sub_stock_qty
      ,a.template_out_org_before_model_ra_include_stock_qty as template_before_model_ra_include_stock_qty
      ,a.template_out_org_after_model_ra_stock_qty as template_after_model_ra_stock_qty
      ,a.template_out_org_after_model_ra_onroad_stock_qty as template_after_model_ra_onroad_stock_qty
      ,a.template_out_org_after_model_ra_sub_stock_qty as template_after_model_ra_sub_stock_qty
      ,a.template_out_org_after_model_ra_include_stock_qty as template_after_model_ra_include_stock_qty
      --out_human 模板调出方人工补调前/后库存
      ,a.template_out_org_before_human_ra_stock_qty as template_before_human_ra_stock_qty
      ,a.template_out_org_before_human_ra_onroad_stock_qty as template_before_human_ra_onroad_stock_qty
      ,a.template_out_org_before_human_ra_sub_stock_qty as template_before_human_ra_sub_stock_qty
      ,a.template_out_org_before_human_ra_include_stock_qty as template_before_human_ra_include_stock_qty

      ,a.template_out_org_after_human_ra_stock_qty as template_after_human_ra_stock_qty
      ,a.template_out_org_after_human_ra_onroad_stock_qty as template_after_human_ra_onroad_stock_qty
      ,a.template_out_org_after_human_ra_sub_stock_qty as template_after_human_ra_sub_stock_qty
      ,a.template_out_org_after_human_ra_include_stock_qty as template_after_human_ra_include_stock_qty
      --人工补调后
      ,a.out_org_after_human_ra_stock_qty as after_human_ra_stock_qty
      ,a.out_org_after_human_ra_onroad_stock_qty as after_human_ra_onroad_stock_qty
      ,a.out_org_after_human_ra_sub_stock_qty as after_human_ra_sub_stock_qty
      ,a.out_org_after_human_ra_include_stock_qty as after_human_ra_include_stock_qty
    from tmp_biz_rst_ra_skc_org_detail a
    ) aa
    where batch_id is not null
    --and store_sk='10373197' and skc_sk='10184633'
    group by skc_sk,store_sk,biz_action_template_code,batch_id
    ;"""
exesql(sql)	

#%%
sql=f"""    --人工补调后
    drop table if exists tmp_skc_org_after_huaman_kpi;
    create  table tmp_skc_org_after_huaman_kpi as
    select skc_sk,store_sk
      ,min(after_human_ra_stock_qty::text) as after_human_ra_stock_qty
      ,min(after_human_ra_onroad_stock_qty::text) as after_human_ra_onroad_stock_qty
      ,min(after_human_ra_sub_stock_qty::text) as after_human_ra_sub_stock_qty
      ,min(after_human_ra_include_stock_qty::text) as after_human_ra_include_stock_qty
    from tmp_rst_ra_skc_org_detail
    group by skc_sk,store_sk
    ;"""
exesql(sql)	

#%%
sql=f"""    -- tmp_rst_ra_skc_org_detail_kpi 取skc表kpi
    drop table if exists tmp_rst_ra_skc_org_detail_kpi;
    create  table tmp_rst_ra_skc_org_detail_kpi as
    select a.order_id
      ,max(human_exception_type) as human_exception_type
      ,max(model_exception_type) as model_exception_type
    from tmp_biz_rst_ra_skc_org_detail a
    inner join tmp2 b on a.order_id=b.skc_order_id
    group by a.order_id
    ;"""
exesql(sql)	

#%%
sql=f"""  
    drop table if exists tmp_virtual_suit_code;
    create table tmp_virtual_suit_code as
    select skc_sk,
    max(virtual_suit_code) as virtual_suit_code,
    max(reserved9) as reserved9,
    max(reserved10) as reserved10,
    max(size_group_code) as size_group_code
    from tmp_rst_sku
    group by skc_sk
    ;"""
exesql(sql)	

#%%
sql=f""" 
    drop table if exists tmp_rst_ra_skc_org_detail_result;
    create table tmp_rst_ra_skc_org_detail_result as
    select a.skc_sk,b.skc_code,b.product_code,b.color_code,b.brand_code,b.brand_name,(b.product_year)::text||b.product_quarter product_year_quarter,
      b.product_range,b.band,
      o.class_longcode,
      b.suit_id,b.tag_price,
      i.sales_level out_sales_level_code,
      case when i.sales_level='1'then '畅'
           when i.sales_level='2'then '平'
           when i.sales_level='3'then '滞' end out_sales_level_name,
      d.sales_level in_sales_level_code,
      case when d.sales_level='1'then '畅'
           when d.sales_level='2'then '平'
           when d.sales_level='3'then '滞' end in_sales_level_name,
      b.reserved2 sales_type,x.size_group_code,
      g.manager_org_code model_allot_out_manager_org_code,
      h.manager_org_code model_allot_in_manager_org_code,
      g.manager_org_name model_allot_out_manager_org_name,
      h.manager_org_name  model_allot_in_manager_org_name,
      a.model_allot_out_org_sk,a.model_allot_in_org_sk,
      g.stockorg_code model_allot_out_org_code,
      h.stockorg_code model_allot_in_org_code,
      g.stockorg_name model_allot_out_org_name,
      h.stockorg_name model_allot_in_org_name,
      e.manager_org_code human_allot_out_manager_org_code,f.manager_org_code human_allot_in_manager_org_code,
      e.manager_org_name human_allot_out_manager_org_name,f.manager_org_name human_allot_in_manager_org_name,
      a.human_allot_out_org_sk,a.human_allot_in_org_sk,e.stockorg_code human_allot_out_org_code,
      f.stockorg_code human_allot_in_org_code,e.stockorg_name human_allot_out_org_name,f.stockorg_name human_allot_in_org_name,
      f.store_level in_store_level,f.city in_store_city,d.history_first_distribution_date in_store_history_first_distribution_date,
      d.first_distribution_date in_store_first_distribution_date,d.last_7days_sales_qty in_org_last_7days_sales_qty,
      d.last_7_14days_sales_qty in_org_last_7_14days_sales_qty,d.total_sales_qty in_org_total_sales_qty
      --
      ,d.before_ra_stock_qty as in_org_before_ra_stock_qty
      ,d.before_ra_onroad_stock_qty as in_org_before_ra_onroad_stock_qty
      ,d.before_ra_sub_stock_qty as in_org_before_ra_sub_stock_qty
      ,d.before_ra_include_stock_qty as in_org_before_ra_include_stock_qty
      --
      ,d_model.after_model_ra_stock_qty as in_org_after_model_ra_stock_qty
      ,d_model.after_model_ra_onroad_stock_qty as in_org_after_model_ra_onroad_stock_qty
      ,d_model.after_ra_sub_stock_qty as in_org_after_model_ra_sub_stock_qty
      ,d_model.after_model_ra_include_stock_qty as in_org_after_model_ra_include_stock_qty
      --
      ,d.onroad_stock_qty in_org_onroad_stock_qty
      ,d.onorder_in_stock_qty in_org_onorder_in_stock_qty
      ,d.onorder_out_stock_qty in_org_onorder_out_stock_qty
      ,e.store_level out_store_level,e.city out_store_city,i.history_first_distribution_date out_store_history_first_distribution_date,
      i.first_distribution_date out_store_first_distribution_date,i.last_7days_sales_qty out_org_last_7days_sales_qty,
      i.last_7_14days_sales_qty out_org_last_7_14days_sales_qty,i.total_sales_qty out_org_total_sales_qty
      --
      ,i.before_ra_stock_qty as out_org_before_ra_stock_qty
      ,i.before_ra_onroad_stock_qty as out_org_before_ra_onroad_stock_qty
      ,i.before_ra_sub_stock_qty as out_org_before_ra_sub_stock_qty
      ,i.before_ra_include_stock_qty as out_org_before_ra_include_stock_qty
      --
      ,i_model.after_model_ra_stock_qty as out_org_after_model_ra_stock_qty
      ,i_model.after_model_ra_onroad_stock_qty as out_org_after_model_ra_onroad_stock_qty
      ,i_model.after_ra_sub_stock_qty as out_org_after_ra_sub_stock_qty
      ,i_model.after_model_ra_include_stock_qty as  out_org_after_model_ra_include_stock_qty
      --
      ,i.onroad_stock_qty as out_org_onroad_stock_qty
      ,i.onorder_in_stock_qty out_org_onorder_in_stock_qty,i.onorder_out_stock_qty out_org_onorder_out_stock_qty
      ,d.forecast_available_stock_qty as in_org_forecast_available_stock_qty
      ,i.forecast_available_stock_qty as out_org_forecast_available_stock_qty
      ,d.committed_onorder_out_qty as in_org_committed_onorder_out_qty
      ,i.committed_onorder_out_qty as out_org_committed_onorder_out_qty
      ,a.model_ra_qty,a.human_ra_qty,a.scene_code,a.scene_name,a.ra_source,a.commit_status,a.modify_status,a.skc_order_id order_id,
      a.is_effective,null creator,s.commit_user_name as commit_user_name,a.remark,current_timestamp update_time,current_timestamp etl_time
      ,coalesce(s.day_date,'{day_date}') as day_date
      ,a.reserved1 as reserved1, x.reserved9 as reserved2,x.reserved10 as reserved3
      ,p.label_value as reserved4 --厚薄
      ,q.band as reserved5   --调入方波段
      ,r.band as reserved6   --调出方波段
      ,case when f.org_flag='1' then f.reserved4  else null end as reserved7  --调入门店管理人
      ,case when e.org_flag='1' then e.reserved4  else null end as reserved8  --调出门店管理人
      ,case when a.reserved1 is not null and a.reserved1 not in ('','None') then a.reserved1
      when a.reserved9 is not null and a.reserved9 not in ('','None') then a.reserved9
      else null end as reserved9 --'预留字段9'
      ,case when a.reserved1 is not null and a.reserved1 not in ('','None') then 'plan_id'
      when a.reserved9 is not null and a.reserved9 not in ('','None') then 'collection_id'
      else null end as reserved10 --'预留字段10'
      ,b.gender as reserved11
      --,null reserved12,null reserved13,null reserved14,null reserved15,null reserved16,null reserved17,null reserved18,null reserved19,null reserved20

    ,a.color_name --'颜色名称'
    ,a.national_skc_total_sales_qty --'全国SKC累计销量'
    ,a.national_skc_last_7days_sales_qty	--'全国SKC近7天销量'
    ,a.national_skc_last_7_14days_sales_qty	--'全国SKC近7-14天销量'
    ,a.allot_out_manager_org_skc_total_sales_qty --'调出区域skc累计销量'
    ,a.allot_in_manager_org_skc_total_sales_qty --'调入区域skc累计销量'
    ,a.allot_out_manager_org_skc_last_7days_sales_qty --'调出区域skc近7天销量'
    ,a.allot_in_manager_org_skc_last_7days_sales_qty --'调入区域skc近7天销量'
    ,a.allot_out_manager_org_skc_last_7_14days_sales_qty --'调出区域skc近7-14天销量'
    ,a.allot_in_manager_org_skc_last_7_14days_sales_qty --'调入区域skc近7-14天销量'
    ,a.national_suit_total_sales_qty --'全国套装累计销量'
    ,a.national_suit_last_7days_sales_qty --'全国套装近7天销量'
    ,a.national_suit_last_7_14days_sales_qty --'全国套装近7-14天销量'
    ,a.allot_out_org_suit_last_7days_sales_qty --'调出门店套装近7天销量'
    ,a.allot_in_org_suit_last_7days_sales_qty --'调入门店套装近7天销量'
    ,a.allot_out_org_suit_last_7_14days_sales_qty --'调出门店套装近7-14天销量'
    ,a.allot_in_org_suit_last_7_14days_sales_qty --'调入门店套装近7-14天销量'
    ,a.allot_out_org_suit_total_sales_qty --'调出门店套装累计销量'
    ,a.allot_in_org_suit_total_sales_qty --'调入门店套装累计销量'
    ,a.allot_out_manager_org_suit_last_7days_sales_qty --'调出区域套装近7天销量'
    ,a.allot_in_manager_org_suit_last_7days_sales_qty --'调入区域套装近7天销量'
    ,a.allot_out_manager_org_suit_last_7_14days_sales_qty --'调出区域套装近7-14天销量'
    ,a.allot_in_manager_org_suit_last_7_14days_sales_qty --'调入区域套装近7-14天销量'
    ,a.allot_out_manager_org_suit_total_sales_qty --'调出区域套装累计销量'
    ,a.allot_in_manager_org_suit_total_sales_qty --'调入区域套装累计销量'
    ,a.allot_out_org_all_last_7days_sales_qty --'调出门店近7天销量'
    ,a.allot_in_org_all_last_7days_sales_qty --'调入门店近7天销量'
    ,a.allot_out_org_all_last_7_14days_sales_qty --'调出门店近7-14天销量'
    ,a.allot_in_org_all_last_7_14days_sales_qty --'调入门店近7-14天销量'

    ,a.allot_out_org_spu_total_sales_qty
    ,a.allot_out_org_spu_last_7days_sales_qty
    ,a.allot_out_org_spu_last_7_14days_sales_qty
    ,a.allot_in_org_spu_total_sales_qty
    ,a.allot_in_org_spu_last_7days_sales_qty
    ,a.allot_in_org_spu_last_7_14days_sales_qty

    ,b.product_name
    ,x.virtual_suit_code
    ,b.big_class
    ,b.mid_class
    ,b.tiny_class
    ,d.after_ra_wh_sub_stock_total_qty as in_org_after_ra_wh_sub_stock_total_qty
    ,i.after_ra_wh_sub_stock_total_qty as out_org_after_ra_wh_sub_stock_total_qty
    ,s.biz_action_template_code
    ,s.biz_action_template_name
    ,s.org_sk
    ,s.step_id --步骤id
    ,s.batch_id --批次id
    ,zf.org_name --所属组织名称
    ,s.job_id --任务id
    ,s.task_status --任务状态
    ,zd.manager_org_code as model_allot_out_up_org_code --模型调出方上级区域编码
    ,ze.manager_org_code as model_allot_in_up_org_code --模型调入方上级区域编码
    ,zb.manager_org_code as human_allot_out_up_org_code --人工调出方上级区域编码
    ,zc.manager_org_code as human_allot_in_up_org_code --人工调入方上级区域编码
    ,t.model_exception_type as model_exception_type --异常类型 （模型）
    ,t.human_exception_type as human_exception_type --异常类型 （人工）
    ,a.document_code
    ,zb.org_flag as out_flag
    ,zc.org_flag as in_flag

    from tmp2_kpi a
    left join tmp_skc_info b on a.skc_sk=b.skc_sk

    left join tmp4 d on a.human_allot_in_org_sk=d.stockorg_sk and a.skc_sk=d.skc_sk
      and a.scene_code=d.scene_code and a.ra_source=d.ra_source
    left join tmp4 i on a.human_allot_out_org_sk=i.stockorg_sk and a.skc_sk=i.skc_sk
      and a.scene_code=i.scene_code and a.ra_source=i.ra_source

    left join tmp4 d_model on a.model_allot_in_org_sk=d_model.stockorg_sk and a.skc_sk=d_model.skc_sk
      and a.scene_code=d_model.scene_code and a.ra_source=d_model.ra_source
    left join tmp4 i_model on a.model_allot_out_org_sk=i_model.stockorg_sk and a.skc_sk=i_model.skc_sk
      and a.scene_code=i_model.scene_code and a.ra_source=i_model.ra_source

    left join tmp_org_integration e on a.human_allot_out_org_sk=e.store_sk and a.biz_action_template_code=e.biz_action_template_code
    left join tmp_org_integration f on a.human_allot_in_org_sk=f.store_sk and a.biz_action_template_code=f.biz_action_template_code

    left join tmp_org_integration g on a.model_allot_out_org_sk=g.store_sk and a.biz_action_template_code=g.biz_action_template_code
    left join tmp_org_integration h on a.model_allot_in_org_sk=h.store_sk and a.biz_action_template_code=h.biz_action_template_code

    left join tmp6 o on a.skc_sk=o.skc_sk
    inner join tmp_virtual_suit_code x on a.skc_sk=x.skc_sk

    left join gp.tenant_peacebird_biz.dim_skp_label p on p.product_code=b.product_code and p.label_source='01' and p.label_code='00014'
    left join gp.tenant_peacebird_biz.dim_ra_life_cycle q on a.skc_sk=q.skc_sk and a.human_allot_in_org_sk=q.store_sk and q.day_date='{day_date}'
    left join gp.tenant_peacebird_biz.dim_ra_life_cycle r on a.skc_sk=r.skc_sk and a.human_allot_out_org_sk=r.store_sk and r.day_date='{day_date}'
    left join tmp_sku_org_detail s on a.skc_order_id=s.skc_order_id
    left join tmp_rst_ra_skc_org_detail_kpi t on a.skc_order_id=t.order_id

    left join tmp_org_info zb on a.human_allot_out_org_sk=zb.store_sk and b.brand_code=zb.brand_code
    left join tmp_org_info zc on a.human_allot_in_org_sk=zc.store_sk and b.brand_code=zc.brand_code
    left join tmp_org_info zd on a.model_allot_out_org_sk=zd.store_sk and b.brand_code=zd.brand_code
    left join tmp_org_info ze on a.model_allot_in_org_sk=ze.store_sk and b.brand_code=ze.brand_code
    left join tmp_dim_org zf on s.org_sk=zf.org_sk
    ;"""
exesql(sql)	

#%%
sql=f"""
    ----批次更新
    --tmp_batch_id 批次id
    drop table if exists tmp_batch_id;
    create table tmp_batch_id as
    select store_sk::text as stockorg_sk
      ,skc_sk::text skc_sk
      ,biz_action_template_code::text as biz_action_template_code
      ,batch_id::int as batch_id
      ,min(template_before_model_ra_stock_qty) as template_before_model_ra_stock_qty
      ,min(template_before_model_ra_onroad_stock_qty) as template_before_model_ra_onroad_stock_qty
      ,min(template_before_model_ra_sub_stock_qty) as template_before_model_ra_sub_stock_qty
      ,min(template_before_model_ra_include_stock_qty) as template_before_model_ra_include_stock_qty
      ,min(template_after_model_ra_stock_qty) as template_after_model_ra_stock_qty
      ,min(template_after_model_ra_onroad_stock_qty) as template_after_model_ra_onroad_stock_qty
      ,min(template_after_model_ra_sub_stock_qty) as template_after_model_ra_sub_stock_qty
      ,min(template_after_model_ra_include_stock_qty) as template_after_model_ra_include_stock_qty
      --模板调出方人工补调前/后库存
      ,min(template_before_human_ra_stock_qty) as template_before_human_ra_stock_qty
      ,min(template_before_human_ra_onroad_stock_qty) as template_before_human_ra_onroad_stock_qty
      ,min(template_before_human_ra_sub_stock_qty) as template_before_human_ra_sub_stock_qty
      ,min(template_before_human_ra_include_stock_qty) as template_before_human_ra_include_stock_qty
      ,min(template_after_human_ra_stock_qty) as template_after_human_ra_stock_qty
      ,min(template_after_human_ra_onroad_stock_qty) as template_after_human_ra_onroad_stock_qty
      ,min(template_after_human_ra_sub_stock_qty) as template_after_human_ra_sub_stock_qty
      ,min(template_after_human_ra_include_stock_qty) as template_after_human_ra_include_stock_qty
    from tmp_rst_ra_skc_org_detail
    group by stockorg_sk,skc_sk,biz_action_template_code,batch_id
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_template_code_batch_id 模板对应批次
    drop table if exists tmp_template_code_batch_id;
    create table tmp_template_code_batch_id as
    select biz_action_template_code,max(batch_id::int) as batch_id
    from tmp_batch_id
    group by biz_action_template_code
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_batch_id_upd;
    create table tmp_batch_id_upd as
    select a.skc_order_id
      ,a.human_allot_out_org_sk
      ,a.human_allot_in_org_sk
      ,a.skc_sk
      ,a.batch_id::text as batch_id
      ,min(b.template_before_model_ra_stock_qty)::json as template_in_org_before_model_ra_stock_qty
      ,min(b.template_before_model_ra_onroad_stock_qty)::json as template_in_org_before_model_ra_onroad_stock_qty
      ,min(b.template_before_model_ra_sub_stock_qty)::json as template_in_org_before_model_ra_sub_stock_qty
      ,min(b.template_before_model_ra_include_stock_qty)::json as template_in_org_before_model_ra_include_stock_qty
      ,min(b.template_after_model_ra_stock_qty)::json as template_in_org_after_model_ra_stock_qty
      ,min(b.template_after_model_ra_onroad_stock_qty)::json as template_in_org_after_model_ra_onroad_stock_qty
      ,min(b.template_after_model_ra_sub_stock_qty)::json as template_in_org_after_model_ra_sub_stock_qty
      ,min(b.template_after_model_ra_include_stock_qty)::json as template_in_org_after_model_ra_include_stock_qty

      ,min(c.template_before_model_ra_stock_qty)::json as template_out_org_before_model_ra_stock_qty
      ,min(c.template_before_model_ra_onroad_stock_qty)::json as template_out_org_before_model_ra_onroad_stock_qty
      ,min(c.template_before_model_ra_sub_stock_qty)::json as template_out_org_before_model_ra_sub_stock_qty
      ,min(c.template_before_model_ra_include_stock_qty)::json as template_out_org_before_model_ra_include_stock_qty
      ,min(c.template_after_model_ra_stock_qty)::json as template_out_org_after_model_ra_stock_qty
      ,min(c.template_after_model_ra_onroad_stock_qty)::json as template_out_org_after_model_ra_onroad_stock_qty
      ,min(c.template_after_model_ra_sub_stock_qty)::json as template_out_org_after_model_ra_sub_stock_qty
      ,min(c.template_after_model_ra_include_stock_qty)::json as template_out_org_after_model_ra_include_stock_qty
    from tmp2_kpi a
    left join tmp_batch_id b on a.skc_sk=b.skc_sk and a.model_allot_in_org_sk=b.stockorg_sk and a.batch_id=b.batch_id
    left join tmp_batch_id c on a.skc_sk=c.skc_sk and a.model_allot_out_org_sk=c.stockorg_sk and a.batch_id=c.batch_id
    --left join tmp_batch_id d on a.skc_sk=d.skc_sk and a.human_allot_in_org_sk=d.stockorg_sk and a.batch_id=d.batch_id
    group by a.skc_order_id,a.human_allot_out_org_sk,a.human_allot_in_org_sk,a.skc_sk,a.batch_id
    ;"""
exesql(sql)	

#%%

sql=f"""    ----人工补调前/后库存更新-模板
    ----模板粒度库存指标
    --tmp_mod_sku_ra 获取模型输出
    drop table if exists tmp_mod_sku_ra;
    create table tmp_mod_sku_ra as
    select human_allot_out_org_sk
    ,human_allot_in_org_sk
    ,skc_sk
    ,sku_sk
    ,human_ra_qty
    ,biz_action_template_code
    ,batch_id
    ,day_date
    from tmp_sku_target
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_mod_sku_org_bef 转置sku-org，用于累和
    drop table if exists tmp_mod_sku_org_bef;
    create table tmp_mod_sku_org_bef as
    select human_allot_out_org_sk as org_sk,skc_sk,sku_sk,biz_action_template_code,batch_id,coalesce(human_ra_qty,0)*-1 as qty,day_date
    from tmp_mod_sku_ra
    union all
    select human_allot_in_org_sk as org_sk,skc_sk,sku_sk,biz_action_template_code,batch_id,coalesce(human_ra_qty,0)*1 as qty,day_date
    from tmp_mod_sku_ra
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_mod_sku_org 剔除空门店
    drop table if exists tmp_mod_sku_org;
    create table tmp_mod_sku_org as
    select org_sk,skc_sk,sku_sk,biz_action_template_code,batch_id,qty,day_date
    from tmp_mod_sku_org_bef
    where org_sk is not null
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_mod_sku_org_bef;
    --tmp_template_row_num 模板按max(批次)排序，用于计算累和顺序 --相同模板多个批次指标会有问题
    drop table if exists tmp_template_row_num;
    create table tmp_template_row_num as
    select biz_action_template_code,batch_id,day_date
    ,row_number()over(partition by day_date order by batch_id) as row_num
    from tmp_mod_sku_ra
    where batch_id is not null
    group by biz_action_template_code,batch_id,day_date
    ;"""
exesql(sql)	

#%%
sql=f"""   drop table if exists tmp_mod_sku_ra;
    --tmp_mod_sku_org_template_pre 生成sku-org模板序列
    drop table if exists tmp_mod_sku_org_template_pre;
    create table tmp_mod_sku_org_template_pre as
    select a.org_sk,a.skc_sk,a.sku_sk,b.biz_action_template_code,a.batch_id,a.day_date
    from tmp_mod_sku_org a
    left join tmp_template_row_num b on a.day_date=b.day_date
    group by a.org_sk,a.skc_sk,a.sku_sk,b.biz_action_template_code,a.batch_id,a.day_date
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_mod_sku_org_template 关联模板款店补调量
    drop table if exists tmp_mod_sku_org_template;
    create table tmp_mod_sku_org_template as
    select a.org_sk,a.skc_sk,a.sku_sk,a.biz_action_template_code,a.batch_id,sum(c.qty) as qty,a.day_date
    from tmp_mod_sku_org_template_pre a
    left join tmp_mod_sku_org c on a.org_sk=c.org_sk and a.sku_sk=c.sku_sk and a.biz_action_template_code=c.biz_action_template_code and a.batch_id=c.batch_id and a.day_date=c.day_date
    group by a.org_sk,a.skc_sk,a.sku_sk,a.biz_action_template_code,a.batch_id,a.day_date
    ;"""
exesql(sql)	

#%%
sql=f""" 
    --生成sku-org模板补调量
    drop table if exists tmp_mod_sku_org_result;
    create table tmp_mod_sku_org_result as
    select a.org_sk,a.skc_sk,a.sku_sk
    ,a.biz_action_template_code,a.batch_id
    ,a.qty as move_qty
    ,a.day_date
    ,c.row_num
    ,sum(a.qty)over(partition by a.day_date,a.org_sk,a.sku_sk order by c.row_num) as sum_over_qty
    from tmp_mod_sku_org_template a
    --left join tmp_mod_sku_org b on a.org_sk=b.org_sk and a.sku_sk=b.sku_sk and a.biz_action_template_code=b.biz_action_template_code
    left join tmp_template_row_num c on a.biz_action_template_code=c.biz_action_template_code and a.batch_id=c.batch_id and a.day_date=c.day_date
    --where a.org_sk='10406335' and a.sku_sk='10247164'
    ;"""
exesql(sql)	

#%%
sql=f"""    drop table if exists tmp_template_row_num;
    --生成skc-org模板补调量
    drop table if exists tmp_mod_skc_org_result;
    create table tmp_mod_skc_org_result as
    select a.org_sk,a.skc_sk,a.biz_action_template_code,a.batch_id,a.day_date,a.row_num
    from tmp_mod_sku_org_result a
    group by a.org_sk,a.skc_sk,a.biz_action_template_code,a.batch_id,a.day_date,a.row_num
    ;"""
exesql(sql)	

#%%
sql=f"""    --生成sku-org模板补调量 --补齐尺码
    drop table if exists tmp_mod_sku_org_result_1;
    create table tmp_mod_sku_org_result_1 as
    select a.org_sk,a.skc_sk,b.sku_sk
    ,a.biz_action_template_code,a.batch_id
    ,a.day_date
    ,a.row_num
    from tmp_mod_skc_org_result a
    left join tmp_sku_skc b on a.skc_sk=b.skc_sk
    --where a.skc_sk=b.skc_sk
    --order by a.org_sk,a.skc_sk,b.sku_sk,a.biz_action_template_code
    ;"""
exesql(sql)	

#%%
sql=f"""    --生成sku-org模板补调量 --补齐尺码,关联数量
    drop table if exists tmp_mod_sku_org_result_2;
    create table tmp_mod_sku_org_result_2 as
    select a.org_sk,a.skc_sk,a.sku_sk
    ,a.biz_action_template_code,a.batch_id
    ,b.move_qty
    ,a.day_date
    ,a.row_num
    ,b.sum_over_qty
    from tmp_mod_sku_org_result_1 a
    left join tmp_mod_sku_org_result b on a.org_sk=b.org_sk
      and a.sku_sk=b.sku_sk and a.biz_action_template_code=b.biz_action_template_code and a.batch_id=b.batch_id
    ;"""
exesql(sql)	

#%%
sql=f""" 
    --tmp_sku_org_data_kpi sku-org库存指标
    drop table if exists tmp_sku_org_data_kpi;
    create  table tmp_sku_org_data_kpi as
    select a.org_sk
          ,a.skc_sk
          ,a.sku_sk
          ,a.size_code
          ,a.before_ra_stock_qty as before_ra_stock_qty --'补调前库存'
          ,a.before_ra_onroad_stock_qty as before_ra_onroad_stock_qty --补调前库存（含在途）
          ,a.before_ra_sub_stock_qty as before_ra_sub_stock_qty --'补调前库存（减在单出）'
          ,a.before_ra_include_stock_qty as before_ra_include_stock_qty --补调前库存（含在单在途）
    from tmp_ra_sku_org_data_filter a
    ;"""
exesql(sql)	

#%%
sql=f"""  
    --tmp_org_sku_template_kpi sku-org库存指标
    drop table if exists tmp_org_sku_template_kpi;
    create  table tmp_org_sku_template_kpi as
    select a.org_sk,a.skc_sk,a.sku_sk
    ,a.biz_action_template_code,a.batch_id
    ,c.size_code
    ,a.day_date
    --补调前库存=期初+leg(sum_over())
    ,coalesce(b.before_ra_stock_qty,0)+coalesce(lag(a.sum_over_qty::int,1,0) over(partition by a.org_sk,a.sku_sk order by a.row_num),0) as template_before_model_ra_stock_qty --'补调前库存'
    ,coalesce(b.before_ra_onroad_stock_qty,0)+coalesce(lag(a.sum_over_qty::int,1,0) over(partition by a.org_sk,a.sku_sk order by a.row_num),0) as template_before_model_ra_onroad_stock_qty --补调前库存（含在途）
    ,coalesce(b.before_ra_sub_stock_qty,0)+coalesce(lag(a.sum_over_qty::int,1,0) over(partition by a.org_sk,a.sku_sk order by a.row_num),0) as template_before_model_ra_sub_stock_qty --'补调前库存（减在单出）'
    ,coalesce(b.before_ra_include_stock_qty,0)+coalesce(lag(a.sum_over_qty::int,1,0) over(partition by a.org_sk,a.sku_sk order by a.row_num),0) as template_before_model_ra_include_stock_qty --补调前库存（含在单在途）
    --
    ,coalesce(lag(a.sum_over_qty::int,1,0) over(partition by a.org_sk,a.sku_sk order by a.row_num),0) as lag_sum_over_qty
    ,coalesce(a.sum_over_qty,0) as sum_over_qty
    --补调后库存=期初+    sum_over()
    ,coalesce(b.before_ra_stock_qty,0)+coalesce(a.sum_over_qty,0) as template_after_model_ra_stock_qty --'补调后库存'
    ,coalesce(b.before_ra_onroad_stock_qty,0)+coalesce(a.sum_over_qty,0) as template_after_model_ra_onroad_stock_qty --补调后库存（含在途）
    ,coalesce(b.before_ra_sub_stock_qty,0)+coalesce(a.sum_over_qty,0) as template_after_model_ra_sub_stock_qty --'补调后库存（减在单出）'
    ,coalesce(b.before_ra_include_stock_qty,0)+coalesce(a.sum_over_qty,0) as template_after_model_ra_include_stock_qty --补调后库存（含在单在途）

    from tmp_mod_sku_org_result_2 a
    left join tmp_sku_org_data_kpi b on a.org_sk=b.org_sk and a.sku_sk=b.sku_sk
    inner join tmp_sku_skc c on a.sku_sk=c.sku_sk
    --where a.org_sk='10406335' and a.sku_sk='10247164'
    ;"""
exesql(sql)	
# 0.03
#%% 
sql=f"""   --tmp_org_sku_template_kpi_total sku-org库存指标+total尺码
    drop table if exists tmp_org_sku_template_kpi_total;
    create  table tmp_org_sku_template_kpi_total as
    select a.org_sk,a.skc_sk,a.day_date
    ,a.biz_action_template_code,a.batch_id
    ,a.size_code
    ,sum(a.template_before_model_ra_stock_qty) as template_before_model_ra_stock_qty --'补调前库存'
    ,sum(a.template_before_model_ra_onroad_stock_qty) as template_before_model_ra_onroad_stock_qty --补调前库存（含在途）
    ,sum(a.template_before_model_ra_sub_stock_qty) as template_before_model_ra_sub_stock_qty --'补调前库存（减在单出）'
    ,sum(a.template_before_model_ra_include_stock_qty) as template_before_model_ra_include_stock_qty --补调前库存（含在单在途）

    ,sum(a.template_after_model_ra_stock_qty) as template_after_model_ra_stock_qty --'补调后库存'
    ,sum(a.template_after_model_ra_onroad_stock_qty) as template_after_model_ra_onroad_stock_qty --补调后库存（含在途）
    ,sum(a.template_after_model_ra_sub_stock_qty) as template_after_model_ra_sub_stock_qty --'补调后库存（减在单出）'
    ,sum(a.template_after_model_ra_include_stock_qty) as template_after_model_ra_include_stock_qty --补调后库存（含在单在途）
    from tmp_org_sku_template_kpi a
    group by grouping sets((1,2,3,4,5,6),(1,2,3,4,5))
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_org_sku_template_kpi_json_pre sku-org库存指标+json
    drop table if exists tmp_org_sku_template_kpi_json_pre;
    create  table tmp_org_sku_template_kpi_json_pre as
    select a.org_sk,a.skc_sk
    ,a.biz_action_template_code,a.batch_id
    ,a.day_date,
    json_group_array(json_object('key', a.size_code, 'value', a.template_before_model_ra_stock_qty)) AS template_before_model_ra_stock_qty, --'补调前库存'
    json_group_array(json_object('key', a.size_code, 'value', a.template_before_model_ra_onroad_stock_qty)) AS template_before_model_ra_onroad_stock_qty, --补调前库存（含在途）
    json_group_array(json_object('key', a.size_code, 'value', a.template_before_model_ra_sub_stock_qty)) AS template_before_model_ra_sub_stock_qty, --'补调前库存（减在单出）'
    json_group_array(json_object('key', a.size_code, 'value', a.template_before_model_ra_include_stock_qty)) AS template_before_model_ra_include_stock_qty, --补调前库存（含在单在途）
    --
    json_group_array(json_object('key', a.size_code, 'value', a.template_after_model_ra_stock_qty)) AS template_after_model_ra_stock_qty, --'补调后库存'
    json_group_array(json_object('key', a.size_code, 'value', a.template_after_model_ra_onroad_stock_qty)) AS template_after_model_ra_onroad_stock_qty, --补调后库存（含在途）
    json_group_array(json_object('key', a.size_code, 'value', a.template_after_model_ra_sub_stock_qty)) AS template_after_model_ra_sub_stock_qty, --'补调后库存（减在单出）'
    json_group_array(json_object('key', a.size_code, 'value', a.template_after_model_ra_include_stock_qty)) AS template_after_model_ra_include_stock_qty --补调前库存（含在单在途）
from tmp_org_sku_template_kpi_total a
    group by a.org_sk,a.skc_sk,a.biz_action_template_code,a.batch_id,a.day_date
    ;"""
exesql(sql)	

#%%
sql="""drop table if exists tmp_org_sku_template_kpi_json;
    create  table tmp_org_sku_template_kpi_json as
    select a.org_sk,a.skc_sk
    ,a.biz_action_template_code,a.batch_id
    ,a.day_date
    ,(case when b.org_sk is not null then c.template_before_human_ra_stock_qty::json else a.template_before_model_ra_stock_qty end) as template_before_model_ra_stock_qty--'补调前库存'
    ,(case when b.org_sk is not null then c.template_before_human_ra_onroad_stock_qty::json else a.template_before_model_ra_onroad_stock_qty end) as template_before_model_ra_onroad_stock_qty --补调前库存（含在途）
    ,(case when b.org_sk is not null then c.template_before_human_ra_sub_stock_qty::json else a.template_before_model_ra_sub_stock_qty end) as template_before_model_ra_sub_stock_qty --'补调前库存（减在单出）'
    ,(case when b.org_sk is not null then c.template_before_human_ra_include_stock_qty::json else a.template_before_model_ra_include_stock_qty end) as template_before_model_ra_include_stock_qty --补调前库存（含在单在途）

    ,(case when b.org_sk is not null then c.template_after_human_ra_stock_qty::json else a.template_after_model_ra_stock_qty end) as template_after_model_ra_stock_qty --'补调后库存'
    ,(case when b.org_sk is not null then c.template_after_human_ra_onroad_stock_qty::json else a.template_after_model_ra_onroad_stock_qty end) as template_after_model_ra_onroad_stock_qty --补调后库存（含在途）
    ,(case when b.org_sk is not null then c.template_after_human_ra_sub_stock_qty::json else a.template_after_model_ra_sub_stock_qty end) as template_after_model_ra_sub_stock_qty --'补调后库存（减在单出）'
    ,(case when b.org_sk is not null then c.template_after_human_ra_include_stock_qty::json else a.template_after_model_ra_include_stock_qty end) as template_after_model_ra_include_stock_qty --补调前库存（含在单在途）
    from tmp_org_sku_template_kpi_json_pre a
    left join tmp_skc_org_store_c b on a.skc_sk=b.skc_sk and a.org_sk=b.org_sk
    left join tmp_batch_id c on a.skc_sk=c.skc_sk and a.org_sk=c.stockorg_sk and a.biz_action_template_code=c.biz_action_template_code and a.batch_id::int=c.batch_id::int
    
    ;"""
exesql(sql)	

#%%
sql=f""" 
    ----人工补调后库存更新
    ----非模板粒度库存指标
    --生成sku-org模板补调量
    drop table if exists tmp_mod_sku_org_result;
    create table tmp_mod_sku_org_result as
    select a.org_sk,a.skc_sk,a.sku_sk,a.day_date
    ,sum(a.qty) as move_qty
    from tmp_mod_sku_org_template a
    --where a.org_sk='10406335' and a.sku_sk='10247164'
    group by a.org_sk,a.skc_sk,a.sku_sk,a.day_date
    ;"""
exesql(sql)	

#%%
sql=f"""   
    --生成sku-org模板补调量 --补齐尺码
    drop table if exists tmp_mod_sku_org_result_size;
    create table tmp_mod_sku_org_result_size as
    select distinct a.org_sk,a.skc_sk,b.sku_sk,a.day_date
    from tmp_mod_sku_org_result a
    cross join tmp_sku_skc b
    where a.skc_sk=b.skc_sk
    order by a.org_sk,a.skc_sk,b.sku_sk,a.day_date
    ;"""
exesql(sql)	

#%%
sql=f"""    --生成sku-org模板补调量 --补齐尺码,关联数量
    drop table if exists tmp_mod_sku_org_result_2;
    create table tmp_mod_sku_org_result_2 as
    select a.org_sk,a.skc_sk,a.sku_sk,a.day_date
    ,b.move_qty as sum_over_qty
    from tmp_mod_sku_org_result_size a
    left join tmp_mod_sku_org_result b on a.org_sk=b.org_sk
      and a.sku_sk=b.sku_sk and a.day_date=b.day_date
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_org_sku_kpi sku-org库存指标
    drop table if exists tmp_org_sku_kpi;
    create  table tmp_org_sku_kpi as
    select a.org_sk,a.skc_sk,a.sku_sk
    ,c.size_code
    ,a.day_date
    --补调前库存=期初+leg(sum_over())
    ,coalesce(b.before_ra_stock_qty,0)+coalesce(a.sum_over_qty,0) as before_model_ra_stock_qty --'补调前库存'
    ,coalesce(b.before_ra_onroad_stock_qty,0)+coalesce(a.sum_over_qty,0) as before_model_ra_onroad_stock_qty --补调前库存（含在途）
    ,coalesce(b.before_ra_sub_stock_qty,0)+coalesce(a.sum_over_qty,0) as before_model_ra_sub_stock_qty --'补调前库存（减在单出）'
    ,coalesce(b.before_ra_include_stock_qty,0)+coalesce(a.sum_over_qty,0) as before_model_ra_include_stock_qty --补调前库存（含在单在途）
    --
    ,coalesce(a.sum_over_qty,0) as sum_over_qty
    --补调后库存=期初+    sum_over()
    ,coalesce(b.before_ra_stock_qty,0)+coalesce(a.sum_over_qty,0) as after_model_ra_stock_qty --'补调后库存'
    ,coalesce(b.before_ra_onroad_stock_qty,0)+coalesce(a.sum_over_qty,0) as after_model_ra_onroad_stock_qty --补调后库存（含在途）
    ,coalesce(b.before_ra_sub_stock_qty,0)+coalesce(a.sum_over_qty,0) as after_model_ra_sub_stock_qty --'补调后库存（减在单出）'
    ,coalesce(b.before_ra_include_stock_qty,0)+coalesce(a.sum_over_qty,0) as after_model_ra_include_stock_qty --补调后库存（含在单在途）

    from tmp_mod_sku_org_result_2 a
    left join tmp_sku_org_data_kpi b on a.org_sk=b.org_sk and a.sku_sk=b.sku_sk
    inner join tmp_sku_skc c on a.sku_sk=c.sku_sk
    --where a.org_sk='10406335' and a.sku_sk='10247164'
    ;"""
exesql(sql)	

#%%
sql=f"""    
    --tmp_org_sku_kpi_total sku-org库存指标+total尺码
    drop table if exists tmp_org_sku_kpi_total;
    create  table tmp_org_sku_kpi_total as
    select a.org_sk,a.skc_sk,a.day_date
    ,a.size_code
    ,sum(a.before_model_ra_stock_qty) as before_model_ra_stock_qty --'补调前库存'
    ,sum(a.before_model_ra_onroad_stock_qty) as before_model_ra_onroad_stock_qty --补调前库存（含在途）
    ,sum(a.before_model_ra_sub_stock_qty) as before_model_ra_sub_stock_qty --'补调前库存（减在单出）'
    ,sum(a.before_model_ra_include_stock_qty) as before_model_ra_include_stock_qty --补调前库存（含在单在途）

    ,sum(a.after_model_ra_stock_qty) as after_model_ra_stock_qty --'补调后库存'
    ,sum(a.after_model_ra_onroad_stock_qty) as after_model_ra_onroad_stock_qty --补调后库存（含在途）
    ,sum(a.after_model_ra_sub_stock_qty) as after_model_ra_sub_stock_qty --'补调后库存（减在单出）'
    ,sum(a.after_model_ra_include_stock_qty) as after_model_ra_include_stock_qty --补调后库存（含在单在途）
    from tmp_org_sku_kpi a
    group by grouping sets((1,2,3,4),(1,2,3))
    ;"""
exesql(sql)	

#%%
sql=f""" 
    --tmp_org_sku_kpi_json_pre sku-org库存指标+json
    drop table if exists tmp_org_sku_kpi_json_pre;
    create  table tmp_org_sku_kpi_json_pre as
    select a.org_sk,a.skc_sk,a.day_date
    ,json_group_array(json_object('key', a.size_code, 'value', a.before_model_ra_stock_qty)) AS before_model_ra_stock_qty, --'补调前库存'
    json_group_array(json_object('key', a.size_code, 'value', a.before_model_ra_onroad_stock_qty)) AS before_model_ra_onroad_stock_qty, --补调前库存（含在途）
    json_group_array(json_object('key', a.size_code, 'value', a.before_model_ra_sub_stock_qty)) AS before_model_ra_sub_stock_qty, --'补调前库存（减在单出）'
    json_group_array(json_object('key', a.size_code, 'value', a.before_model_ra_include_stock_qty)) AS before_model_ra_include_stock_qty, --补调前库存（含在单在途）
    --
    json_group_array(json_object('key', a.size_code, 'value', a.after_model_ra_stock_qty)) AS after_model_ra_stock_qty, --'补调后库存'
    json_group_array(json_object('key', a.size_code, 'value', a.after_model_ra_onroad_stock_qty)) AS after_model_ra_onroad_stock_qty, --补调后库存（含在途）
    json_group_array(json_object('key', a.size_code, 'value', a.after_model_ra_sub_stock_qty)) AS after_model_ra_sub_stock_qty, --'补调后库存（减在单出）'
    json_group_array(json_object('key', a.size_code, 'value', a.after_model_ra_include_stock_qty)) AS after_model_ra_include_stock_qty --补调前库存（含在单在途）
    from tmp_org_sku_kpi_total a
    group by a.org_sk,a.skc_sk,a.day_date
    ;"""
exesql(sql)	

#%%
sql=f"""    
    --tmp_org_sku_kpi_json sku-org库存指标+json
    drop table if exists tmp_org_sku_kpi_json;
    create  table tmp_org_sku_kpi_json as
    select a.org_sk,a.skc_sk,a.day_date
    ,a.before_model_ra_stock_qty --'补调前库存'
    ,a.before_model_ra_onroad_stock_qty --补调前库存（含在途）
    ,a.before_model_ra_sub_stock_qty --'补调前库存（减在单出）'
    ,a.before_model_ra_include_stock_qty --补调前库存（含在单在途）

    ,(case when b.org_sk is not null then c.after_human_ra_stock_qty::json else a.after_model_ra_stock_qty end) as after_model_ra_stock_qty --'补调后库存'
    ,(case when b.org_sk is not null then c.after_human_ra_onroad_stock_qty::json else a.after_model_ra_onroad_stock_qty end) as after_model_ra_onroad_stock_qty --补调后库存（含在途）
    ,(case when b.org_sk is not null then c.after_human_ra_sub_stock_qty::json else a.after_model_ra_sub_stock_qty end) as after_model_ra_sub_stock_qty --'补调后库存（减在单出）'
    ,(case when b.org_sk is not null then c.after_human_ra_include_stock_qty::json else a.after_model_ra_include_stock_qty end) as after_model_ra_include_stock_qty --补调前库存（含在单在途）

    from tmp_org_sku_kpi_json_pre a
    left join tmp_skc_org_store_c b on a.skc_sk=b.skc_sk and a.org_sk=b.org_sk
    left join tmp_skc_org_after_huaman_kpi c on a.skc_sk=c.skc_sk and a.org_sk=c.store_sk
    ;"""
exesql(sql)	

#%%
sql=f"""

    ----优先级评分更新
    --tmp_stock_move_score SKU-ORG收发评分
    drop table if exists tmp_stock_move_score;
    create  table tmp_stock_move_score as
    select batch_id
      ,skc_sk::text skc_sk
      ,store_sk::text as stockorg_sk
      ,min(send_priority_score) as send_priority_score
      ,min(receive_priority_score) as receive_priority_score
    from tmp_rst_ra_skc_org_detail
    group by batch_id,skc_sk,store_sk
    ;"""
exesql(sql)	

#%%
sql=f"""    
    --tmp_stock_move_score_upd 优先级评分upd
    drop table if exists tmp_stock_move_score_upd;
    create  table tmp_stock_move_score_upd as
    select a.skc_order_id
        ,za.send_priority_score::numeric(16,2) as in_org_send_priority_score --调入方发出优先级评分
        ,za.receive_priority_score::numeric(16,2) as in_org_receive_priority_score --调入方接收优先级评分
        ,z.send_priority_score::numeric(16,2) as out_org_send_priority_score --调出方发出优先级评分
        ,z.receive_priority_score::numeric(16,2) as out_org_receive_priority_score --调出方接收优先级评分
    from tmp_batch_id_upd a
    left join tmp_stock_move_score z on a.human_allot_out_org_sk=z.stockorg_sk and a.skc_sk=z.skc_sk and a.batch_id=z.batch_id
    left join tmp_stock_move_score za on a.human_allot_in_org_sk=za.stockorg_sk and a.skc_sk=za.skc_sk and a.batch_id=za.batch_id
    ;"""
exesql(sql)	

#%%
sql=f"""
--     --更新对应skc+org 优先级评分

    ----目标库存、满足率更新
    -- tmp_ra_skc_data_total skc组织data数据表
    drop table if exists tmp_ra_skc_data_total;
    create  table tmp_ra_skc_data_total as
    select  a.org_sk
            ,a.skc_sk
            ,sum(a.after_model_ra_include_stock_qty) as after_model_ra_include_stock_qty --'模型补调后库存（含在单在途）'
    from gp.tenant_peacebird_biz.rst_ra_sku_org_data a
    where a.day_date = '{day_date}' and a.after_model_ra_include_stock_qty>0
    group by a.org_sk,a.skc_sk"""
exesql(sql)	

#%%
sql=f"""   -- tmp_rst_ra_sku_org_detail_2 取sku表
    drop table if exists tmp_rst_ra_sku_org_detail_2;
    create  table tmp_rst_ra_sku_org_detail_2 as
    select
        a.skc_order_id
        ,a.skc_sk
        ,a.human_allot_out_org_sk
        ,a.human_allot_in_org_sk
        ,b.batch_id::text as batch_id
    from tmp_rst_ra_sku_org_detail a
    inner join tmp2_kpi b on a.skc_order_id=b.skc_order_id
    --left join tmp_template_code_batch_id c on a.biz_action_template_code=c.biz_action_template_code
    --where a.batch_id is not null
    group by a.skc_order_id,a.skc_sk,a.human_allot_out_org_sk,a.human_allot_in_org_sk,b.batch_id
    ;"""
exesql(sql)	

#%%
sql=f"""
    --获取相关单据决策日期 --用于过滤目标库存
    drop table if exists tmp_sku_target_date;
    create table tmp_sku_target_date as
    select day_date::timestamp as day_date
    from tmp_sku_target
    group by day_date
    ;"""
exesql(sql)	
# 2.19
#%%
sql=f"""  
    -- tmp_sku_store_target_stock SKU-门店目标库存结果表
    drop table if exists tmp_sku_store_target_stock;
    create table tmp_sku_store_target_stock as
    select a.operating_unit_sk
          ,a.skc_sk::text skc_sk
          ,a.stockorg_sk::text stockorg_sk
          ,a.batch_id
          ,b.size_code
          ,sum(a.target_stock) as target_stock
    from gp.tenant_peacebird_adm.gto_sku_store_target_stock a
    inner join tmp_sku_target_date d on a.day_date=d.day_date
    inner join tmp_sku_skc b on a.sku_sk::text=b.sku_sk
    inner join tmp_mod_sku_org_result_size c on a.stockorg_sk::text=c.org_sk and a.sku_sk::text=c.sku_sk and a.day_date=c.day_date::timestamp
    where a.day_date='{day_date}'::timestamp
    --and a.stockorg_sk='10062070' and a.skc_sk='10018881' and batch_id='132'
    group by grouping sets ((1,2,3,4,5),(1,2,3,4))
    ;"""
exesql(sql)	

#%%
sql=f""" 
    drop table if exists tmp_sku_store_target_stock_json;
    create table tmp_sku_store_target_stock_json as
    select a.skc_sk
      ,a.stockorg_sk
      ,a.batch_id,
json_group_array(json_object('key', a.size_code, 'value', a.target_stock)) AS target_stock --目标库存
    from tmp_sku_store_target_stock a
    group by a.skc_sk,a.stockorg_sk,a.batch_id
    ;"""
exesql(sql)	

#%%
sql=f"""    --tmp_ra_skc_org_total 生成skc粒度表，计算模型补调量total
    drop table if exists tmp_ra_skc_org_total;
    create  table tmp_ra_skc_org_total as
    select
        a.skc_order_id, a.skc_sk, 'total' as size_code
        ,a.human_allot_out_org_sk
        ,a.human_allot_in_org_sk
        ,f.target_stock as out_org_target_stock
        ,g.target_stock as in_org_target_stock
        ,h.target_stock as out_org_target_stock_json
        ,i.target_stock as in_org_target_stock_json
        ,a.batch_id::varchar as batch_id
    from tmp_rst_ra_sku_org_detail_2 a
    left join tmp_sku_store_target_stock f on a.skc_sk=f.skc_sk and a.human_allot_out_org_sk=f.stockorg_sk and a.batch_id=f.batch_id and f.size_code is null
    left join tmp_sku_store_target_stock g on a.skc_sk=g.skc_sk and a.human_allot_in_org_sk=g.stockorg_sk and a.batch_id=g.batch_id and g.size_code is null
    left join tmp_sku_store_target_stock_json h on a.skc_sk=h.skc_sk and a.human_allot_out_org_sk=h.stockorg_sk and a.batch_id=h.batch_id
    left join tmp_sku_store_target_stock_json i on a.skc_sk=i.skc_sk and a.human_allot_in_org_sk=i.stockorg_sk and a.batch_id=i.batch_id
    --group by a.skc_order_id, a.skc_sk, a.human_allot_out_org_sk, a.human_allot_in_org_sk
    ;"""
exesql(sql)	

#%%
sql=f"""  
    --tmp_ra_skc_org_fill_rate 目标库存、满足率
    drop table if exists tmp_ra_skc_org_fill_rate;
    create  table tmp_ra_skc_org_fill_rate as
    select
        a.skc_order_id, a.skc_sk
        ,a.human_allot_out_org_sk
        ,a.human_allot_in_org_sk
        ,a.out_org_target_stock_json as out_org_target_stock
        ,a.in_org_target_stock_json as in_org_target_stock
        ,b.after_model_ra_include_stock_qty as out_org_after_model_ra_include_stock_qty
        ,c.after_model_ra_include_stock_qty as in_org_after_model_ra_include_stock_qty
        ,case when a.out_org_target_stock=0 then 0 else (coalesce(b.after_model_ra_include_stock_qty,0)::numeric/a.out_org_target_stock::numeric)::numeric(16,2) end as out_org_fill_rate
        ,case when a.in_org_target_stock=0 then 0 else (coalesce(c.after_model_ra_include_stock_qty,0)::numeric/a.in_org_target_stock::numeric)::numeric(16,2) end  as in_org_fill_rate
        ,a.batch_id

    from tmp_ra_skc_org_total a
    left join tmp_ra_skc_data_total b on a.skc_sk=b.skc_sk and a.human_allot_out_org_sk=b.org_sk
    left join tmp_ra_skc_data_total c on a.skc_sk=c.skc_sk and a.human_allot_in_org_sk=c.org_sk
    ;"""
exesql(sql)	

#%%
sql=f"""  
    drop table if exists tmp_rst_ra_skc_org_detail_result_2;
    create table tmp_rst_ra_skc_org_detail_result_2 as
    select a.skc_sk
      ,a.skc_code
      ,a.product_code
      ,a.color_code
      ,a.brand_code
      ,a.brand_name
      ,a.product_year_quarter
      ,a.product_range
      ,a.band
      ,a.class_longcode
      ,a.suit_id
      ,a.tag_price
      ,a.out_sales_level_code
      ,a.out_sales_level_name
      ,a.in_sales_level_code
      ,a.in_sales_level_name
      ,a.sales_type
      ,a.size_group_code
      ,a.model_allot_out_manager_org_code
      ,a.model_allot_in_manager_org_code
      ,a.model_allot_out_manager_org_name --新增
      ,a.model_allot_in_manager_org_name --新增
      ,a.model_allot_out_org_sk
      ,a.model_allot_in_org_sk
      ,a.model_allot_out_org_code --新增
      ,a.model_allot_in_org_code --新增
      ,a.model_allot_out_org_name --新增
      ,a.model_allot_in_org_name --新增
      ,a.human_allot_out_manager_org_code
      ,a.human_allot_in_manager_org_code

      ,a.human_allot_out_manager_org_name
      ,a.human_allot_in_manager_org_name
      ,a.human_allot_out_org_sk
      ,a.human_allot_in_org_sk
      ,a.human_allot_out_org_code
      ,a.human_allot_in_org_code
      ,a.human_allot_out_org_name
      ,a.human_allot_in_org_name
      ,a.in_store_level
      ,a.in_store_city
      ,a.in_store_history_first_distribution_date
      ,a.in_store_first_distribution_date
      ,a.in_org_last_7days_sales_qty
      ,a.in_org_last_7_14days_sales_qty
      ,a.in_org_total_sales_qty
      ,a.in_org_before_ra_stock_qty
      ,a.in_org_before_ra_sub_stock_qty
      ,a.in_org_after_model_ra_stock_qty
      ,a.in_org_after_model_ra_include_stock_qty
      ,a.in_org_onroad_stock_qty
      ,a.in_org_onorder_in_stock_qty
      ,a.in_org_onorder_out_stock_qty
      ,a.out_store_level
      ,a.out_store_city
      ,a.out_store_history_first_distribution_date
      ,a.out_store_first_distribution_date
      ,a.out_org_last_7days_sales_qty
      ,a.out_org_last_7_14days_sales_qty
      ,a.out_org_total_sales_qty
      ,a.out_org_before_ra_stock_qty
      ,a.out_org_before_ra_sub_stock_qty
      ,a.out_org_after_model_ra_stock_qty
      ,a.out_org_after_model_ra_include_stock_qty
      ,a.out_org_onroad_stock_qty
      ,a.out_org_onorder_in_stock_qty
      ,a.out_org_onorder_out_stock_qty
      ,a.out_org_forecast_available_stock_qty
      ,a.out_org_committed_onorder_out_qty,a.model_ra_qty
      ,a.human_ra_qty
      ,a.scene_code
      ,a.scene_name
      ,a.ra_source
      ,a.commit_status
      ,a.modify_status
      ,a.order_id
      ,a.is_effective
      ,a.creator
      ,a.commit_user_name
      ,a.remark
      ,a.update_time
      ,a.etl_time
      ,a.day_date
      ,a.reserved1,a.reserved2,a.reserved3,a.reserved4,a.reserved5,a.reserved6,a.reserved7,a.reserved8,a.reserved9,a.reserved10
      ,a.reserved11--,a.reserved12,a.reserved13,a.reserved14,a.reserved15,a.reserved16,a.reserved17,a.reserved18,a.reserved19,a.reserved20

      ,a.color_name --'颜色名称'
      ,a.national_skc_total_sales_qty --'全国SKC累计销量'
      ,a.national_skc_last_7days_sales_qty	--'全国SKC近7天销量'
      ,a.national_skc_last_7_14days_sales_qty	--'全国SKC近7-14天销量'
      ,a.allot_out_manager_org_skc_total_sales_qty --'调出区域skc累计销量'
      ,a.allot_in_manager_org_skc_total_sales_qty --'调入区域skc累计销量'
      ,a.allot_out_manager_org_skc_last_7days_sales_qty --'调出区域skc近7天销量'
      ,a.allot_in_manager_org_skc_last_7days_sales_qty --'调入区域skc近7天销量'
      ,a.allot_out_manager_org_skc_last_7_14days_sales_qty --'调出区域skc近7-14天销量'
      ,a.allot_in_manager_org_skc_last_7_14days_sales_qty --'调入区域skc近7-14天销量'
      ,a.national_suit_total_sales_qty --'全国套装累计销量'
      ,a.national_suit_last_7days_sales_qty --'全国套装近7天销量'
      ,a.national_suit_last_7_14days_sales_qty --'全国套装近7-14天销量'
      ,a.allot_out_org_suit_last_7days_sales_qty --'调出门店套装近7天销量'
      ,a.allot_in_org_suit_last_7days_sales_qty --'调入门店套装近7天销量'
      ,a.allot_out_org_suit_last_7_14days_sales_qty --'调出门店套装近7-14天销量'
      ,a.allot_in_org_suit_last_7_14days_sales_qty --'调入门店套装近7-14天销量'
      ,a.allot_out_org_suit_total_sales_qty --'调出门店套装累计销量'
      ,a.allot_in_org_suit_total_sales_qty --'调入门店套装累计销量'
      ,a.allot_out_manager_org_suit_last_7days_sales_qty --'调出区域套装近7天销量'
      ,a.allot_in_manager_org_suit_last_7days_sales_qty --'调入区域套装近7天销量'
      ,a.allot_out_manager_org_suit_last_7_14days_sales_qty --'调出区域套装近7-14天销量'
      ,a.allot_in_manager_org_suit_last_7_14days_sales_qty --'调入区域套装近7-14天销量'
      ,a.allot_out_manager_org_suit_total_sales_qty --'调出区域套装累计销量'
      ,a.allot_in_manager_org_suit_total_sales_qty --'调入区域套装累计销量'
      ,a.allot_out_org_all_last_7days_sales_qty --'调出门店近7天销量'
      ,a.allot_in_org_all_last_7days_sales_qty --'调入门店近7天销量'
      ,a.allot_out_org_all_last_7_14days_sales_qty --'调出门店近7-14天销量'
      ,a.allot_in_org_all_last_7_14days_sales_qty --'调入门店近7-14天销量'
      ,a.allot_out_org_spu_total_sales_qty
      ,a.allot_out_org_spu_last_7days_sales_qty
      ,a.allot_out_org_spu_last_7_14days_sales_qty
      ,a.allot_in_org_spu_total_sales_qty
      ,a.allot_in_org_spu_last_7days_sales_qty
      ,a.allot_in_org_spu_last_7_14days_sales_qty

      ,a.product_name
      ,a.virtual_suit_code
      ,a.big_class
      ,a.mid_class
      ,a.tiny_class
      ,a.in_org_after_ra_wh_sub_stock_total_qty
      ,a.in_org_before_ra_onroad_stock_qty
      ,a.in_org_before_ra_include_stock_qty
      ,a.out_org_before_ra_include_stock_qty
      ,a.out_org_after_ra_sub_stock_qty
      ,a.out_org_after_ra_wh_sub_stock_total_qty
      ,a.biz_action_template_code
      ,a.biz_action_template_name
      ,a.org_sk
      ,a.step_id --步骤id
      ,a.batch_id --批次id
      ,a.org_name --所属组织名称
      ,a.job_id --任务id
      ,a.task_status --任务状态
      ,a.model_allot_out_up_org_code --模型调出方上级区域编码
      ,a.model_allot_in_up_org_code --模型调入方上级区域编码
      ,a.human_allot_out_up_org_code --人工调出方上级区域编码
      ,a.human_allot_in_up_org_code --人工调入方上级区域编码
      ,a.model_exception_type --异常类型 （模型）
      ,a.human_exception_type --异常类型 （人工）
      ,a.document_code --需求单号
      ,a.out_flag
      ,a.in_flag
      ,a.in_org_forecast_available_stock_qty --调入方预计可用库存
      ,a.in_org_after_model_ra_onroad_stock_qty --调入方模型补调后库存（含在途）
      ,a.in_org_after_model_ra_sub_stock_qty --调入方模型补调后库存（减在单出）
      ,a.out_org_before_ra_onroad_stock_qty --调出方补调前库存（含在途）
      ,a.out_org_after_model_ra_onroad_stock_qty --调出方模型补调后库存（含在途）
      ,a.in_org_committed_onorder_out_qty --调入方当日已提交在单出
      --更新对应skc+org 批次、模板调入/调出方补调前/后库存
      --,b.batch_id as batch_id
      ,b.template_in_org_before_model_ra_stock_qty as template_in_org_before_model_ra_stock_qty
      ,b.template_in_org_before_model_ra_onroad_stock_qty as template_in_org_before_model_ra_onroad_stock_qty
      ,b.template_in_org_before_model_ra_sub_stock_qty as template_in_org_before_model_ra_sub_stock_qty
      ,b.template_in_org_before_model_ra_include_stock_qty as template_in_org_before_model_ra_include_stock_qty
      ,b.template_in_org_after_model_ra_stock_qty as template_in_org_after_model_ra_stock_qty
      ,b.template_in_org_after_model_ra_onroad_stock_qty as template_in_org_after_model_ra_onroad_stock_qty
      ,b.template_in_org_after_model_ra_sub_stock_qty as template_in_org_after_model_ra_sub_stock_qty
      ,b.template_in_org_after_model_ra_include_stock_qty as template_in_org_after_model_ra_include_stock_qty

      ,b.template_out_org_before_model_ra_stock_qty as template_out_org_before_model_ra_stock_qty
      ,b.template_out_org_before_model_ra_onroad_stock_qty as template_out_org_before_model_ra_onroad_stock_qty
      ,b.template_out_org_before_model_ra_sub_stock_qty as template_out_org_before_model_ra_sub_stock_qty
      ,b.template_out_org_before_model_ra_include_stock_qty as template_out_org_before_model_ra_include_stock_qty
      ,b.template_out_org_after_model_ra_stock_qty as template_out_org_after_model_ra_stock_qty
      ,b.template_out_org_after_model_ra_onroad_stock_qty as template_out_org_after_model_ra_onroad_stock_qty
      ,b.template_out_org_after_model_ra_sub_stock_qty as template_out_org_after_model_ra_sub_stock_qty
      ,b.template_out_org_after_model_ra_include_stock_qty as template_out_org_after_model_ra_include_stock_qty
      --in_human 模板调入方人工补调前/后库存
      ,c.template_before_model_ra_stock_qty as template_in_org_before_human_ra_stock_qty
      ,c.template_before_model_ra_onroad_stock_qty as template_in_org_before_human_ra_onroad_stock_qty
      ,c.template_before_model_ra_sub_stock_qty as template_in_org_before_human_ra_sub_stock_qty
      ,c.template_before_model_ra_include_stock_qty as template_in_org_before_human_ra_include_stock_qty

      ,c.template_after_model_ra_stock_qty as template_in_org_after_human_ra_stock_qty
      ,c.template_after_model_ra_onroad_stock_qty as template_in_org_after_human_ra_onroad_stock_qty
      ,c.template_after_model_ra_sub_stock_qty as template_in_org_after_human_ra_sub_stock_qty
      ,c.template_after_model_ra_include_stock_qty as template_in_org_after_human_ra_include_stock_qty
      --out_human 模板调出方人工补调前/后库存
      ,d.template_before_model_ra_stock_qty as template_out_org_before_human_ra_stock_qty
      ,d.template_before_model_ra_onroad_stock_qty as template_out_org_before_human_ra_onroad_stock_qty
      ,d.template_before_model_ra_sub_stock_qty as template_out_org_before_human_ra_sub_stock_qty
      ,d.template_before_model_ra_include_stock_qty as template_out_org_before_human_ra_include_stock_qty

      ,d.template_after_model_ra_stock_qty as template_out_org_after_human_ra_stock_qty
      ,d.template_after_model_ra_onroad_stock_qty as template_out_org_after_human_ra_onroad_stock_qty
      ,d.template_after_model_ra_sub_stock_qty as template_out_org_after_human_ra_sub_stock_qty
      ,d.template_after_model_ra_include_stock_qty as template_out_org_after_human_ra_include_stock_qty
      --in_human 调入方人工补调前/后库存
      ,e.after_model_ra_stock_qty as in_org_after_human_ra_stock_qty
      ,e.after_model_ra_onroad_stock_qty as in_org_after_human_ra_onroad_stock_qty
      ,e.after_model_ra_sub_stock_qty as in_org_after_human_ra_sub_stock_qty
      ,e.after_model_ra_include_stock_qty as in_org_after_human_ra_include_stock_qty
      --out_human 调出方人工补调前/后库存
      ,f.after_model_ra_stock_qty as out_org_after_human_ra_stock_qty
      ,f.after_model_ra_onroad_stock_qty as out_org_after_human_ra_onroad_stock_qty
      ,f.after_model_ra_sub_stock_qty as out_org_after_human_ra_sub_stock_qty
      ,f.after_model_ra_include_stock_qty as out_org_after_human_ra_include_stock_qty
      --更新对应skc+org 优先级评分
      ,g.in_org_send_priority_score as in_org_send_priority_score
      ,g.in_org_receive_priority_score as in_org_receive_priority_score
      ,g.out_org_send_priority_score as out_org_send_priority_score
      ,g.out_org_receive_priority_score as out_org_receive_priority_score
      --更新对应skc+org 目标库存、满足率
      ,h.in_org_target_stock as in_org_target_stock
      ,h.in_org_fill_rate as in_org_fill_rate
      ,h.out_org_target_stock as out_org_target_stock
      ,h.out_org_fill_rate as out_org_fill_rate
    from tmp_rst_ra_skc_org_detail_result a
    left join tmp_batch_id_upd b on a.order_id=b.skc_order_id

    left join tmp_org_sku_template_kpi_json c on a.skc_sk=c.skc_sk
      and a.human_allot_in_org_sk=c.org_sk and a.biz_action_template_code=c.biz_action_template_code and a.batch_id::int=c.batch_id::int and a.day_date=c.day_date
    left join tmp_org_sku_template_kpi_json d on a.skc_sk=d.skc_sk
      and a.human_allot_out_org_sk=d.org_sk and a.biz_action_template_code=d.biz_action_template_code and a.batch_id::int=d.batch_id::int and a.day_date=d.day_date

    left join tmp_org_sku_kpi_json e on a.skc_sk=e.skc_sk and a.human_allot_in_org_sk=e.org_sk and a.day_date=e.day_date
    left join tmp_org_sku_kpi_json f on a.skc_sk=f.skc_sk and a.human_allot_out_org_sk=f.org_sk and a.day_date=f.day_date

    left join tmp_stock_move_score_upd g on a.order_id=g.skc_order_id
    left join tmp_ra_skc_org_fill_rate h on a.order_id=h.skc_order_id
    ;"""
exesql(sql)	


#%%
sql=f"""  
    delete from gp.tenant_peacebird_biz.rst_ra_skc_org_detail
    where order_id in (
      select skc_order_id as order_id from tmp2
    ) and is_deleted = '0' and  day_date = '{day_date}'
    ;"""
#exesql(sql)	
#%%
sql=f"""
    --结果表中插入数据
    insert into gp.tenant_peacebird_biz.rst_ra_skc_org_detail
    (skc_sk , skc_code, product_code, color_code, brand_code,brand_name,product_year_quarter,
    product_range, band,
    class_longcode,
    suit_id, tag_price,
    out_sales_level_code,
    out_sales_level_name,
    in_sales_level_code,
    in_sales_level_name,
    sales_type, size_group_code, model_allot_out_manager_org_code,
    model_allot_in_manager_org_code,
    model_allot_out_manager_org_name,--新增
    model_allot_in_manager_org_name,--新增
    model_allot_out_org_sk, model_allot_in_org_sk,
    model_allot_out_org_code ,--新增
    model_allot_in_org_code,--新增
    model_allot_out_org_name,--新增
    model_allot_in_org_name,--新增
    human_allot_out_manager_org_code, human_allot_in_manager_org_code,
    human_allot_out_manager_org_name, human_allot_in_manager_org_name,
    human_allot_out_org_sk, human_allot_in_org_sk, human_allot_out_org_code,
    human_allot_in_org_code, human_allot_out_org_name, human_allot_in_org_name,
    in_store_level, in_store_city, in_store_history_first_distribution_date,
    in_store_first_distribution_date, in_org_last_7days_sales_qty,
    in_org_last_7_14days_sales_qty, in_org_total_sales_qty,
    in_org_before_ra_stock_qty, in_org_before_ra_sub_stock_qty,
    in_org_after_model_ra_stock_qty, in_org_after_model_ra_include_stock_qty,
    in_org_onroad_stock_qty, in_org_onorder_in_stock_qty,
    in_org_onorder_out_stock_qty, out_store_level,
    out_store_city, out_store_history_first_distribution_date,
    out_store_first_distribution_date, out_org_last_7days_sales_qty,
    out_org_last_7_14days_sales_qty, out_org_total_sales_qty,
    out_org_before_ra_stock_qty, out_org_before_ra_sub_stock_qty,
    out_org_after_model_ra_stock_qty, out_org_after_model_ra_include_stock_qty,
    out_org_onroad_stock_qty, out_org_onorder_in_stock_qty, out_org_onorder_out_stock_qty,
    out_org_forecast_available_stock_qty, out_org_committed_onorder_out_qty, model_ra_qty,
    human_ra_qty, scene_code,scene_name, ra_source, commit_status,modify_status, order_id,
    is_effective, creator, commit_user_name, remark, update_time, etl_time, day_date,
    reserved1, reserved2, reserved3, reserved4, reserved5, reserved6, reserved7, reserved8, reserved9, reserved10,
    reserved11--,-- reserved12, reserved13, reserved14, reserved15, reserved16, reserved17, reserved18, reserved19, reserved20

    ,color_name --'颜色名称'
    ,national_skc_total_sales_qty --'全国SKC累计销量'
    ,national_skc_last_7days_sales_qty	--'全国SKC近7天销量'
    ,national_skc_last_7_14days_sales_qty	--'全国SKC近7-14天销量'
    ,allot_out_manager_org_skc_total_sales_qty --'调出区域skc累计销量'
    ,allot_in_manager_org_skc_total_sales_qty --'调入区域skc累计销量'
    ,allot_out_manager_org_skc_last_7days_sales_qty --'调出区域skc近7天销量'
    ,allot_in_manager_org_skc_last_7days_sales_qty --'调入区域skc近7天销量'
    ,allot_out_manager_org_skc_last_7_14days_sales_qty --'调出区域skc近7-14天销量'
    ,allot_in_manager_org_skc_last_7_14days_sales_qty --'调入区域skc近7-14天销量'
    ,national_suit_total_sales_qty --'全国套装累计销量'
    ,national_suit_last_7days_sales_qty --'全国套装近7天销量'
    ,national_suit_last_7_14days_sales_qty --'全国套装近7-14天销量'
    ,allot_out_org_suit_last_7days_sales_qty --'调出门店套装近7天销量'
    ,allot_in_org_suit_last_7days_sales_qty --'调入门店套装近7天销量'
    ,allot_out_org_suit_last_7_14days_sales_qty --'调出门店套装近7-14天销量'
    ,allot_in_org_suit_last_7_14days_sales_qty --'调入门店套装近7-14天销量'
    ,allot_out_org_suit_total_sales_qty --'调出门店套装累计销量'
    ,allot_in_org_suit_total_sales_qty --'调入门店套装累计销量'
    ,allot_out_manager_org_suit_last_7days_sales_qty --'调出区域套装近7天销量'
    ,allot_in_manager_org_suit_last_7days_sales_qty --'调入区域套装近7天销量'
    ,allot_out_manager_org_suit_last_7_14days_sales_qty --'调出区域套装近7-14天销量'
    ,allot_in_manager_org_suit_last_7_14days_sales_qty --'调入区域套装近7-14天销量'
    ,allot_out_manager_org_suit_total_sales_qty --'调出区域套装累计销量'
    ,allot_in_manager_org_suit_total_sales_qty --'调入区域套装累计销量'
    ,allot_out_org_all_last_7days_sales_qty --'调出门店近7天销量'
    ,allot_in_org_all_last_7days_sales_qty --'调入门店近7天销量'
    ,allot_out_org_all_last_7_14days_sales_qty --'调出门店近7-14天销量'
    ,allot_in_org_all_last_7_14days_sales_qty --'调入门店近7-14天销量'
    ,allot_out_org_spu_total_sales_qty
    ,allot_out_org_spu_last_7days_sales_qty
    ,allot_out_org_spu_last_7_14days_sales_qty
    ,allot_in_org_spu_total_sales_qty
    ,allot_in_org_spu_last_7days_sales_qty
    ,allot_in_org_spu_last_7_14days_sales_qty

    ,product_name
    ,virtual_suit_code
    ,big_class
    ,mid_class
    ,tiny_class
    ,in_org_after_ra_wh_sub_stock_total_qty
    ,in_org_before_ra_onroad_stock_qty
    ,in_org_before_ra_include_stock_qty
    ,out_org_before_ra_include_stock_qty
    ,out_org_after_ra_sub_stock_qty
    ,out_org_after_ra_wh_sub_stock_total_qty
    ,biz_action_template_code
    ,biz_action_template_name
    ,org_sk
    ,step_id
    ,batch_id
    ,org_name --所属组织名称
    ,job_id --任务id
    ,task_status --任务状态
    ,model_allot_out_up_org_code --模型调出方上级区域编码
    ,model_allot_in_up_org_code --模型调入方上级区域编码
    ,human_allot_out_up_org_code --人工调出方上级区域编码
    ,human_allot_in_up_org_code --人工调入方上级区域编码
    ,model_exception_type --异常类型 （模型）
    ,human_exception_type --异常类型 （人工）
    ,document_code --需求单号
    ,in_org_forecast_available_stock_qty --调入方预计可用库存
    ,in_org_after_model_ra_onroad_stock_qty --调入方模型补调后库存（含在途）
    ,in_org_after_model_ra_sub_stock_qty --调入方模型补调后库存（减在单出）
    ,out_org_before_ra_onroad_stock_qty --调出方补调前库存（含在途）
    ,out_org_after_model_ra_onroad_stock_qty --调出方模型补调后库存（含在途）
    ,in_org_committed_onorder_out_qty --调入方当日已提交在单出
    ,out_flag --调出方（人工）店仓标记
    ,in_flag --调入方（人工）店仓标记
    --更新对应skc+org 批次、模板调入/调出方补调前/后库存
    --,batch_id
    ,template_in_org_before_model_ra_stock_qty
    ,template_in_org_before_model_ra_onroad_stock_qty
    ,template_in_org_before_model_ra_sub_stock_qty
    ,template_in_org_before_model_ra_include_stock_qty
    ,template_in_org_after_model_ra_stock_qty
    ,template_in_org_after_model_ra_onroad_stock_qty
    ,template_in_org_after_model_ra_sub_stock_qty
    ,template_in_org_after_model_ra_include_stock_qty

    ,template_out_org_before_model_ra_stock_qty
    ,template_out_org_before_model_ra_onroad_stock_qty
    ,template_out_org_before_model_ra_sub_stock_qty
    ,template_out_org_before_model_ra_include_stock_qty
    ,template_out_org_after_model_ra_stock_qty
    ,template_out_org_after_model_ra_onroad_stock_qty
    ,template_out_org_after_model_ra_sub_stock_qty
    ,template_out_org_after_model_ra_include_stock_qty
    --in_human 模板调入方人工补调前/后库存
    ,template_in_org_before_human_ra_stock_qty
    ,template_in_org_before_human_ra_onroad_stock_qty
    ,template_in_org_before_human_ra_sub_stock_qty
    ,template_in_org_before_human_ra_include_stock_qty

    ,template_in_org_after_human_ra_stock_qty
    ,template_in_org_after_human_ra_onroad_stock_qty
    ,template_in_org_after_human_ra_sub_stock_qty
    ,template_in_org_after_human_ra_include_stock_qty
    --out_human 模板调出方人工补调前/后库存
    ,template_out_org_before_human_ra_stock_qty
    ,template_out_org_before_human_ra_onroad_stock_qty
    ,template_out_org_before_human_ra_sub_stock_qty
    ,template_out_org_before_human_ra_include_stock_qty

    ,template_out_org_after_human_ra_stock_qty
    ,template_out_org_after_human_ra_onroad_stock_qty
    ,template_out_org_after_human_ra_sub_stock_qty
    ,template_out_org_after_human_ra_include_stock_qty
    --in_human 调入方人工补调前/后库存
    ,in_org_after_human_ra_stock_qty
    ,in_org_after_human_ra_onroad_stock_qty
    ,in_org_after_human_ra_sub_stock_qty
    ,in_org_after_human_ra_include_stock_qty
    --out_human 调出方人工补调前/后库存
    ,out_org_after_human_ra_stock_qty
    ,out_org_after_human_ra_onroad_stock_qty
    ,out_org_after_human_ra_sub_stock_qty
    ,out_org_after_human_ra_include_stock_qty
    --更新对应skc+org 优先级评分
    ,in_org_send_priority_score
    ,in_org_receive_priority_score
    ,out_org_send_priority_score
    ,out_org_receive_priority_score
    --更新对应skc+org 目标库存、满足率
    ,in_org_target_stock
    ,in_org_fill_rate
    ,out_org_target_stock
    ,out_org_fill_rate
    )
    select skc_sk , skc_code, product_code, color_code, brand_code,brand_name,product_year_quarter,
      product_range, band,
      class_longcode,
      suit_id, tag_price,
      out_sales_level_code,
      out_sales_level_name,
      in_sales_level_code,
      in_sales_level_name,
      sales_type, size_group_code, model_allot_out_manager_org_code,
      model_allot_in_manager_org_code,
      model_allot_out_manager_org_name,--新增
      model_allot_in_manager_org_name,--新增
      model_allot_out_org_sk, model_allot_in_org_sk,
      model_allot_out_org_code ,--新增
      model_allot_in_org_code,--新增
      model_allot_out_org_name,--新增
      model_allot_in_org_name,--新增
      human_allot_out_manager_org_code, human_allot_in_manager_org_code,
      human_allot_out_manager_org_name, human_allot_in_manager_org_name,
      human_allot_out_org_sk, human_allot_in_org_sk, human_allot_out_org_code,
      human_allot_in_org_code, human_allot_out_org_name, human_allot_in_org_name,
      in_store_level, in_store_city, in_store_history_first_distribution_date,
      in_store_first_distribution_date, in_org_last_7days_sales_qty,
      in_org_last_7_14days_sales_qty, in_org_total_sales_qty,
      in_org_before_ra_stock_qty, in_org_before_ra_sub_stock_qty,
      in_org_after_model_ra_stock_qty, in_org_after_model_ra_include_stock_qty,
      in_org_onroad_stock_qty, in_org_onorder_in_stock_qty,
      in_org_onorder_out_stock_qty, out_store_level,
      out_store_city, out_store_history_first_distribution_date,
      out_store_first_distribution_date, out_org_last_7days_sales_qty,
      out_org_last_7_14days_sales_qty, out_org_total_sales_qty,
      out_org_before_ra_stock_qty, out_org_before_ra_sub_stock_qty,
      out_org_after_model_ra_stock_qty, out_org_after_model_ra_include_stock_qty,
      out_org_onroad_stock_qty, out_org_onorder_in_stock_qty, out_org_onorder_out_stock_qty,
      out_org_forecast_available_stock_qty, out_org_committed_onorder_out_qty, model_ra_qty,
      human_ra_qty, scene_code,scene_name, ra_source, commit_status,modify_status, order_id,
      is_effective, creator, commit_user_name, remark, update_time, etl_time, day_date,
      reserved1, reserved2, reserved3, reserved4, reserved5, reserved6, reserved7, reserved8, reserved9, reserved10,
      reserved11--, --reserved12, reserved13, reserved14, reserved15, reserved16, reserved17, reserved18, reserved19, reserved20

      ,color_name --'颜色名称'
      ,national_skc_total_sales_qty --'全国SKC累计销量'
      ,national_skc_last_7days_sales_qty	--'全国SKC近7天销量'
      ,national_skc_last_7_14days_sales_qty	--'全国SKC近7-14天销量'
      ,allot_out_manager_org_skc_total_sales_qty --'调出区域skc累计销量'
      ,allot_in_manager_org_skc_total_sales_qty --'调入区域skc累计销量'
      ,allot_out_manager_org_skc_last_7days_sales_qty --'调出区域skc近7天销量'
      ,allot_in_manager_org_skc_last_7days_sales_qty --'调入区域skc近7天销量'
      ,allot_out_manager_org_skc_last_7_14days_sales_qty --'调出区域skc近7-14天销量'
      ,allot_in_manager_org_skc_last_7_14days_sales_qty --'调入区域skc近7-14天销量'
      ,national_suit_total_sales_qty --'全国套装累计销量'
      ,national_suit_last_7days_sales_qty --'全国套装近7天销量'
      ,national_suit_last_7_14days_sales_qty --'全国套装近7-14天销量'
      ,allot_out_org_suit_last_7days_sales_qty --'调出门店套装近7天销量'
      ,allot_in_org_suit_last_7days_sales_qty --'调入门店套装近7天销量'
      ,allot_out_org_suit_last_7_14days_sales_qty --'调出门店套装近7-14天销量'
      ,allot_in_org_suit_last_7_14days_sales_qty --'调入门店套装近7-14天销量'
      ,allot_out_org_suit_total_sales_qty --'调出门店套装累计销量'
      ,allot_in_org_suit_total_sales_qty --'调入门店套装累计销量'
      ,allot_out_manager_org_suit_last_7days_sales_qty --'调出区域套装近7天销量'
      ,allot_in_manager_org_suit_last_7days_sales_qty --'调入区域套装近7天销量'
      ,allot_out_manager_org_suit_last_7_14days_sales_qty --'调出区域套装近7-14天销量'
      ,allot_in_manager_org_suit_last_7_14days_sales_qty --'调入区域套装近7-14天销量'
      ,allot_out_manager_org_suit_total_sales_qty --'调出区域套装累计销量'
      ,allot_in_manager_org_suit_total_sales_qty --'调入区域套装累计销量'
      ,allot_out_org_all_last_7days_sales_qty --'调出门店近7天销量'
      ,allot_in_org_all_last_7days_sales_qty --'调入门店近7天销量'
      ,allot_out_org_all_last_7_14days_sales_qty --'调出门店近7-14天销量'
      ,allot_in_org_all_last_7_14days_sales_qty --'调入门店近7-14天销量'
      ,allot_out_org_spu_total_sales_qty
      ,allot_out_org_spu_last_7days_sales_qty
      ,allot_out_org_spu_last_7_14days_sales_qty
      ,allot_in_org_spu_total_sales_qty
      ,allot_in_org_spu_last_7days_sales_qty
      ,allot_in_org_spu_last_7_14days_sales_qty

      ,product_name
      ,virtual_suit_code
      ,big_class
      ,mid_class
      ,tiny_class
      ,in_org_after_ra_wh_sub_stock_total_qty
      ,in_org_before_ra_onroad_stock_qty
      ,in_org_before_ra_include_stock_qty
      ,out_org_before_ra_include_stock_qty
      ,out_org_after_ra_sub_stock_qty
      ,out_org_after_ra_wh_sub_stock_total_qty
      ,biz_action_template_code
      ,biz_action_template_name
      ,org_sk
      ,step_id
      ,batch_id
      ,org_name --所属组织名称
      ,job_id --任务id
      ,task_status --任务状态
      ,model_allot_out_up_org_code --模型调出方上级区域编码
      ,model_allot_in_up_org_code --模型调入方上级区域编码
      ,human_allot_out_up_org_code --人工调出方上级区域编码
      ,human_allot_in_up_org_code --人工调入方上级区域编码
      ,model_exception_type --异常类型 （模型）
      ,human_exception_type --异常类型 （人工）
      ,document_code --需求单号
      ,in_org_forecast_available_stock_qty --调入方预计可用库存
      ,in_org_after_model_ra_onroad_stock_qty --调入方模型补调后库存（含在途）
      ,in_org_after_model_ra_sub_stock_qty --调入方模型补调后库存（减在单出）
      ,out_org_before_ra_onroad_stock_qty --调出方补调前库存（含在途）
      ,out_org_after_model_ra_onroad_stock_qty --调出方模型补调后库存（含在途）
      ,in_org_committed_onorder_out_qty --调入方当日已提交在单出
      ,out_flag --调出方（人工）店仓标记
      ,in_flag --调入方（人工）店仓标记
      --更新对应skc+org 批次、模板调入/调出方补调前/后库存
      --,batch_id
      ,template_in_org_before_model_ra_stock_qty
      ,template_in_org_before_model_ra_onroad_stock_qty
      ,template_in_org_before_model_ra_sub_stock_qty
      ,template_in_org_before_model_ra_include_stock_qty
      ,template_in_org_after_model_ra_stock_qty
      ,template_in_org_after_model_ra_onroad_stock_qty
      ,template_in_org_after_model_ra_sub_stock_qty
      ,template_in_org_after_model_ra_include_stock_qty

      ,template_out_org_before_model_ra_stock_qty
      ,template_out_org_before_model_ra_onroad_stock_qty
      ,template_out_org_before_model_ra_sub_stock_qty
      ,template_out_org_before_model_ra_include_stock_qty
      ,template_out_org_after_model_ra_stock_qty
      ,template_out_org_after_model_ra_onroad_stock_qty
      ,template_out_org_after_model_ra_sub_stock_qty
      ,template_out_org_after_model_ra_include_stock_qty
      --in_human 模板调入方人工补调前/后库存
      ,template_in_org_before_human_ra_stock_qty
      ,template_in_org_before_human_ra_onroad_stock_qty
      ,template_in_org_before_human_ra_sub_stock_qty
      ,template_in_org_before_human_ra_include_stock_qty

      ,template_in_org_after_human_ra_stock_qty
      ,template_in_org_after_human_ra_onroad_stock_qty
      ,template_in_org_after_human_ra_sub_stock_qty
      ,template_in_org_after_human_ra_include_stock_qty
      --out_human 模板调出方人工补调前/后库存
      ,template_out_org_before_human_ra_stock_qty
      ,template_out_org_before_human_ra_onroad_stock_qty
      ,template_out_org_before_human_ra_sub_stock_qty
      ,template_out_org_before_human_ra_include_stock_qty

      ,template_out_org_after_human_ra_stock_qty
      ,template_out_org_after_human_ra_onroad_stock_qty
      ,template_out_org_after_human_ra_sub_stock_qty
      ,template_out_org_after_human_ra_include_stock_qty
      --in_human 调入方人工补调前/后库存
      ,in_org_after_human_ra_stock_qty
      ,in_org_after_human_ra_onroad_stock_qty
      ,in_org_after_human_ra_sub_stock_qty
      ,in_org_after_human_ra_include_stock_qty
      --out_human 调出方人工补调前/后库存
      ,out_org_after_human_ra_stock_qty
      ,out_org_after_human_ra_onroad_stock_qty
      ,out_org_after_human_ra_sub_stock_qty
      ,out_org_after_human_ra_include_stock_qty
      --更新对应skc+org 优先级评分
      ,in_org_send_priority_score
      ,in_org_receive_priority_score
      ,out_org_send_priority_score
      ,out_org_receive_priority_score
      --更新对应skc+org 目标库存、满足率
      ,in_org_target_stock
      ,in_org_fill_rate
      ,out_org_target_stock
      ,out_org_fill_rate
    from tmp_rst_ra_skc_org_detail_result_2
    ;"""
exesql(sql)	
#%%
sql=f"""   
    --删除重复单据
    delete from gp.tenant_peacebird_biz.rst_ra_skc_org_detail
    --4 删除多余的id
    where id in(
        --3 查询重复id
      select id
      from (
          --2 获取重复order_id+id
        select order_id
            ,id
          ,row_number()over(partition by order_id order by id) as row_id
        from tmp_all_biz_rst_ra_skc_org_detail
        where order_id in (
            --1 查询order_id重复-如有
          select a.order_id
          from tmp_rst_ra_skc_org_detail_result a
          group by a.order_id
        ) --and is_deleted = '0' and  day_date = '{day_date}'
      ) aa
      where row_id>=2
    ) and day_date = '{day_date}'
    ;"""
sql="""
DELETE FROM gp.tenant_peacebird_biz.rst_ra_skc_org_detail
WHERE id IN (
    SELECT  id
    FROM (
        SELECT a.order_id,
               a.id,
               ROW_NUMBER() OVER (PARTITION BY a.order_id ORDER BY a.id) AS rn
        FROM tmp_all_biz_rst_ra_skc_org_detail a
        inner join (select order_id from tmp_rst_ra_skc_org_detail_result group by order_id) b on a.order_id=b.order_id
    ) sub
    WHERE rn > 1
) and day_date = '{day_date}'
;"""
exesql(sql)	
#%%
sql=f"""   
    --更新异常类型-update增量
    select * from postgres_query('gp',$$select tenant_peacebird_biz.update_skc_exception_type('{day_date}','biz','update');$$)
"""
#exesql(sql)	
#%%
sql=f"""update gp.tenant_peacebird_biz.rst_ra_sku_org_detail a set
	compute_status='0'
    from tmp_distinct_order_id b
    where a.skc_order_id=b.skc_order_id
    and a.day_date = '{day_date}' and a.is_deleted = '0'
    ;"""
#exesql(sql)	
#%%  
sql=f"""SELECT GROUP_CONCAT(skc_order_id, ',') from tmp_distinct_order_id
--id from (select * FROM tmp_distinct_order_id limit 3)t;    
"""
ids=dd.execute(sql).fetchall()[0][0]
#%%
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
print(f'total cost:{round(time.time()-t0,2)}')
