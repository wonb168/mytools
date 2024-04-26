# %%
import psycopg
dsn_src="postgresql://gpadmin:8ntK0Ppkct8sm45Z@192.168.200.82:2345/mdmaster_lansheng5_prod"
dsn_tgt="postgresql://gpadmin:20150826@192.168.12.136:5437/postgres"  #mdmaster_hggp7_dev
dsn_tgt="postgresql://gpadmin:lzsj2021@192.168.200.101:2345/postgres"
#%%
%%time
# 单表copy
t="tenant_lansheng5_biz.rst_ra_skc_org_detail"
sql_copy=f"COPY (select * from {t} where day_date='2024-03-21') TO STDOUT (FORMAT BINARY)"
print(sql_copy)
with psycopg.connect(dsn_src) as conn1, psycopg.connect(dsn_tgt) as conn2:
    with conn1.cursor().copy(sql_copy) as copy1:
        with conn2.cursor().copy(f"COPY {t} FROM STDIN (FORMAT BINARY)") as copy2:
            for data in copy1:
                copy2.write(data)
# %%
