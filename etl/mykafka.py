#%%
from confluent_kafka.admin import AdminClient

# Kafka集群配置，使用Aliyun Kafka地址
conf = {
    'bootstrap.servers': 'alikafka-pre-cn-0pp0wshvu00b-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-3-vpc.alikafka.aliyuncs.com:9092'
}

# 创建AdminClient实例
admin_client = AdminClient(conf)

# 获取所有topics
topics = admin_client.list_topics().topics.keys()
print("Topics:", topics)

# 关闭AdminClient
#admin_client.close()
#%%
Topics: dict_keys(['PRE_CP_C_PRODUCT_PROMOTION_INFORMATION', 
                   'PRE_DL_B_PUR_REQ_TRAN_ITEM', 
                   'PRE_DL_OM_OMNI_CHANNEL_ORDER_BACK_ITEM', 
                   'PRE_AD_LIMIT_VALUES', 'PRE_CP_C_EXPCOMPANY', 
                   'PRE_SM_SEAL_SAMPLE', 'PRE_DL_B_RETAIL_ERROR', 
                   'PRE_SC_B_SHARESTOCK_STOCK', 
                   'PRE_DL_B_PUR_RET_ITEM', 'PRE_DL_B_PUR_TMPIN', 
                   'PRE_DL_F_RETAIL_PRICE_SKU_ITEM', 'PRE_ALL_C_RETURN_REASON', 'PRE_DL_OM_DELIVERY_ORDER', 'PRE_DL_B_PUR_RET', 'PRE_DL_F_PRICE_RETAIL', 'PRE_CP_C_SUBJECT_PERMISSION', 'PRE_DL_B_PUR_ORDER_NODE', 'PRE_SM_SAMPLE_CLOTHING_SKC', 
                   'PRE_PS_C_PRO', 'PRE_MAT_FABRIC_INGREDIENT', 'PRE_CP_C_SEND_RULE', 'PRE_DL_B_BUY_PLAN_ADJUSTPRE', 'PRE_PS_C_PRO_SPECIAL_RETURN_SETTING', 'PRE_DL_B_PUR_ORDER_ITEM', 'PRE_DL_T_PUR_OUT_BILL_ID_ITEM', 'PRE_SC_B_SHARESTOCK_RETAIL_PRICE_SETTING', 'PRE_DL_F_RETAIL_PRICE_SKC_ITEM', 'PRE_DL_TRADE_SHOP_TYPE_LOCATION', 'PRE_DL_B_RETAIL', 'PRE_ERROR_PRODUCER', 'PRE_CP_C_STOREORG_LEVEL', 'PRE_VP_C_CRM_ACTIVITY_LUCK_DRAW_FTP', 'PRE_DL_C_DISTRIBTIONTFTP', 'PRE_CP_C_PLATFORM_TYPE', 'PRE_VP_C_VIP_ORDER_ASSESS', 'PRE_CP_C_FRONT_CLOSE_STORE_PLAN', 'PRE_CP_C_STORE_GROUP_STYLE_CONFIGURATION', 'PRE_DL_OM_REFUSE_APPLY', 'PRE_DL_B_PUR_ORDER_SYN_ITEM', 'PRE_CP_C_TARGET_INVENTORY', 'PRE_DL_F_RETAIL_PRICE', 'PRE_DL_C_BILLDIM_ITEM', 'PRE_DL_OM_OMNI_CHANNEL_ORDER_PAY', 'PRE_DL_B_STOREBANKCARDFTP', 'PRE_SC_B_SHARESTOCK_PRO', 'PRE_CP_C_STORE', 'PRE_SC_B_SHARESTOCK', 'PRE_CP_C_HRORG', 'PRE_USERS', 'PRE_DL_B_PAND_ITEM', 'PRE_SC_B_SHARESTOCK_SETTING_RECOUNT_IO', 'PRE_DL_B_ADJ_PPD', 'PRE_DL_B_PUR_ADJ_PRICE_ITEM', 'PRE_DL_B_PUR_ADJ_PRICE', 'PRE_PS_C_SKU', 'PRE_DL_B_PUR_TMPIN_ITEM', 'PRE_CP_C_CREDIT_RECORD', 'PRE_DL_B_TRAN_ITEM_SKU', 'PRE_FC_T_PICK_STORE', 'PRE_DL_B_PUR_REQ', 'PRE_DL_B_PAL_SUMM', 'PRE_VP_C_DEF_VOUS', 'PRE_PS_C_PRODIM_ITEM', 'PRE_EX_EXCHANGE_FAIR_SCORE', 'PRE_DL_B_BUY_PLAN_ITEM', 'PRE_DL_C_BILLDIM', 'PRE_SM_SEAL_SAMPLE_UPDATE_LOG', 'PRE_CP_C_ZONAL_SEASONAL_TIME', 'PRE_SM_SAMPLE_CLOTHING', 'PRE_DL_B_ADJ_PLAN', 'PRE_DL_B_SINGLE_PRO_DECISION', 'PRE_PROD_SCHEDUL_DEMAND_ORDER_ITEM', 'PRE_CP_C_STOREDIM', 'PRE_SC_B_SHARESTOCK_SETTING', 'PRE_DL_B_STORE_DISPLAY', 'PRE_DL_C_TONGLIAN_TERMINAL_NO', 'PRE_CP_C_HOLIDAY', 'PRE_DL_OM_DELIVERY_ORDER_LOG', '__consumer_offsets', 'PRE_DL_B_BUY_PLAN', 'PRE_VP_C_VIP_INTEFTP', 'PRE_VP_C_VIP_OAUTH', 'PRE_DL_T_PUR_OUT_BILL_ID', 'PRE_CP_C_EMP', 'PRE_DL_F_RETAIL_PRICE_PRO_ITEM', 'PRE_DL_B_INV_ADJ_ITEM', 'PRE_ERROR_ES', 'PRE_SCM_LOG_CONFIGURATION', 'PRE_CP_C_SUPPLIER', 'PRE_CP_C_JOB', 'PRE_MD_PLAN_MEETING_STRUCTURE', 'PRE_DL_B_INV_ADJ', 'PRE_MAT_FABRIC', 'PRE_FC_T_PICK_SKU', 'PRE_AC_B_BANK_CARD', 'PRE_SC_B_SHARESTOCK_MICRO_MALL_PRODUCT', 'PRE_VP_C_VIP_AMOUNT', 'PRE_DL_B_PAND', 'PRE_DL_B_STORE_BILL_ITEM', 'PRE_CP_C_BUFFER_DAYS', 'PRE_CALLBACK_CUSTOMER_INFO_CHANGE', 'PRE_SC_B_STOCK_DAILY_IO', 'PRE_VP_C_VIP_LABEL', 'PRE_DL_B_PUR_ORDER', 'PRE_PS_C_PRODIM', 'PRE_POS_CPU', 'PRE_PS_C_PRO_SUPITEM', 'PRE_DL_B_RETAIL_PAY_ITEM', 'PRE_DL_OM_REFUSE_APPLY_ITEM', 'PRE_DL_B_TRAN', 'PRE_GROUPUSER', 'PRE_CALLBACK_MEMBER_CARD_CHANGE', 'PRE_CP_C_SPECIAL_FIRST_ITEM', 'PRE_DL_B_STORE_PAL', 'PRE_SC_B_SHARESTOCK_CRITICAL_SETTING', 'PRE_LINGMAO_DEAD_LETTER', 'PRE_SM_SAMPLE_CLOTHING_SKU', 'PRE_DL_B_TRAN_PLAN', 'PRE_DL_OM_OMNI_CHANNEL_ORDER', 'PRE_DL_OM_OMNI_ACHIEVEMENT_EXPRESS_ITEM', 'PRE_CP_C_STOREFIRST_ITEM', 'PRE_DL_B_PUR_REQ_ITEM', 'PRE_B_VIPMONEY_RECORD', 'PRE_DL_B_STORE_FEE_ITEM', 'PRE_DL_B_UNSALABLE_PRO_APPLY', 'PRE_DL_OM_OMNI_CHANNEL_ORDER_BACK', 'PRE_PS_C_SPECGROUP', 'PRE_CP_C_REGION', 'PRE_DL_OM_OMNI_ACHIEVEMENT_ORDER', 'PRE_VP_C_VIP_JOIN_ACTIVITY', 'PRE_PS_C_SHAPE', 'PRE_DL_B_STORE_BILL', 'PRE_DL_B_STORE_REVENUE_ADJ', 'PRE_DL_B_ADJ_PLAN_ITEM', 'PRE_PS_C_SPECOBJ', 'PRE_CP_C_POST', 'PRE_CALLBACK_GRADE_CHANGE_INFO', 'PRE_PS_C_SHAPEGROUP', 'PRE_DL_B_PUR_ADJ_QTY_ITEM', 'PRE_DL_C_MOBMONEY_DISTRIB', 'PRE_DL_B_TONGLIAN', 'PRE_CP_C_STOREDIM_ITEM', 'PRE_VP_C_VIP_RETAILFTP', 'PRE_SCM_REGRESSION_TARGET_RATE', 'PRE_DL_B_PUR_ADJ_QTY', 'PRE_DL_F_PRICE_RETAIL_SKU', 'PRE_CALLBACK_COUPON_USE', 'PRE_SC_B_PICKSTOCK_VIRTUAL_STOCK', 'PRE_DL_B_TRAN_PLAN_ITEM', 'PRE_VP_C_VIP', 'PRE_CALLBACK_COUPON_CREATE', 'PRE_CP_C_CATEGORY_SPECIAL_PRICE', 'PRE_DL_B_STORE_FEE', 'PRE_DL_B_SINGLE_PRO_DECISION_ITEM', 'PRE_DL_B_ALIPAY_BILL', 'PRE_CP_C_SUPPDIM', 'PRE_SC_B_STORE_SETTING', 'PRE_MAT_PREPARE_ESTIMATE_DOSAGE', 'PRE_DL_B_PUR_ORDER_SYN', 'PRE_VP_C_VIP_SERVICE_ASSESS', 'PRE_CP_C_SUPPDIM_ITEM', 'PRE_CP_C_ALLIANCE_BUSINESS', 'PRE_DL_B_SINGLE_PRO_DECISION_TRACE', 'PRE_DL_B_PUR_IN', 'PRE_CALLBACK_CUSTOMER', 'PRE_VP_C_VIP_ACC', 'PRE_DL_B_WECHAT_BILL', 'PRE_GROUPS', 'PRE_DL_B_PUR_IN_ITEM', 'PRE_PS_C_PRO_RETURN_SETTING', 'PRE_VP_C_VIP_AMOUNTFTP', 'PRE_DL_B_BUY_PLAN_ADJUST', 'PRE_DL_OM_DELIVERY_ORDER_ITEM', 'PRE_SC_B_PICKSTOCK_STOCK', 'PRE_SC_B_CHANNEL', 'PRE_SC_B_SHARESTOCK_CRITICAL_RULE', 'PRE_PS_C_BRAND', 'PRE_DL_B_RETAIL_ITEM', 'PRE_CP_C_CREDIT_LEVEL', 'PRE_DL_B_PUR_ORDER_PRE_NODE', 'PRE_DL_B_PFT_LOS', 'PRE_CALLBACK_COUPON_SEND', 'PRE_CP_C_STOREORG', 'PRE_SM_SAMPLE_CLOTHING_EXTEND', 'PRE_DL_T_PUR_OUT_BILL_ID_ITEM_014', 'PRE_CP_C_STORE_CONFIGURATION_UPPER_LIMIT',
                    'PRE_DL_T_TRAN_WEIGHT_INFO'])
#%%
from confluent_kafka import Consumer, KafkaException

# Kafka集群配置
conf = {
    'bootstrap.servers': 'alikafka-pre-cn-0pp0wshvu00b-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-0pp0wshvu00b-3-vpc.alikafka.aliyuncs.com:9092',
    'group.id': 'GID_LANGZHONG_TEST',
    'auto.offset.reset': 'earliest'
}

# 创建Kafka Consumer实例
consumer = Consumer(conf)

# 订阅特定的topic
consumer.subscribe(['PRE_PS_C_PRO'])

try:
    while True:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                print('%% %s [%d] reached end at offset %d\n' %
                      (message.topic(), message.partition(), message.offset()))
            elif message.error():
                raise KafkaException(message.error())
        else:
            # Process the message
            print('Received message: {}'.format(message.value().decode('utf-8')))

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
#%%
from datetime import datetime, timedelta

# 已知的日期和天数
dt = '2024-06-14'
date_obj = datetime.strptime(dt, '%Y-%m-%d')

sql_list = []
# 循环输出 dt 及之前的 7 天
for i in range(days):
    # 计算日期
    dt=(date_obj - timedelta(days=i)).strftime('%Y-%m-%d')
    sql_list.append(f"SELECT * FROM '/data/mdmaster_peacebird_prod/gto_skc_org_feature_pivot{dt}.parquet'")
print(' union all by name '.join(sql_list))



# %%
