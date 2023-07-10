# !/usr/bin/python
# coding=utf-8

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

conf = SparkConf().setMaster('yarn').setAppName('abt')\
    .set('spark.executor.memory', '16G')\
    .set('spark.executor.cores', '12')\
    .set('spark.executor.instances', '24')\
    .set('spark.yarn.isPython', 'true')\
    .set('spark.sql.catalogImplementation', 'hive')\
    .set("spark.memory.offHeap.enabled",'true')\
    .set("spark.memory.offHeap.size","12G")\
    .set("spark.executor.memoryOverhead","12G")\
    .set('spark.sql.autoBroadcastJoinThreshold','-1') \
    .set('spark.port.maxRetries','100')\

#\.set('script_var_data', data) 
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

spark.sparkContext.setCheckpointDir('hdfs://HDP3PRD/tmp')
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

# import os
# import sys
# os.environ['SPARK_MAJOR_VERSION']='2'
# os.system("pyspark --master yarn \
# --driver-memory 4G \
# --executor-memory 12G \
# --num-executors 24 \
# --executor-cores 12 \
# --jars '/metadata/3.1.5.0-152/sqoop/lib/ojdbc7.jar' \
# --conf 'spark.rpc.message.maxSize=200' \
# --conf 'spark.core.connection.ack.wait.timeout=600' \
# --conf 'spark.executor.memoryOverhead=2048' \
# --conf 'spark.sql.autoBroadcastJoinThreshold=-1' \
# --conf 'spark.port.maxRetries=32' \
# ")
# spark.sparkContext.setCheckpointDir('hdfs://HDP3PRD/tmp')
# spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

import pyspark.sql.functions as f
from itertools import chain
import datetime as dt
from pyspark.sql.window import Window
import datetime
from utils.additional_functions import *
from utils.feauturetools_functions import *
from utils.dictionaries import *
import utils.spark_utils as su

path="/metadata/usr_home/s_aitechuser/products/abt"
#hdfs_path = "hdfs://HDP3PRD/tmp/abt_temp/abt_customer_general/temp_data_prep_files"
#abt_hdfs_path = "hdfs://HDP3PRD/tmp/abt_temp/abt_customer_general/abt"
hdfs_path = "hdfs://HDP3PRD/apps/hive/crm/crm.db/abt_temp/abt_customer_general/temp_data_prep_files"
abt_hdfs_path = "hdfs://HDP3PRD/apps/hive/crm/crm.db/abt_temp/abt_customer_general/abt"
recipients = ['krzysztof.chojnowski@bnpparibas.pl'] 

############################################
#KCH CHECK DWH_NEW_DATA
check_date = sys.argv[1]  
#check_date = '2022-10-31'

cntr_dwh_date = su.spark_sql_load(spark, 'pi', 'select cast(max(repo_date) as date) as dt from oa.rach_biez_statystyki', 'oracle').collect()[0][0].date()
print("check_date", check_date)
print("cntr_dwh_date.strptime('Y%-m%')", cntr_dwh_date.strftime('%Y-%m'))

if cntr_dwh_date.strftime('%Y-%m') == check_date[:7]: 
    print('CHECK running for date ',cntr_dwh_date)
else:
    raise Exception("New repo_date for date '{d}' is not ready yet !!!".format(d=check_date[:7]))
# end check
############################################

mapping_packg = f.create_map([f.lit(x) for x in chain(*ror_pack_dict.items())])
mapping_prod_grp = f.create_map([f.lit(x) for x in chain(*pvss_prod_grp_dict.items())])

###########p_DATE_END - for which month to run
###########FINAL_DATA_PERIOD - how many periods to run
###########p_HIST_VARS_PERIOD - how long hestory period

last_day_month = "'"+str(get_month_end(datetime.date.today()+relativedelta(months=-1)).date())+ "'"

run_date = su.spark_sql_load(spark, 'PI', 
    "select max(repo_date) as repo_Date from oa.calendar where work_day = 'Y' and repo_date <= to_date("+ last_day_month + ", 'YYYY-MM-DD')", 'oracle')
run_date = str(su.col_to_list(run_date, 'REPO_DATE'))[1:12] + "'"
req_dates = last_day_month[1:11]

#last_day_month = "'2022-09-30'"
#req_dates = last_day_month[1:11]
#run_date="'2022-09-30'"

steering_dict = {
        "DATE_END" : run_date, 
        "FINAL_DATA_PERIOD" : '1',
        "HIST_VARS_PERIOD" : '6',
        "MONTH_NO" : run_date[1:5] + run_date[6:8]
    }

print "run for " + run_date

steering_dict["DATA_PERIOD"] = str(int(steering_dict["FINAL_DATA_PERIOD"]) + int(steering_dict["HIST_VARS_PERIOD"]))

print('Steering_dict: {d}'.format(d=steering_dict))

####checks list of dates required for new monthly calculation

stdate = datetime.datetime.strptime(steering_dict["DATE_END"][1:len(steering_dict["DATE_END"])-1], '%Y-%m-%d').date()

required_date_list=[]
for i in range(int(steering_dict["DATA_PERIOD"])):
    dt=(get_month_end(stdate + relativedelta(months=-i))).date()
    required_date_list.append(dt)

cust_old = spark.read.parquet(hdfs_path + "/cust_vars.parquet")

# if cust_old[['tranmonth_end']].filter(f.col('tranmonth_end')==str(stdate + relativedelta(day=31))) > 0:
#     raise ValueError('Data arleady calculated')

#########################################################START CUSTOMERS##########################################################

###LIST OF CUSTOMERS FOR ABT

cust_sql = su.parametrize_sql(su.read_sql('customers_pi'), steering_dict)
cust = su.spark_sql_load(spark, 'pi', cust_sql, 'oracle')
cust.cache().count()

# ##### CUSTOMER VARIABLES

cust_vars_sql = su.parametrize_sql(su.read_sql('customer_variables_pi'), steering_dict)
cust_vars = su.spark_sql_load(spark, 'pi', cust_vars_sql, 'oracle')
cust_vars.cache().count()

###############SAVING CUSTOMER VARIABLES
            
cust_vars=cust_vars.repartition(200) 
cust_vars=cust_vars.withColumn('AGE', f.months_between(f.col('TRANMONTH_END'),f.col('BIRTH_DATE'))/f.lit(12.0))
cust_vars=cust_vars.drop('PHONE_I_NO', 'CUST_SRC_NO', 'PHONE_II_NO','POWIAT','MIASTO','PACKG_CODE','POSTG_DATE','CUST_ID','CUST_ID_TRUE')
cust_vars=cust_vars.join(cust['TRANMONTH_END', 'PESEL_NO'].distinct(), on=[ 'TRANMONTH_END', 'PESEL_NO'], how='inner').cache()
#cust_vars=cust_vars.withColumn('cust_id', f.col('cust_id').cast('int'))
cust_vars=cust_vars.withColumn('age_postg_date', f.col('age_postg_date').cast('decimal(38,0)'))
cust_vars=cust_vars.withColumn('tranmonth_end', f.col('tranmonth_end').cast('date'))
cust_vars=cust_vars.withColumn('birth_date', f.col('birth_date').cast('date'))
cust_vars=cust_vars.withColumn('akt_ziz', f.col('akt_ziz').cast('int'))
cust_vars=cust_vars.withColumn('gdpr_prfl', f.col('gdpr_prfl').cast('int'))
cust_vars=cust_vars.withColumn('zgoda_mark', f.col('zgoda_mark').cast('int'))
cust_vars=cust_vars.withColumn('zgoda_tel', f.col('zgoda_tel').cast('int'))
cust_vars=cust_vars.withColumn('zgoda_elektr', f.col('zgoda_elektr').cast('int'))
row_count = cust_vars.count()
print('[CHECK] KCH COUNT cust_vars', row_count)
cust_vars.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/cust_vars.parquet","append")

print "customers done"


##########################################################END CUSTOMERS##########################################################

##########################################################START PRODUCTS#########################################################

######## CHECK WHICH DATA NEEDS TO BE COPIED FROM DWH

product_vars_sql = su.parametrize_sql(su.read_sql('products_pi'), steering_dict)
product_vars = su.spark_sql_load(spark, 'pi', product_vars_sql, 'oracle')
product_vars.cache().count()
product_vars = product_vars.withColumnRenamed('KLIENT_ID', 'CUST_ID')

####JOINING CUSTOMERS WITH PRODUCTS

cust_temp1=cust['PESEL_NO','TRANMONTH_END','CUST_ID', 'PACKG_CODE'].distinct()
cust_products = product_vars.join(cust_temp1, on=["CUST_ID","TRANMONTH_END"], how='inner')
cust_products.cache().count()


cust_products=cust_products.withColumn('pr_cntr_data', f.col('pr_cntr_data').cast('date'))
cust_products=cust_products.withColumn('pr_pocz_data', f.col('pr_pocz_data').cast('date'))
cust_products=cust_products.withColumn('pr_zapad_data', f.col('pr_zapad_data').cast('date'))
cust_products=cust_products.withColumn('tranmonth_end', f.col('tranmonth_end').cast('date'))
cust_products=cust_products.withColumn('orig_matu_days', f.col('orig_matu_days').cast('int'))
cust_products=cust_products.withColumn('commt_days', f.col('commt_days').cast('int'))
cust_products=cust_products.withColumn('pr_zamkn_data', f.col('pr_zamkn_data').cast('date'))
cust_products=cust_products.withColumn('pr_odnawialny_auto_fl', f.col('pr_odnawialny_auto_fl').cast('int'))
cust_products=cust_products.withColumn('pr_OTWARTY_FL', f.when((((f.col('pr_zamkn_data').isNull()) | \
    (f.col('pr_zamkn_data') == '1800-01-01')) | (f.col('pr_zamkn_data') > steering_dict['DATE_END'][1:-1])) \
    & (((f.col('pr_zapad_data').isNull()) | (f.col('pr_zapad_data') > steering_dict['DATE_END'][1:-1]))), f.lit('O')).otherwise(f.lit('Z')))
cust_products = cust_products.withColumn('pr_zamkn_data', f.when(f.col('pr_zamkn_data')=='1800-01-01', f.lit(None)).otherwise(f.col('pr_zamkn_data')))
cust_products = cust_products.filter(f.substring(f.col('PROD_TYP_CODE'), 1, 1)!='L')
cust_products=cust_products.withColumn('pr_saldo_biez_pln', f.col('pr_saldo_biez_pln').cast('decimal(16,2)'))
cust_products=cust_products.withColumn('pr_saldo_cntr_pln', f.col('pr_saldo_cntr_pln').cast('decimal(16,2)'))

####PRODUCT DATA TRANSFORMING

cust_products2=cust_products.withColumnRenamed('COMMT_DAYS','DP_DNI_ZAANGAZOWANIA_N')\
                        .filter(f.col("PROD_TYP_CODE").isin(["KRLOK", "DKKRE", "DKPRE", "KINNY"]) == False) \
                        .filter(f.col("BAL_ACC_CODE").isin(["CA", "CE"]) == False) \
                        .withColumn('R_TECH_FLAG',f.when(((f.col("CNTR_SRC_NO").substr(13,3).isin(['170','171','172','173','174','175','176','177','178','179','180',\
                        '181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','675','676','677','993','339'])) & \
                        (f.col("BAL_ACC_CODE").isin(['CB','CL','CC', 'CK']) == False )) | f.col("BAL_ACC_CODE").isin(['9Q','FM']),f.lit(1)).otherwise(f.lit(0)))\
                        .withColumn("PVSS_PROD",\
                            f.when(((f.col("PROD_TYP_CODE") == 'DRORZ' ) | ((f.col("PROD_TYP_CODE") == 'DRBIE' ) & (f.col("BAL_ACC_CODE").isin('CC','CK')))) \
                            & (f.col("PACKG_CODE").isin(["PAKL","PRMM","MAXI","PKXL","PKLK"])), 'ROR_PREM')\
                            .when((f.col("PROD_TYP_CODE") == 'DRORZ' ) | ((f.col("PROD_TYP_CODE") == 'DRBIE' ) & (f.col("BAL_ACC_CODE").isin('CC','CK'))), 'ROR')\
                            .when(((f.col("PROD_TYP_CODE") == 'DRORZ' ) | ((f.col("PROD_TYP_CODE") == 'DRBIE' )) & (f.col("BAL_ACC_CODE").isin('CB'))), 'ROR_WAL')\
                            .when(((f.col("PROD_TYP_CODE") == 'ROR' ) & (f.col("BAL_ACC_CODE").isin('CL'))), 'R_OBS_LOK')\
                            .when((f.col("PROD_TYP_CODE")=="DRLOK"),"KOSZ")\
                            .when((f.col("PROD_TYP_CODE").isin(["DPMST","DRAIN","DFFIO","DRPOM","DINWE","ZTERM","DRIGL","DKOSM"])),"DEP_INNY")\
                            .when((f.col("PROD_TYP_CODE").isin(["DTERM"])) & ((f.col("ORIG_MATU_DAYS") < f.lit(100)) \
                                | (f.col("ORIG_MATU_DAYS").isNull()) &  (f.col("DP_DNI_ZAANGAZOWANIA_N") < f.lit(100) )),"DEP_INNY")\
                            .when((f.col("PROD_TYP_CODE").isin(["DTERM"])) & ((f.col("ORIG_MATU_DAYS") >= f.lit(100)) \
                                | (f.col("ORIG_MATU_DAYS").isNull()) &  (f.col("DP_DNI_ZAANGAZOWANIA_N") >= f.lit(100) )),"DEP_TERM_DL")\
                            .when((f.col("PROD_TYP_CODE").isin(["DSTRU","CERTI"])),"DEP_STRU")\
                            .when((f.col("PROD_TYP_CODE").isin(["DELOK"])),"DEP_ELOK")\
                            .when((f.col("PROD_TYP_CODE").isin(["DRIKE"])),"IKE")\
                            .when((f.col("PROD_TYP_CODE").isin(["DPOVN"])),"DEP_OVRN")\
                            .when((f.col("PROD_TYP_CODE").isin(["UBEZP"])),"UBEZP")\
                            .when((f.col("PROD_TYP_CODE").isin(["KPHIP"])),"KPHIP")\
                            .when((f.col("PROD_TYP_CODE").isin(["KKCHR","KKPRE","KRPOM","KOPER","KINWI","KSTCK","KSTUD"])),"KRE_INNY")\
                            .when((f.col("PROD_TYP_CODE").isin(["KGOTW"])),"KRE_GOT")\
                            .when((f.col("PROD_TYP_CODE").isin(["KKKRE"])),"KKRED")\
                            .when((f.col("PROD_TYP_CODE").isin(["KSAMO"])),"KRE_SAMO")\
                            .when((f.col("PROD_TYP_CODE").isin(["RMAKL"])),"R_MAKLER")\
                            .when((f.col("PROD_TYP_CODE").isin(["KRATL"])),"KRE_RATAL")\
                            .when((f.col("PROD_TYP_CODE").isin(["KKNSL"])),"KRE_KONS")\
                            .when((f.col("PROD_TYP_CODE").isin(["KNMSZ"])),"KRE_MIESZK")\
                            .when((f.col("PROD_TYP_CODE").isin(['ZROWN','PIENI','AKCYJ','STBWZ','INNYF'])),"F_INVEST")\
                            .when((f.col("PROD_TYP_CODE").isin(['ODODR','DRBLC','ROZLC','GNKPN','ZZABE','LORCH','ZINNY','SINNY','DRPOW','LFRCH','KROTR','GNKZN','LKOTR',\
                            'ZPRZE','RSAMO','RNMSZ','RPZBL','RINNY','RGOTW''PORCH',	'LEASN','PONCH','PFRCH','FAKTR','DRPLC','GWWYP','GNKPF','PINCH','LINCH'])),"TECH")\
                            .otherwise(f.lit("")))\
                        .withColumn('ROR_PAK', mapping_packg[f.col('PACKG_CODE')])\
                        .filter((f.col("PVSS_PROD") != "TECH") & (f.col("PVSS_PROD") != "") & (f.col("R_TECH_FLAG") == f.lit(0)))\
                        .withColumn('PVSS_PROD_GRP', mapping_prod_grp[f.col('PVSS_PROD')])\
                        .withColumn('PR_CNTR_DATA',f.when(f.col("PR_CNTR_DATA") < f.lit("1990-01-01") , f.lit("1990-01-01")).otherwise( f.col('PR_CNTR_DATA')))\
                        .withColumn('PR_ZAPAD_DATA',f.when(f.col("PR_ZAPAD_DATA") < f.lit("1990-01-01") , f.lit("1990-01-01")).otherwise( f.col('PR_ZAPAD_DATA')))\
                        .withColumn('PR_SALDO_BIEZ_PLN',f.when((f.col("PR_SALDO_BIEZ_PLN").isNull()) | (f.col("PR_SALDO_BIEZ_PLN")<f.lit(0)), f.lit(0))\
                            .otherwise(f.col('PR_SALDO_BIEZ_PLN'))).\
                            distinct()

product_vars = cust_products2.withColumn('PR_ZAPAD_DATA',f.when(f.col("PR_CNTR_DATA") > f.lit("2050-01-01") , f.lit("2050-01-01")).otherwise(f.col('PR_ZAPAD_DATA')))\
                                .withColumn('PR_CNTR_DATA_MIES', f.months_between(f.col('TRANMONTH_END'), f.col("PR_CNTR_DATA")))\
                                .withColumn('PR_ZAPAD_DATA_MIES', f.months_between(f.col('TRANMONTH_END'), f.col("PR_ZAPAD_DATA")))\
                                .withColumn('PR_SALDO_BIEZ_PLN', f.col('PR_SALDO_BIEZ_PLN').cast('decimal'))\
                                .select(f.col('CUST_ID'),f.col('PESEL_NO'),f.col('TRANMONTH_END'),f.col('PR_ODNAWIALNY_AUTO_FL'),f.col('ORIG_MATU_DAYS')\
                                ,f.col('PR_SALDO_CNTR_PLN'),f.col('PR_SALDO_BIEZ_PLN'),	f.col('PVSS_PROD'),	f.col('PR_OTWARTY_FL'),	f.col('PVSS_PROD_GRP')\
                                ,f.col('PR_CNTR_DATA_MIES'), f.col('PR_ZAPAD_DATA_MIES')).cache()

product_vars=product_vars.repartition(1000).cache()
row_count = product_vars.count()
print('[CHECK] KCH COUNT product_vars', row_count)
######TEMPORARY SAVE

product_vars.write.parquet(hdfs_path + "/temporary_files/products_vars1.parquet","overwrite")
spark.catalog.clearCache()
product_vars=spark.read.parquet(hdfs_path + "/temporary_files/products_vars1.parquet")
######AGREGATION OF PRODUCT DATA TO PESEL AND MONTH LEVEL 


prod_agg_dictionary = {
                        'PR_SALDO_CNTR_PLN' : [["MAX","SUM","AVG"], 0.001, False],
                        'PR_SALDO_CNTR_PLN PVSS_PROD_GRP' : [["MAX","SUM"], 0.001, False],
                        'PR_SALDO_BIEZ_PLN' : [ ["MAX","SUM","AVG"], 0.001, False],
                        'PVSS_PROD' : [["count"], 0.001, False],
                        'PVSS_PROD PR_OTWARTY_FL' : [["count"], 0.001, False],
                        'PVSS_PROD_GRP' : [["count"], 0.001, False],
                        'PVSS_PROD_GRP PR_OTWARTY_FL' : [ ["count"], 0.001, False],
                        'PR_OTWARTY_FL' : [ ["count"], 0.001, False],
                        'PR_CNTR_DATA_MIES PVSS_PROD_GRP' : [ ["MAX","MIN","AVG"], 0.001, False],
                        'PR_CNTR_DATA_MIES' : [ ["MAX","MIN"], 0.001, False],
                        'PR_ZAPAD_DATA_MIES PVSS_PROD_GRP' : [ ["MAX","MIN"], 0.001, False],
                        'PR_ZAPAD_DATA_MIES' : [ ["MAX","MIN"], 0.001, False]}

######IMPUTATION WITH 0

product_vars_agg=aggregate_variables(df=product_vars, dimensions=['PESEL_NO','TRANMONTH_END'], aggregate_dictionary=prod_agg_dictionary, abbr='PR', breaks=12).cache()
row_count = product_vars_agg.count()
print('[CHECK] KCH COUNT product_vars_agg', row_count)

# product_vars_agg_imputate_dictionary = {'MIN' : [0, "all"],
# 										'MAX' : [0, "all"],
# 										'AVG' : [0, "all"],
# 										'SUM' : [0, "all"],
# 										'COUNT' : [0, "all"],
# 										'FLAG' : [0, "all"]}

product_vars_agg_imp=product_vars_agg.na.fill(0).cache()
row_count = product_vars_agg_imp.count()
print('[CHECK] KCH COUNT product_vars_agg_imp', row_count)

######APPENDING DATA FROM REQUIRED MONTHS

product_vars_agg_imp.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/product_vars2.parquet","append")

spark.catalog.clearCache()

product_vars_agg_imp2=spark.read.parquet(hdfs_path + "/temporary_files/product_vars2.parquet").cache()
row_count = product_vars_agg_imp2.count()
print('[CHECK] KCH COUNT product_vars_agg_imp2', row_count)


######CREATION OF RUNNING VARIABLES

product_win_dictionary = {
                        'PR_*.*_MAX' : [[3,6],['max']],
                        'PR_*.*_AVG' : [[3,6],['avg']],
                        'PR_*.*_COUNT' : [[3,6],['sum']],
                        'PR_*.*_SUM' : [[3,6],['sum','max','avg','range']]
                        }

product_vars_agg_months = running_vars(df=product_vars_agg_imp2, 
                                        moving_dictionary=product_win_dictionary, 
                                        date_column='TRANMONTH_END', 
                                        id_column='PESEL_NO', 
                                        breaks=80, 
                                        history_period = steering_dict["FINAL_DATA_PERIOD"]).cache()

######CREATION OF SHARES VARIABLES

prod_dict_shares = {
                         'PR_*.*_MAX_MAX_*' : ['3M','6M','12M'],
                        'PR_*.*_SUM_SUM_*' : ['3M','6M','12M'],
                        'PR_*.*_AVG_AVG_*' : ['3M','6M','12M'],
                        'PR_*.*_COUNT_SUM_*' : ['3M','6M','12M'],
                     }

#####APPENDING PRODUCTS DATA

product_vars_agg_months2=shares_variables(df=product_vars_agg_months,shares_dictionary=prod_dict_shares).na.fill(0)
product_vars_agg_months2.cache().count()

row_count = product_vars_agg_months2.cache().count()
print('[CHECK] KCH COUNT product_vars_agg_months2', row_count)

product_vars_agg_months2.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/product_vars.parquet","append")


print "products done"
################################################################END PRODUCTS################################################################

#############FLOW OF OTHER VARIABLES THE SAME AS FOR PRODUCTS

################################################################START BALANCE################################################################
spark.catalog.clearCache()

balance_vars_sql = su.parametrize_sql(su.read_sql('balance_pi'), steering_dict)
balance = su.spark_sql_load(spark, 'pi', balance_vars_sql, 'oracle')
balance.cache().count()
#BALANCE DATA TRANSFORMATION

balance=balance.withColumn('pr_zamkn_data', f.col('pr_zamkn_data').cast('date'))
balance=balance.withColumn('pr_zapad_data', f.col('pr_zapad_data').cast('date'))
balance=balance.withColumn('cntr_bal_pln', f.col('cntr_bal_pln').cast('decimal(18,2)'))
balance=balance.withColumn('mntly_min_bal_pln', f.col('cntr_bal_pln').cast('decimal(18,2)'))
balance=balance.withColumn('mntly_max_bal_pln', f.col('cntr_bal_pln').cast('decimal(18,2)'))
balance=balance.withColumn('cre_line_cntr_id', f.col('cre_line_cntr_id').cast('int'))
balance=balance.withColumn('curr_acc_cntr_id', f.col('curr_acc_cntr_id').cast('int'))

balance=balance.withColumn('pr_OTWARTY_FL', f.when((((f.col('pr_zamkn_data').isNull()) | \
    (f.col('pr_zamkn_data') == '1800-01-01')) | (f.col('pr_zamkn_data') > steering_dict['DATE_END'][1:-1])) \
    & (((f.col('pr_zapad_data').isNull()) | (f.col('pr_zapad_data') > steering_dict['DATE_END'][1:-1]))), f.lit('O')).otherwise(f.lit('Z')))

#kch check
balance.write.parquet(abt_hdfs_path + "/kch_check_balance.parquet",'overwrite')

linia2 = balance.filter(f.col("PROD_GRP_CODE") != "LINIA")\
                        .join(balance.select(f.col("CNTR_ID"),(f.col("TRANMONTH_END")),f.col("CNTR_BAL_PLN"), f.col("MNTLY_MIN_BAL_PLN"), f.col("MNTLY_MAX_BAL_PLN"))\
                        .withColumnRenamed('CNTR_ID', 'CRE_LINE_CNTR_ID')\
                        .withColumnRenamed('CNTR_BAL_PLN', 'LN_CNTR_BAL_PLN')\
                        .withColumnRenamed('MNTLY_MAX_BAL_PLN', 'LN_MNTLY_MAX_BAL_PLN')\
                        .withColumnRenamed('MNTLY_MIN_BAL_PLN', 'LN_MNTLY_MIN_BAL_PLN')\
                        .filter(f.col("PROD_GRP_CODE") == "LINIA"), on=["CRE_LINE_CNTR_ID","TRANMONTH_END"], how='left')


balance2 = linia2.filter(f.col("CURR_ACC_CNTR_ID").isNull())\
                    .withColumnRenamed('MNTLY_MAX_BAL_PLN','AC_MNTLY_MAX_BAL_PLN')\
                    .withColumnRenamed('MNTLY_MIN_BAL_PLN','AC_MNTLY_MIN_BAL_PLN')\
                    .select(f.col('CNTR_ID'),f.col('CUST_ID'),f.col('AC_MNTLY_MAX_BAL_PLN'),f.col('AC_MNTLY_MIN_BAL_PLN'), f.col('TRANMONTH_END'))	\
                        .join (\
                            linia2.filter(f.col("CURR_ACC_CNTR_ID").isNotNull())\
                            .drop(f.col("CNTR_ID"))\
                            .withColumnRenamed('CNTR_BAL_PLN','KR_CNTR_BAL_PLN')\
                            .withColumnRenamed('MNTLY_MAX_BAL_PLN','KR_MNTLY_MAX_BAL_PLN')\
                            .withColumnRenamed('MNTLY_MIN_BAL_PLN','KR_MNTLY_MIN_BAL_PLN')\
                            .withColumnRenamed('CURR_ACC_CNTR_ID','CNTR_ID')\
                            .select(f.col('KR_CNTR_BAL_PLN'),f.col('KR_MNTLY_MAX_BAL_PLN'),f.col('KR_MNTLY_MIN_BAL_PLN'),f.col('CNTR_ID'),f.col('LN_CNTR_BAL_PLN')\
                            ,f.col('LN_MNTLY_MAX_BAL_PLN'),f.col('LN_MNTLY_MIN_BAL_PLN'), f.col('TRANMONTH_END'))\
                        , on=["CNTR_ID","TRANMONTH_END"], how='left')\
                    .withColumn('LN_MNTLY_MAX_BAL_PLN',f.when(f.col("LN_MNTLY_MAX_BAL_PLN").isNull(), f.col("LN_CNTR_BAL_PLN")).otherwise(f.col("LN_MNTLY_MAX_BAL_PLN")))\
                    .withColumn('LN_MNTLY_MIN_BAL_PLN',f.when(f.col("LN_MNTLY_MIN_BAL_PLN").isNull(), f.col("LN_CNTR_BAL_PLN") ).otherwise(f.col("LN_MNTLY_MIN_BAL_PLN")))\
                    .withColumn('LN_MNTLY_MIN_BAL_PLN',f.when((f.col("LN_MNTLY_MIN_BAL_PLN").isNotNull()) & (f.col("LN_MNTLY_MIN_BAL_PLN") != 0), \
                    (f.col("LN_MNTLY_MIN_BAL_PLN")*f.lit(-1))).otherwise(f.col("LN_MNTLY_MIN_BAL_PLN")))\
                    .withColumn('LN_MNTLY_MAX_BAL_PLN',f.when((f.col("LN_MNTLY_MAX_BAL_PLN").isNotNull()) & (f.col("LN_MNTLY_MAX_BAL_PLN") != 0), \
                    (f.col("LN_MNTLY_MAX_BAL_PLN")*f.lit(-1))).otherwise(f.col("LN_MNTLY_MAX_BAL_PLN")))\
                    .withColumn('OG_MNTLY_MIN_BAL_PLN', f.when((f.col('AC_MNTLY_MIN_BAL_PLN')==f.lit(0) ) & (f.col('LN_MNTLY_MIN_BAL_PLN').isNotNull())\
                    , f.col('LN_MNTLY_MIN_BAL_PLN')).otherwise(f.col("AC_MNTLY_MIN_BAL_PLN")))\
                    .withColumn('OG_MNTLY_MAX_BAL_PLN', f.when((f.col('AC_MNTLY_MAX_BAL_PLN')==f.lit(0) ) & (f.col('LN_MNTLY_MAX_BAL_PLN').isNotNull())\
                    , f.col('LN_MNTLY_MAX_BAL_PLN')).otherwise(f.col("AC_MNTLY_MAX_BAL_PLN")))\
                    .drop(f.col('KR_CNTR_BAL_PLN'))\
                    .drop(f.col('KR_MNTLY_MAX_BAL_PLN'))\
                    .drop(f.col('KR_MNTLY_MIN_BAL_PLN'))\
                    .drop(f.col('LN_CNTR_BAL_PLN'))\
                    .join(cust['PESEL_NO','TRANMONTH_END','CUST_ID'].distinct(), on=["CUST_ID","TRANMONTH_END"], how='inner')\
                    .drop(f.col('CUST_DEPT_COLL_FLAG'))
balance2=balance2\
                    .withColumn('S_TMP', f.when(f.col('OG_MNTLY_MIN_BAL_PLN') > f.col('OG_MNTLY_MAX_BAL_PLN'), f.col('OG_MNTLY_MAX_BAL_PLN')).otherwise(f.lit(None)))\
                    .withColumn('OG_MNTLY_MAX_BAL_PLN', f.when(f.col('S_TMP').isNotNull(), f.col('OG_MNTLY_MIN_BAL_PLN')).otherwise(f.col("OG_MNTLY_MAX_BAL_PLN")))\
                    .withColumn('OG_MNTLY_MIN_BAL_PLN', f.when(f.col('S_TMP').isNotNull(), f.col('S_TMP')).otherwise(f.col("OG_MNTLY_MIN_BAL_PLN")))\
                    .withColumn('SALDO_MIN_MAX_PCT', f.round(f.col("OG_MNTLY_MIN_BAL_PLN")/f.col("OG_MNTLY_MAX_BAL_PLN") * f.lit(10000.00))/f.lit(100.00))\
                    .withColumn('SALDO_RANGE', f.col("OG_MNTLY_MAX_BAL_PLN")-f.col("OG_MNTLY_MIN_BAL_PLN"))\
                    .withColumn('LN_WYKORZYSTANIE_FL', f.when(f.col('OG_MNTLY_MIN_BAL_PLN')<0, f.lit(1)).otherwise(f.lit(0)))\
                    .withColumn('SALDO_CZYSZCZENIE_FL', f.when(f.col('SALDO_MIN_MAX_PCT')<10, f.lit(1)).otherwise(f.lit(0))).cache()

balance2.count()

#kch check
balance2.write.parquet(abt_hdfs_path + "/kch_check_balance2.parquet",'overwrite')

#AGGREGATES FOR BALANCE VARIABLES

bal_dictionary = {
                        'OG_MNTLY_MAX_BAL_PLN' : [["MAX","MIN","AVG","SUM"], 0.0001, False],
                        'OG_MNTLY_MIN_BAL_PLN' : [["MAX","MIN","AVG","SUM"], 0.0001, False],
                        'SALDO_MIN_MAX_PCT' : [["MAX","MIN","AVG"], 0.0001, False],
                        'SALDO_RANGE' : [["MAX","MIN","AVG"], 0.0001, False],
                        'LN_WYKORZYSTANIE_FL' : [["MAX","SUM"], 0.0001, False],
                        'SALDO_CZYSZCZENIE_FL' : [["MAX","SUM"], 0.0001, False]
                    }

balance2_agg=aggregate_variables(balance2, ['PESEL_NO','TRANMONTH_END'], bal_dictionary, 'BAL', breaks=16).cache()
balance2_agg.count()

#kch check
balance2_agg.write.parquet(abt_hdfs_path + "/kch_check_balance2_agg.parquet",'overwrite')

#IMPUTATION OF PCT VARIABLES (0-100)

balance_imputate_dictionary = {
                        'BAL_SALDO_MIN_MAX_PCT__AVG' : [100, "single"],
                        'BAL_SALDO_MIN_MAX_PCT__MIN' : [100, "single"],
                        'BAL_SALDO_MIN_MAX_PCT__MAX' : [100, "single"]
                    }

balance3_agg=imputate(balance2_agg, balance_imputate_dictionary).cache()
balance3_agg.count()

#kch check
balance3_agg.write.parquet(abt_hdfs_path + "/kch_check_balance3_agg.parquet",'overwrite')

#IMPUTATION OTHERS WITH ZERO

balance_imputate_dictionary = { 'BAL' : [0, "all"] }
balance4_agg=imputate(balance3_agg, balance_imputate_dictionary).cache()
balance4_agg.count()

#kch check
balance4_agg.write.parquet(abt_hdfs_path + "/kch_check_balance4_agg.parquet",'overwrite')

balance5_agg=spark.read.parquet(hdfs_path + "/temporary_files/balance_agg.parquet")

for c in balance4_agg.columns:
    balance4_agg = balance4_agg.withColumn(c, f.col(c).cast(balance5_agg[[c]].dtypes[0][1]))

balance4_agg.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/balance_agg.parquet","append")

#tutaj skoczone

spark.catalog.clearCache()
balance5_agg=spark.read.parquet(hdfs_path + "/temporary_files/balance_agg.parquet")

#RUNNING VARIABLES

balance_run_dictionary = {
                        'BAL_*.*_MAX' : [[3,6],['max','range']],
                        'BAL_*.*_SUM' : [[3,6],['sum','max','avg','range']],
                        'BAL_*.*_AVG' : [[3,6],['avg']],
                        'BAL_*.*_COUNT' : [[3,6],['sum',]]
                        }	

balance3_agg_months = running_vars(df=balance5_agg, 
                                        moving_dictionary=balance_run_dictionary, 
                                        date_column='TRANMONTH_END', 
                                        id_column='PESEL_NO', 
                                        breaks=40, 
                                        history_period = steering_dict["FINAL_DATA_PERIOD"])

balance3_agg_months.cache().count()

#SHARES

shares_dictionary = {
                         'BAL_*.*_MAX_MAX_*' : ['3M','6M'],
                        'BAL_*.*_SUM_SUM_*' : ['3M','6M'],
                        'BAL_*.*_AVG_AVG_*' : ['3M','6M'],
                        'BAL_*.*_COUNT_SUM_*' : ['3M','6M'],
                     }

bal=shares_variables(df=balance3_agg_months,shares_dictionary=shares_dictionary)

bal=bal.na.fill(0)
bal.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/balance.parquet","append")
spark.catalog.clearCache()

print "balance done"

################################################################Balance END################################################################

################################################################ACC_STAT_HIST start################################################################

ACC_STAT_HIST_vars_sql = su.parametrize_sql(su.read_sql('acc_stat_hist_pi'), steering_dict)
ACC_STAT_HIST = su.spark_sql_load(spark, 'pi', ACC_STAT_HIST_vars_sql, 'oracle')
ACC_STAT_HIST.cache().count()

ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_CRE_TOTAL_PLN', f.col('TRX_CNTR_CRE_TOTAL_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_CRE_CLEANED_TOTAL_PLN', f.col('TRX_CNTR_CRE_CLEANED_TOTAL_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_DEB_TOTAL_PLN', f.col('TRX_CNTR_DEB_TOTAL_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_DEB_CARD_PLN', f.col('TRX_CNTR_DEB_CARD_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_DEB_CASH_PLN', f.col('TRX_CNTR_DEB_CASH_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_DEB_TRANSF_PLN', f.col('TRX_CNTR_DEB_TRANSF_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRX_CNTR_CRE_CASH_PLN', f.col('TRX_CNTR_CRE_CASH_PLN').cast('decimal(18,2)'))
ACC_STAT_HIST=ACC_STAT_HIST.withColumn('TRANMONTH_END', f.col('TRANMONTH_END').cast('date'))

ACC_STAT_HIST=ACC_STAT_HIST.join(cust['PESEL_NO','TRANMONTH_END','CUST_ID'].distinct(), on=["CUST_ID","TRANMONTH_END"], how='inner')

print("====> KCH CHECKPOINT : 0")

ACC_STAT_dictionary = {
                        'TRX_CNTR_CRE_TOTAL_PLN' : [ ["MAX","AVG","SUM"], 0.0, False],
                        'TRX_CNTR_CRE_CLEANED_TOTAL_PLN' : [ ["MAX","AVG","SUM"], 0.0, False],
                        'TRX_CNTR_DEB_TOTAL_PLN' : [ ["MAX","AVG","SUM"], 0.0, False],
                        'TRX_CNTR_DEB_CARD_PLN' : [ ["MAX","AVG","SUM"], 0.0, False],
                        'TRX_CNTR_DEB_CASH_PLN' : [ ["MAX","AVG","SUM"], 0.0, False],
                        'TRX_CNTR_DEB_TRANSF_PLN' : [ ["MAX","AVG","SUM"], 0.0, False],
                        'TRX_CNTR_CRE_CASH_PLN' : [ ["MAX","AVG","SUM"], 0.0, False]
                    }


ACC_STAT_HIST_agg=aggregate_variables(ACC_STAT_HIST, ['PESEL_NO','TRANMONTH_END'], ACC_STAT_dictionary, 'ASH', breaks=16).cache()
ACC_STAT_HIST_agg.count()

ACC_STAT_HIST_imputate_dictionary = {
                        'ASH_' : [0, "all"]
                    }

ACC_STAT_HIST_imp=imputate(ACC_STAT_HIST_agg, ACC_STAT_HIST_imputate_dictionary).cache()
ACC_STAT_HIST_imp.count()

print("====> KCH CHECKPOINT : 1")

#########PCT VARIABLES IN GIVEN MONTH 

ACC_STAT_PCT_dictionary = {
    "ASH_TRX_CNTR_TOTAL_PLN__sum": "f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')/f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_CRE_TO_TOTAL_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_CRE_TOTAL_PLN__sum')/(f.col('ASH_TRX_CNTR_CRE_TOTAL_PLN__sum')+f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum'))",
    "ASH_TRX_CNTR_DEB_TO_TOTAL_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum')/(f.col('ASH_TRX_CNTR_CRE_TOTAL_PLN__sum')+f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum'))",
    "ASH_TRX_CNTR_CRE_CL_TO_TOTAL_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum')/(f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')+f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum'))",
    "ASH_TRX_CNTR_DEB_TO_TOTAL_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')/(f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')+f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum'))",
    "ASH_TRX_CNTR_DEB_CASH_TO_DEB_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_DEB_CASH_PLN__sum')/f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_DEB_CASH_TO_CRE_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_DEB_CASH_PLN__sum')/f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_DEB_CARD_TO_DEB_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_DEB_CARD_PLN__sum')/f.col('ASH_TRX_CNTR_DEB_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_DEB_CRE_CASH_TO_CRE_CL_PLN__sum_PCT": "f.col('ASH_TRX_CNTR_CRE_CASH_PLN__sum')/f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_DEB_CRE_CL_TO_CR_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')/f.col('ASH_TRX_CNTR_CRE_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_DEB_CRE_CL_TO_CR_PLN__sum_PCT":"f.col('ASH_TRX_CNTR_CRE_CLEANED_TOTAL_PLN__sum')/f.col('ASH_TRX_CNTR_CRE_TOTAL_PLN__sum')",
    "ASH_TRX_CNTR_CASH_TO_TOTAL_PLN__sum_PCT":"(f.col('ASH_TRX_CNTR_DEB_CASH_PLN__sum')+f.col('ASH_TRX_CNTR_CRE_CASH_PLN__sum'))/(f.col('ASH_TRX_CNTR_TOTAL_PLN__sum'))"
}

ACC_STAT_HIST_agg=pct_variables(ACC_STAT_HIST_imp,ACC_STAT_PCT_dictionary)

ACC_STAT_HIST_agg.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/ACC_STAT_HIST_agg.parquet","append")
spark.catalog.clearCache()
ACC_STAT_HIST_agg=spark.read.parquet(hdfs_path + "/temporary_files/ACC_STAT_HIST_agg.parquet")

ACC_STAT_HIST_dictionary = {
                        'ASH_TRX*.*_MAX' : [[3,6],['sum','max','avg','range']],
                        'ASH_TRX*.*_SUM' : [[3,6],['sum','max','avg','range']],
                        'ASH_TRX*.*_AVG' : [[3,6],['sum','max','avg','range']]
                        }	

ACC_STAT_HIST_agg_months = running_vars(df=ACC_STAT_HIST_agg, 
                                        moving_dictionary=ACC_STAT_HIST_dictionary, 
                                        date_column='TRANMONTH_END', 
                                        id_column='PESEL_NO', 
                                        breaks=50, 
                                        history_period = steering_dict["FINAL_DATA_PERIOD"]).cache()
ACC_STAT_HIST_agg_months.count()

print("====> KCH CHECKPOINT : 2")

shares_dictionary = {
                         'ASH_*.*_MAX_MAX_*' : ['3M','6M'],
                        'ASH_*.*_SUM_SUM_*' : ['3M','6M'],
                        'ASH_*.*_AVG_AVG_*' : ['3M','6M'],
                        'ASH_*.*_COUNT_SUM_*' : ['3M','6M'],
                     }

ash=shares_variables(df=ACC_STAT_HIST_agg_months,shares_dictionary=shares_dictionary).cache()
ash.count()

ash=imputate(ash, ACC_STAT_HIST_imputate_dictionary).cache()
ash.count()



ash.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/acc_stat_hist.parquet","append")
spark.catalog.clearCache()


print "ash done"
# ################################################################ACC_STAT_HIST END################################################################
'''
# ################################################################TRANSACTIONAL VARIABLES START################################################################
'''
trxn_vars_sql = su.parametrize_sql(su.read_sql('transactions_pi'), steering_dict)
trxn = su.spark_sql_load(spark, 'pi', trxn_vars_sql, 'oracle')
trxn.cache().count()

trxn.repartition(800).write.parquet(hdfs_path + "/temporary_files/trxn2.parquet","overwrite")
trxn=spark.read.parquet(hdfs_path + "/temporary_files/trxn2.parquet")
print("KCH CHECK", "/temporary_files/trxn2.parquet" )

#trxn=spark.read.parquet('arch/temp_data_prep_files/temporary_files/trxn.parquet')

trxn=trxn.withColumn('VAL_DATE', f.col('val_date').cast('date'))
trxn=trxn.withColumn('TRANMONTH_END',f.last_day(f.col('VAL_DATE'))).drop('TRANMONTH')
trxn=trxn.withColumn('CASH_TRANSF_TYP_CODE',f.when(trxn.CASH_TRANSF_TYP_CODE.isin(['?','-']),None).otherwise(trxn.CASH_TRANSF_TYP_CODE))
trxn=trxn.withColumn('TRANSF_TYP_CODE',f.when(trxn.TRANSF_TYP_CODE.isin(['?','-']),None).otherwise(trxn.TRANSF_TYP_CODE))
trxn=trxn.withColumn('DISTRIB_CHNL_CODE',f.when(trxn.DISTRIB_CHNL_CODE.isin(['?','-']),None).otherwise(trxn.DISTRIB_CHNL_CODE))
trxn=trxn.withColumn('TRANSF_SUBTYP_CODE',f.when(trxn.TRANSF_SUBTYP_CODE.isin(['?','-']),None).otherwise(trxn.TRANSF_SUBTYP_CODE))
trxn=trxn.withColumn('POSTG_TYP_CODE',f.when(trxn.POSTG_TYP_CODE.isin(['?','-']),None).otherwise(trxn.POSTG_TYP_CODE))
trxn=trxn.withColumn('AMT_PLN', f.col('AMT_PLN').cast('decimal(16,2)'))

trxn = trxn.join(cust, on=["CUST_ID","TRANMONTH_END"], how='inner').cache()
trxn.count()

#TRANSACTION TRANSFORMATION

trxn=trxn.withColumn('CASH_TRANSF_TYP_CODE',f.trim(f.col('CASH_TRANSF_TYP_CODE')))
trxn=trxn.withColumn('TRANSF_TYP_CODE',f.trim(f.col('TRANSF_TYP_CODE')))
trxn=trxn.withColumn('DISTRIB_CHNL_CODE',f.trim(f.col('DISTRIB_CHNL_CODE')))
trxn=trxn.withColumn('TRANSF_SUBTYP_CODE',f.trim(f.col('TRANSF_SUBTYP_CODE')))
trxn=trxn.withColumn('POSTG_TYP_CODE',f.trim(f.col('POSTG_TYP_CODE')))

trxn2=trxn.withColumn('TRAN_TYPE', f.when(f.col('CASH_TRANSF_TYP_CODE').isin(['KG','WG','PG']),f.lit('CASH')).otherwise(f.lit('WIRE')))\
        .withColumn('TRAN_SOURCE', f.when(f.col('CASH_TRANSF_TYP_CODE').isin(['KG','KN']),f.lit('CARD'))\
        .when(((f.col('TRANSF_TYP_CODE').isNull()==True) & (f.col('TRAN_TYPE') != 'CASH')), 'TECH')\
        .otherwise(f.lit('ROR')))\
        .withColumn('DISRIB_CHNL', f.when((f.col('DISTRIB_CHNL_CODE')).isin(['MC','ST','WP','CC']), f.lit('ST')).otherwise(f.col('DISTRIB_CHNL_CODE')))

######DIVIDING BETWEEND CARDS, TOTAL, TECHNICAL AND ROR TRANSACTIONS

card_trxn = trxn2.filter(f.col('TRAN_SOURCE')=='CARD').cache()
card_trxn.count()
ror_trxn = trxn2.filter(f.col('TRAN_SOURCE')=='ROR').cache()
ror_trxn.count()


####CARDS DATA

card_dictionary = {
                        'AMT_PLN POSTG_TYP_CODE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRAN_TYPE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRAN_TYPE POSTG_TYP_CODE' : [["MAX","SUM"], 0.0001, False],
                        'AMT_PLN' : [["MAX","AVG","SUM"], 0.0001, False],
                        'POSTG_TYP_CODE' : [["COUNT"], 0.0001, False],
                        'TRAN_TYPE' : [["COUNT"], 0.0001, False],
                        'TRAN_TYPE POSTG_TYP_CODE' : [["COUNT"], 0.0001, False]
                    }

card_agg=aggregate_variables(card_trxn, ['PESEL_NO','TRANMONTH_END'], card_dictionary, 'TRXN_CARD', breaks=12).cache()
card_agg.count()


card_agg_imp=card_agg.na.fill(0).cache()
card_agg_imp.count()

card_agg_imp.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/card_trxn_temp.parquet","append")
spark.catalog.clearCache()
print("KCH CHECK", "/temporary_files/card_trxn_temp.parquet" )
card_agg_imp=spark.read.parquet(hdfs_path + "/temporary_files/card_trxn_temp.parquet")

card_agg_dictionary = {
                        'TRXN_CARD_*.*_MAX' : [[3,6],['max','range']],
                        'TRXN_CARD_*.*_SUM' : [[3,6],['sum','max','avg','range']],
                        'TRXN_CARD_*.*_AVG' : [[3,6],['avg']],
                        'TRXN_CARD_*.*_COUNT' : [[3,6],['sum']]
                        }		

card_agg2 = running_vars(df=card_agg_imp, 
                                        moving_dictionary=card_agg_dictionary, 
                                        date_column='TRANMONTH_END', 
                                        id_column='PESEL_NO', 
                                        breaks=40, 
                                        history_period = steering_dict["FINAL_DATA_PERIOD"])

shares_dictionary = {
                         'TRXN_*.*_MAX_MAX_*' : ['3M','6M'],
                        'TRXN_*.*_SUM_SUM_*' : ['3M','6M'],
                        'TRXN_*.*_AVG_AVG_*' : ['3M','6M'],
                        'TRXN_*.*_COUNT_SUM_*' : ['3M','6M']
                     } 

card_agg3=shares_variables(df=card_agg2,shares_dictionary=shares_dictionary).cache()
card_agg3.count()

card_agg3=card_agg3.na.fill(0).cache()
card_agg3.count()

card_agg3.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/card_trxn.parquet","append")
print("KCH CHECK", "/temporary_files/card_trxn.parquet" )
spark.catalog.clearCache()

####ROR DATA


ror_dictionary = {
                        'AMT_PLN POSTG_TYP_CODE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRAN_TYPE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRAN_TYPE POSTG_TYP_CODE' : [["MAX","SUM"], 0.0001, False],
                        'AMT_PLN' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN DISRIB_CHNL' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRANSF_TYP_CODE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'POSTG_TYP_CODE' : [["COUNT"], 0.0001, False],
                        'TRAN_TYPE' : [["COUNT"], 0.0001, False],
                        'DISRIB_CHNL' : [["COUNT"], 0.0001, False],
                        'TRAN_TYPE POSTG_TYP_CODE' : [["COUNT"], 0.0001, False]
                    }

ror_agg=aggregate_variables(ror_trxn, ['PESEL_NO','TRANMONTH_END'], ror_dictionary, 'TRXN_ROR', breaks=12).cache()
ror_agg.count()

ror_agg_imp=ror_agg.na.fill(0).cache()
ror_agg_imp.count()

ror_agg_imp=ror_agg_imp.toDF(*[c.upper() for c in ror_agg_imp.columns])

ror_agg_imp.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/ror_trxn_temp.parquet","append")
spark.catalog.clearCache()
print("KCH CHECK", "/temporary_files/ror_trxn_temp.parquet" )
ror_agg_imp=spark.read.parquet(hdfs_path + "/temporary_files/ror_trxn_temp.parquet")

ror_agg_dictionary = {
                        'TRXN_ROR_*.*_MAX' : [[3,6],['max']],
                        'TRXN_ROR_*.*_SUM' : [[3,6],['sum','max','range']],
                        'TRXN_ROR_*.*_AVG' : [[3,6],['avg']],
                        'TRXN_ROR_*.*_COUNT' : [[3,6],['sum']]
                        }		

ror_agg2 = running_vars(df=ror_agg_imp, 
                                        moving_dictionary=ror_agg_dictionary, 
                                        date_column='TRANMONTH_END', 
                                        id_column='PESEL_NO', 
                                        breaks=60, 
                                        history_period = steering_dict["FINAL_DATA_PERIOD"]).cache()

ror_agg2.count()

shares_dictionary = {
                         'TRXN_*.*_MAX_MAX_*' : ['3M','6M'],
                        'TRXN_*.*_SUM_SUM_*' : ['3M','6M'],
                        'TRXN_*.*_AVG_AVG_*' : ['3M','6M'],
                        'TRXN_*.*_COUNT_SUM_*' : ['3M','6M']
                     } 

ror_agg3=shares_variables(df=ror_agg2,shares_dictionary=shares_dictionary).cache()
ror_agg3.count()

ror_agg2.unpersist()

ror_agg3=ror_agg3.na.fill(0).cache()
ror_agg3.count()

ror_agg3.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/ror_trxn.parquet","append")
print("KCH CHECK", "/temporary_files/ror_trxn.parquet" )
####ALL TRANSACTIONS DATA

tot_dictionary = {
                        'AMT_PLN POSTG_TYP_CODE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRAN_TYPE' : [["MAX","AVG","SUM"], 0.0001, False],
                        'AMT_PLN TRAN_TYPE POSTG_TYP_CODE' : [["MAX","SUM"], 0.0001, False],
                        'AMT_PLN' : [["MAX","AVG","SUM"], 0.0001, False],
                        'POSTG_TYP_CODE' : [["COUNT"], 0.0001, False],
                        'TRAN_TYPE' : [["COUNT"], 0.0001, False],
                        'TRAN_TYPE POSTG_TYP_CODE' : [["COUNT"], 0.0001, False]
                    }

tot_agg=aggregate_variables(trxn2, ['PESEL_NO','TRANMONTH_END'], tot_dictionary, 'TRXN_TOT', breaks=12).cache()
tot_agg.count()

tot_agg_imp=tot_agg.na.fill(0).cache()
tot_agg_imp.count()

tot_agg_imp.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/tot_trxn_temp.parquet","append")
spark.catalog.clearCache()
print("KCH CHECK", "/temporary_files/tot_trxn_temp.parquet" )
tot_agg_imp=spark.read.parquet(hdfs_path + "/temporary_files/tot_trxn_temp.parquet")

tot_agg_dictionary = {
                        'TRXN_TOT_*.*_MAX' : [[3,6],['max','range']],
                        'TRXN_TOT_*.*_SUM' : [[3,6],['sum','max','avg','range']],
                        'TRXN_TOT_*.*_AVG' : [[3,6],['avg']],
                        'TRXN_TOT_*.*_COUNT' : [[3,6],['sum']]
                        }		

tot_agg2 = running_vars(df=tot_agg_imp, 
                                        moving_dictionary=tot_agg_dictionary, 
                                        date_column='TRANMONTH_END', 
                                        id_column='PESEL_NO', 
                                        breaks=80, 
                                        history_period = steering_dict["FINAL_DATA_PERIOD"])


shares_dictionary = {
                         'TRXN_*.*_MAX_MAX_*' : ['3M','6M'],
                        'TRXN_*.*_SUM_SUM_*' : ['3M','6M'],
                        'TRXN_*.*_AVG_AVG_*' : ['3M','6M'],
                        'TRXN_*.*_COUNT_SUM_*' : ['3M','6M']
                     } 

tot_agg3=shares_variables(df=tot_agg2,shares_dictionary=shares_dictionary).cache()
tot_agg3.count()

tot_agg3=tot_agg3.na.fill(0).cache()
tot_agg3.count()

tot_agg3.write.partitionBy('TRANMONTH_END').parquet(hdfs_path + "/temporary_files/tot_trxn.parquet","append")

spark.catalog.clearCache()
print("KCH CHECK", "tot_trxn.parquet" )

###JOINING TRANSACTIONS

card_agg2=spark.read.parquet(hdfs_path + "/temporary_files/card_trxn.parquet")\
.drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))
ror_agg2=spark.read.parquet(hdfs_path + "/temporary_files/ror_trxn.parquet")\
.drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))
tot_agg2=spark.read.parquet(hdfs_path + "/temporary_files/tot_trxn.parquet")\
.drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))

trxn_joined = tot_agg2.join(card_agg2, on=['PESEL_NO','TRANMONTH_END'],how='left')\
                    .join(ror_agg2, on=['PESEL_NO','TRANMONTH_END'],how='left')

###PCT VARIABLES FOR TRANSACTIONS

trxn_PCT_dictionary = {
 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_CT_DT_SUM':"f.col( 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_CT_SUM')/f.col( 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_DT_SUM')",
 'TRXN_CARD_AMT_PLN_TRAN_TYPE_CASH_WIRE_SUM':"f.col( 'TRXN_CARD_AMT_PLN_TRAN_TYPE_CASH_SUM')/f.col( 'TRXN_CARD_AMT_PLN_TRAN_TYPE_WIRE_SUM')",
 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_CT_DT_SUM':"f.col( 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_CT_SUM')/f.col( 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_DT_SUM')",
 'TRXN_ROR_AMT_PLN_TRAN_TYPE_CASH_WIRE_SUM':"f.col( 'TRXN_ROR_AMT_PLN_TRAN_TYPE_CASH_SUM')/f.col( 'TRXN_ROR_AMT_PLN_TRAN_TYPE_WIRE_SUM')",
 'TRXN_ROR_AMT_PLN_TRANSF_TYP_CODE_KRJ_ZAG_SUM':"f.col( 'TRXN_ROR_AMT_PLN_TRANSF_TYP_CODE_KRJ_SUM')/f.col( 'TRXN_ROR_AMT_PLN_TRANSF_TYP_CODE_ZAG_SUM')",
 'TRXN_ROR_AMT_PLN_DISRIB_CHNL_PL_ST_SUM':"f.col( 'TRXN_ROR_AMT_PLN_DISRIB_CHNL_PL_SUM')/f.col( 'TRXN_ROR_AMT_PLN_DISRIB_CHNL_ST_SUM')",
 'TRXN_CARD_TOT_AMT_PLN__SUM':"f.col( 'TRXN_CARD_AMT_PLN__SUM')/f.col( 'TRXN_TOT_AMT_PLN__SUM')",
 'TRXN_ROR_TOT_AMT_PLN__SUM':"f.col( 'TRXN_ROR_AMT_PLN__SUM')/f.col( 'TRXN_TOT_AMT_PLN__SUM')",
 'TRXN_ROR_CARD_AMT_PLN__SUM':"f.col( 'TRXN_ROR_AMT_PLN__SUM')/f.col( 'TRXN_CARD_AMT_PLN__SUM')",
 'TRXN_CARD_TOT_AMT_PLN_POSTG_TYP_CODE_CT_SUM':"f.col( 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_CT_SUM')/f.col( 'TRXN_TOT_AMT_PLN_POSTG_TYP_CODE_CT_SUM')",
 'TRXN_ROR_TOT_AMT_PLN_POSTG_TYP_CODE_CT_SUM':"f.col( 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_CT_SUM')/f.col( 'TRXN_TOT_AMT_PLN_POSTG_TYP_CODE_CT_SUM')",
 'TRXN_CARD_ROR_AMT_PLN_POSTG_TYP_CODE_CT_SUM':"f.col( 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_CT_SUM')/f.col( 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_CT_SUM')",
 'TRXN_CARD_TOT_AMT_PLN_POSTG_TYP_CODE_DT_SUM':"f.col( 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_DT_SUM')/f.col( 'TRXN_TOT_AMT_PLN_POSTG_TYP_CODE_DT_SUM')",
 'TRXN_ROR_TOT_AMT_PLN_POSTG_TYP_CODE_DT_SUM':"f.col( 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_DT_SUM')/f.col( 'TRXN_TOT_AMT_PLN_POSTG_TYP_CODE_DT_SUM')",
 'TRXN_CARD_RORD_AMT_PLN_POSTG_TYP_CODE_DT_SUM':"f.col( 'TRXN_CARD_AMT_PLN_POSTG_TYP_CODE_DT_SUM')/f.col( 'TRXN_ROR_AMT_PLN_POSTG_TYP_CODE_DT_SUM')",
 'TRXN_CARD_TOT_AMT_PLN_TRAN_TYPE_CASH_SUM':"f.col( 'TRXN_CARD_AMT_PLN_TRAN_TYPE_CASH_SUM')/f.col( 'TRXN_TOT_AMT_PLN_TRAN_TYPE_CASH_SUM')",
 'TRXN_ROR_TOT_AMT_PLN_TRAN_TYPE_CASH_SUM':"f.col( 'TRXN_ROR_AMT_PLN_TRAN_TYPE_CASH_SUM')/f.col( 'TRXN_TOT_AMT_PLN_TRAN_TYPE_CASH_SUM')",
 'TRXN_CARD_ROR_AMT_PLN_TRAN_TYPE_CASH_SUM':"f.col( 'TRXN_CARD_AMT_PLN_TRAN_TYPE_CASH_SUM')/f.col( 'TRXN_ROR_AMT_PLN_TRAN_TYPE_CASH_SUM')",
 'TRXN_CARD_TOT_AMT_PLN_TRAN_TYPE_WIRE_SUM':"f.col( 'TRXN_CARD_AMT_PLN_TRAN_TYPE_WIRE_SUM')/f.col( 'TRXN_TOT_AMT_PLN_TRAN_TYPE_WIRE_SUM')",
 'TRXN_ROR_TOT_AMT_PLN_TRAN_TYPE_WIRE_SUM':"f.col( 'TRXN_ROR_AMT_PLN_TRAN_TYPE_WIRE_SUM')/f.col( 'TRXN_TOT_AMT_PLN_TRAN_TYPE_WIRE_SUM')",
 'TRXN_CARD_ROR_AMT_PLN_TRAN_TYPE_WIRE_SUM':"f.col( 'TRXN_CARD_AMT_PLN_TRAN_TYPE_WIRE_SUM')/f.col( 'TRXN_ROR_AMT_PLN_TRAN_TYPE_WIRE_SUM')"
}

trxn_joined3=pct_variables(trxn_joined,trxn_PCT_dictionary)
trxn_joined3.repartition(200).write.parquet(hdfs_path + "/trxn.parquet","overwrite")
print("KCH CHECK", "trxn.parquet" )

trxn_joined3.unpersist()
spark.catalog.clearCache()

################################################################TRANSACTIONAL VARIABLES END################################################################


#################### JOINING AND APPENDING ABT #################### SENDING DATA TO HIVE ###################

customers=spark.read.parquet(hdfs_path + "/cust_vars.parquet").drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))
products=spark.read.parquet(hdfs_path + "/product_vars.parquet").drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))
balance=spark.read.parquet(hdfs_path + "/balance.parquet").drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))
acc_stat_hist=spark.read.parquet(hdfs_path + "/acc_stat_hist.parquet").drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))
trxn=spark.read.parquet(hdfs_path + "/trxn.parquet").drop(f.col('DT_TIMESTAMP')).drop(f.col('TRANMONTH')).filter(f.col('TRANMONTH_END').isin(req_dates))

#JOINING ALL TABLES

abt=customers.join(products, on=['PESEL_NO','TRANMONTH_END'], how="left")\
        .join(balance, on=['PESEL_NO','TRANMONTH_END'], how="left")\
        .join(acc_stat_hist, on=['PESEL_NO','TRANMONTH_END'], how="left")\
        .join(trxn, on=['PESEL_NO','TRANMONTH_END'], how="left")

abt=abt.toDF(*[c.upper() for c in abt.columns])

abt2=abt.na.fill(0)

print("KCH CHECK", "read and join all abt data" )

#APPENDING DATA IN CRM SCHEMA

abt_old = spark.read.parquet(abt_hdfs_path + "/abt.parquet").columns
abt2=abt2[abt_old]
abt2.write.parquet(abt_hdfs_path + "/abt.parquet",'overwrite')
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
abt2 = spark.read.parquet(abt_hdfs_path + "/abt.parquet")

abt3=abt2.withColumn('dt',f.col('TRANMONTH_END').cast('string')).repartition(200)
abt3=abt3.toDF(*[c.lower() for c in abt3.columns])

abt3.createOrReplaceTempView("abt22")
#KCH final save
spark.sql("insert into crm.abt_general_data PARTITION (dt) select * from abt22")

print("KCH CHECK", "final insert into crm.abt_general_data" )

# types = [z.name.lower() + ' ' + str(z.dataType).replace('Type','').lower() for z in abt3.schema]
# types=types[0:len(types)-1]
# types=",".join(types)
# spark.sql("create table crm.abt_general_data (" + str(types) + ") PARTITIONED BY (dt string) STORED AS parquet")


# for i in x:
# 	abt3=abt2.filter(f.col('TRANMONTH_END')==i)
# 	abt3=abt3.withColumn('dt',f.col('TRANMONTH_END').cast('string')).repartition(200)
# 	abt3=abt3.toDF(*[c.lower() for c in abt3.columns])
# 	abt3.createOrReplaceTempView("abt")
# 	spark.sql("insert into crm.abt_general_data PARTITION (dt) select * from abt")	

#SENDING RESULTS

result = spark.sql("\
SELECT tranmonth_end, \
sum(trxn_card_amt_pln__sum) as trxn_check,\
sum(PR_PVSS_PROD_KRE_GOT_COUNT) as prod_check,\
sum(BAL_SALDO_RANGE__MAX_MAX3M_PAST) as bal_check,\
sum(ash_trx_cntr_cre_total_pln__sum) as ash_check,\
count(*) as count_check \
FROM crm.abt_general_data \
GROUP BY tranmonth_end \
order by tranmonth_end desc")

###########mail

result.show(100,False)

result=result.toPandas()

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
from itertools import chain

print "sending mails"

emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()
msg['Subject'] = "ABT general tabela"
msg['From'] = ''
    
html = """\
<html>
    <head></head>
    <body>
        <p>Cześć,<br><br>
            ABT general policzyło się.
            <br>
            {0}
            <br>
    </body>
</html>
""".format(result.to_html(index=False))

part1 = MIMEText(html, 'html')
msg.attach(part1)

# Send the message via our own SMTP server, but don't include the
# envelope header.
server = smtplib.SMTP('localhost')
server.sendmail(msg['From'], emaillist , msg.as_string())
