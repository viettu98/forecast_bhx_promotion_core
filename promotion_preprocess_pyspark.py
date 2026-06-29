from spark_config import spark_connect
from read_table_from_hadoop import read_table_csv, read_table_parquet
import pyspark.sql.functions as F
import logging
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, expr, lit, avg, countDistinct, max as spark_max, min as spark_min, size, collect_list, concat_ws
import time
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType, IntegerType, DateType

# Import thư viện cần thiết
import pytz
from datetime import datetime, timedelta

# Import PySpark
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

# HDFS Client
from hdfs import InsecureClient

# Typing
from typing import List, Optional, Dict, NamedTuple

# Garbage Collection & Time
import gc
import time

#Tạo đường link
import sys
import os

#Config spark
spark = spark_connect(appname="promotion_preprocess:daily")


#Tạo file log
import logging
logging.basicConfig(filename='execution_time.log', level=logging.INFO, 
                    format='%(asctime)s - %(message)s')
#Dan link hadoop
# os.environ["HADOOP_USER_NAME"]="hadoop"
linkPathHadoop = "hdfs://172.16.5.69:8020//forecast"



pm_promotion_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion.csv"
pm_item_exchangequantityunit_link = linkPathHadoop + "/data/bhx_masterdata/pm_item_exchangequantityunit.csv"
pm_product_link = linkPathHadoop + "/data/bhx_masterdata/pm_product.csv"
pm_promotiongiftgroup_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotiongiftgroup.csv"
pm_promotion_applyproduct_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion_applyproduct.csv"
pm_promotion_applysubgroup_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion_applysubgroup.csv"
pm_promotiongift_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotiongift.csv"
pr_saleprice_link = linkPathHadoop + "/data/bhx_masterdata/pr_saleprice.csv"
pm_store_link = linkPathHadoop + "/data/bhx_masterdata/pm_store.csv"
pm_promotion_applystore_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion_applystore.csv"
pm_store_storetypebyincome_link = linkPathHadoop + "/data/bhx_masterdata/pm_store_storetypebyincome.csv"
pm_promotion_applystoretypebymanual_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion_applystoretypebymanual.csv"
pm_promotion_applystoretype_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion_applystoretype.csv"
pm_promotion_noapplystore_link = linkPathHadoop + "/data/bhx_masterdata/pm_promotion_noapplystore.csv"
fc_promotion_blacklist_link = linkPathHadoop + "/data/bhx_forecast/fc_promotion_blacklist.csv"
pom_orderarea_link = linkPathHadoop + "/data/purchasing/pom_orderarea.csv"
pom_orderarea_logisticstore_link = linkPathHadoop + "/data/purchasing/pom_orderarea_logisticstore.csv"
pom_orderarea_calcstore_link = linkPathHadoop + "/data/purchasing/pom_orderarea_calcstore.csv"
pom_productstockdays_link = linkPathHadoop + "/data/purchasing/pom_productstockdays.csv"
show_store_product_link = linkPathHadoop + "/data/bachhoaxanh/show_store_product.csv"
df_dimdate_link = linkPathHadoop + "/data/bachhoaxanh/dim_date.csv"
fc_promotion_blacklist_link = linkPathHadoop + "/data/bhx_forecast/fc_promotion_blacklist.csv"

# Bảng Input
pm_promotion = read_table_csv(spark, pm_promotion_link)
pm_item_exchangequantityunit = read_table_csv(spark, pm_item_exchangequantityunit_link)
pm_product = read_table_csv(spark, pm_product_link)
pm_promotiongiftgroup = read_table_csv(spark, pm_promotiongiftgroup_link)
pm_promotion_applyproduct = read_table_csv(spark, pm_promotion_applyproduct_link)
pm_promotion_applysubgroup = read_table_csv(spark, pm_promotion_applysubgroup_link)
pm_promotiongift = read_table_csv(spark, pm_promotiongift_link)
pr_saleprice = read_table_csv(spark, pr_saleprice_link)
pm_store = read_table_csv(spark, pm_store_link)
pm_promotion_applystore = read_table_csv(spark, pm_promotion_applystore_link)
pm_store_storetypebyincome = read_table_csv(spark, pm_store_storetypebyincome_link)
pm_promotion_applystoretypebymanual = read_table_csv(spark, pm_promotion_applystoretypebymanual_link)
pm_promotion_applystoretype = read_table_csv(spark, pm_promotion_applystoretype_link)
pm_promotion_noapplystore = read_table_csv(spark, pm_promotion_noapplystore_link)
fc_promotion_blacklist = read_table_csv(spark, fc_promotion_blacklist_link)
pom_orderarea = read_table_csv(spark, pom_orderarea_link)
pom_orderarea_logisticstore = read_table_csv(spark, pom_orderarea_logisticstore_link)
pom_orderarea_calcstore = read_table_csv(spark, pom_orderarea_calcstore_link)
pom_productstockdays = read_table_csv(spark, pom_productstockdays_link)
show_store_product = read_table_csv(spark, show_store_product_link)
df_dimdate = read_table_csv(spark, df_dimdate_link)
df_Promotion_blacklist = read_table_csv(spark, fc_promotion_blacklist_link)

# Giản lược cột:

pr_saleprice = pr_saleprice.select('newsaleprice', 'productid', 'priceareaid', 'applydate', 'isreview', 'saleprice')
pm_promotion_applystoretype = pm_promotion_applystoretype.select('promotionid', 'storetypeid', 'isdeleted')
show_store_product = show_store_product.select('productid', 'storeid', 'statusid')

# SQL df_dcStoreItem

pom_orderarea.createOrReplaceTempView("pom_orderarea")
pom_orderarea_logisticstore.createOrReplaceTempView("pom_orderarea_logisticstore")
pom_orderarea_calcstore.createOrReplaceTempView("pom_orderarea_calcstore")
pom_productstockdays.createOrReplaceTempView("pom_productstockdays")
pm_product.createOrReplaceTempView("pm_product")
show_store_product.createOrReplaceTempView("show_store_product")

query1 = f'''SELECT 
  tmp.logisticstoreid dcid, 
  tmp.calcstoreid storeid, 
  tmp.itemid 
FROM 
  (
   SELECT 
      pom_orderarea_logisticstore.logisticstoreid, 
      pom_orderarea_calcstore.calcstoreid, 
      pm_product.itemid 
    FROM 
      pom_orderarea 
      JOIN pom_orderarea_logisticstore ON pom_orderarea_logisticstore.orderareaid = pom_orderarea.orderareaid 
      JOIN pom_orderarea_calcstore ON pom_orderarea_calcstore.orderareaid = pom_orderarea.orderareaid 
      JOIN pom_productstockdays ON pom_productstockdays.orderareaid = pom_orderarea.orderareaid 
      JOIN pm_product ON pm_product.productid = pom_productstockdays.productid 
    WHERE 
      pom_orderarea.isdeleted = FALSE 
      AND pom_orderarea_logisticstore.isdefault = TRUE
      AND EXISTS (
        SELECT 
          1 
        FROM 
          show_store_product show_dc_product 
          JOIN pm_product pm_dcproduct ON pm_dcproduct.productid = show_dc_product.productid 
        WHERE 
          show_dc_product.statusid = 3 
          AND pm_dcproduct.itemid = pm_product.itemid)) as tmp 
WHERE 
  EXISTS (
    SELECT 
      1 
    FROM 
      show_store_product 
      JOIN pm_product ON pm_product.productid = show_store_product.productid 
    WHERE 
      show_store_product.storeid = tmp.calcstoreid 
      AND show_store_product.statusid = 3 
      AND pm_product.itemid = tmp.itemid 
    GROUP BY 
      pm_product.itemid);'''
# Chạy truy vấn
df_dcStoreItem = spark.sql(query1)


# SQL df_promotion

pm_promotion.createOrReplaceTempView("pm_promotion")
pm_promotiongiftgroup.createOrReplaceTempView("pm_promotiongiftgroup")
pm_promotion_applyproduct.createOrReplaceTempView("pm_promotion_applyproduct")
pm_promotion_applysubgroup.createOrReplaceTempView("pm_promotion_applysubgroup")
pm_promotiongift.createOrReplaceTempView("pm_promotiongift")
pm_product.createOrReplaceTempView("pm_product")
pr_saleprice.createOrReplaceTempView("pr_saleprice")
pm_item_exchangequantityunit.createOrReplaceTempView("pm_item_exchangequantityunit")
pm_store.createOrReplaceTempView("pm_store")
pm_promotion_applystore.createOrReplaceTempView("pm_promotion_applystore")
pm_store_storetypebyincome.createOrReplaceTempView("pm_store_storetypebyincome")
pm_promotion_applystoretypebymanual.createOrReplaceTempView("pm_promotion_applystoretypebymanual")
pm_promotion_applystoretype.createOrReplaceTempView("pm_promotion_applystoretype")
pm_promotion_noapplystore.createOrReplaceTempView("pm_promotion_noapplystore")

query2 = f'''WITH tmp_applystore AS (
 SELECT
    pm_promotion.promotionid,
    pm_store.storeid
FROM
    pm_store
    LEFT JOIN pm_promotion
        ON 1 = 1  -- Ensures pm_promotion is available before any reference
    LEFT JOIN pm_promotion_applystore
        ON pm_promotion_applystore.promotionid = pm_promotion.promotionid
        AND pm_promotion_applystore.storeid = pm_store.storeid
    LEFT JOIN pm_store_storetypebyincome
        ON pm_store_storetypebyincome.storeid = pm_store.storeid
    LEFT JOIN pm_promotion_applystoretypebymanual
        ON pm_promotion_applystoretypebymanual.promotionid = pm_promotion.promotionid
    LEFT JOIN pm_promotion_applystoretype
        ON pm_promotion_applystoretype.promotionid = pm_promotion.promotionid
        AND pm_promotion_applystoretype.storetypeid = pm_store.storetypeid
        AND pm_promotion_applystoretype.isdeleted = FALSE
WHERE
    pm_store.storetypeid IN (1, 19, 20, 23)
    AND NOT EXISTS (
        SELECT 1 
        FROM pm_promotion_noapplystore
        WHERE
            pm_promotion_noapplystore.promotionid = pm_promotion.promotionid
            AND pm_promotion_noapplystore.storeid = pm_store.storeid
    )
    AND (
        (
            pm_promotion.isapplyallstore = FALSE
            AND (
                pm_promotion_applystore.storeid IS NOT NULL
                OR pm_promotion_applystoretype.promotionid IS NOT NULL
            )
        )
        OR (
            pm_promotion.isapplyallstore = TRUE
            AND (
                (pm_promotion.iseliminateonlinestore = TRUE 
                AND pm_store.storetypeid NOT IN (20, 23))
                OR pm_promotion.iseliminateonlinestore = FALSE
            )
            AND (
                pm_promotion_applystoretypebymanual.storetypebymanualid IS NULL 
                OR pm_promotion_applystoretypebymanual.storetypebymanualid = pm_store_storetypebyincome.storetypebymanualid
            )
        )
    )
)
SELECT
  pm_promotion.promotionid,
  pm_promotion.promotionname,
  pm_promotion.promotiontype,
  pm_promotion.fromdate,
  pm_promotion.todate,
  pm_promotion.mintotalmoney,
  tmp_applystore.storeid,
  pm_promotiongiftgroup.promotiongifttype,
  pm_promotiongiftgroup.discounttype,
  pm_promotiongiftgroup.discountvalue,
  pm_promotiongift.productid AS giftproductid,
  pm_promotiongift.quantity AS giftproductquantity,
  pm_promotiongift.promotionprice,
  CASE
    WHEN (pm_promotion.fromdate >= giftprice.applydate OR pm_promotion.todate >= giftprice.applydate)
      AND giftprice.isreview = TRUE
      AND giftprice.newsaleprice > 100 THEN giftprice.newsaleprice
    WHEN giftprice.saleprice > 100 THEN giftprice.saleprice
    ELSE pm_promotiongift.marketprice
  END AS giftproductprice,
  COALESCE(pm_applyproduct.productid, pm_productapplysubgroup.productid) AS productidmain,
  COALESCE(NULLIF(pm_promotion_applyproduct.quantity, 0), NULLIF(pm_promotion_applysubgroup.quantity, 0), NULLIF(pm_promotion.quantityperobject, 0), 0) AS mainproductquantity,
  price.newsaleprice,
  pm_promotion.promostoretype,
  pm_promotion.iscontinuouspromotion,
  pm_promotion.oldpromotionid,
  pm_promotion.isnosellingpower,
  pm_promotiongift.discountpercent,
  COALESCE(pm_applyproduct.itemid, pm_productapplysubgroup.itemid) AS itemid,
  COALESCE(pm_item_exchangequantityunit.exchangequantity, 1) AS exchangequantity,
  pm_promotion.totalorderpromotiontype,
  pm_promotion.mintotalmoneyquantity,
  CASE WHEN pm_promotion.isapplyallproduct IS NOT NULL THEN 1 ELSE NULL END AS isapplyallproduct,
  pm_promotion.maxtotalmoneyquantity,
  pm_promotion.valuenextstep,
  pm_promotion.isapplynextstep,
  CASE WHEN pm_promotiongiftgroup.donatetype = 2 THEN pm_promotiongiftgroup.totalquantitydonate END AS totalquantitydonate2,
  pm_promotiongiftgroup.quantitypernextstep,
  CASE WHEN pm_promotiongiftgroup.donatetype = 4 THEN pm_promotiongiftgroup.percentvalue END AS totalquantitydonate4,
  pm_promotiongiftgroup.giftproducttype,
  pm_promotiongift.minquantity,
  pm_promotion.percentcustomerbenefit
FROM
  pm_promotion
  JOIN pm_promotiongiftgroup
    ON pm_promotion.promotionid = pm_promotiongiftgroup.promotionid
  LEFT JOIN pm_promotion_applyproduct
    ON pm_promotion.promotionid = pm_promotion_applyproduct.promotionid
  LEFT JOIN pm_promotion_applysubgroup
    ON pm_promotion.promotionid = pm_promotion_applysubgroup.promotionid
  LEFT JOIN pm_promotiongift
    ON pm_promotiongiftgroup.promotiongiftgroupid = pm_promotiongift.promotiongiftgroupid
  LEFT JOIN pm_product pm_applyproduct
    ON pm_applyproduct.productid = pm_promotion_applyproduct.productid
  LEFT JOIN pm_product AS pm_productapplysubgroup
    ON pm_productapplysubgroup.subgroupid = pm_promotion_applysubgroup.subgroupid
    AND pm_productapplysubgroup.isdeleted = FALSE
    AND pm_productapplysubgroup.isactived = TRUE
  LEFT JOIN pr_saleprice price
    ON price.productid = COALESCE(pm_applyproduct.productid, pm_productapplysubgroup.productid)
    AND price.priceareaid = 643
  LEFT JOIN pr_saleprice giftprice
    ON giftprice.productid = pm_promotiongift.productid
    AND giftprice.priceareaid = 643
  LEFT JOIN pm_item_exchangequantityunit
    ON pm_item_exchangequantityunit.itemid = COALESCE(pm_applyproduct.itemid, pm_productapplysubgroup.itemid)
    AND pm_item_exchangequantityunit.exchangequantityunitid = COALESCE(pm_applyproduct.quantityunitid, pm_productapplysubgroup.quantityunitid)
  LEFT JOIN tmp_applystore
    ON tmp_applystore.promotionid = pm_promotion.promotionid
 WHERE
  pm_promotion.promotionid != '731463'
  AND pm_promotion.isdeleted = FALSE
  AND COALESCE(CAST(pm_promotion.isnosellingpower AS BOOLEAN), FALSE) = FALSE
  AND pm_promotion.todate > pm_promotion.fromdate
  AND ( ( pm_promotion.isreview = TRUE AND pm_promotion.fromdate <= now( ) ) OR pm_promotion.fromdate > now( ) )
  AND (
                        pm_promotion.promotiontype IN (1,4,9)
                        OR (
                            pm_promotion.promotiontype = 8 
                            AND (
                                pm_promotiongiftgroup.promotiongifttype IN (3, 6, 4) 
                                OR (
                                pm_promotiongiftgroup.promotiongifttype = 5
                                AND pm_promotiongift.productid = COALESCE(pm_applyproduct.productid, pm_productapplysubgroup.productid)
                                )
                            )
                        )
                )'''
try:
    # Thực thi câu truy vấn
    df_Promotion = spark.sql(query2)
    print("Query executed successfully.")
except Exception as e:
    # Ghi lỗi vào file log
    logging.error("An error occurred while executing the query: %s", query2, exc_info=True)
    print("An error occurred. Check the log file for details.")


# df_promotion.show()
#Chỉnh sửa định dạng ngày:
df_Promotion = df_Promotion.withColumnRenamed("fromdate", "initial_fromdate") \
                            .withColumnRenamed("todate", "initial_todate")

df_Promotion = df_Promotion.withColumn('fromdate', F.when(F.hour(F.col('initial_fromdate'))>21,F.date_add(F.col('initial_fromdate'),1)).otherwise(F.col('initial_fromdate')))\
                                                    .withColumn('todate', F.when(F.hour(F.col('initial_todate'))<8,F.date_add(F.col('initial_todate'),-1)).otherwise(F.col('initial_todate')))\
                                                    .dropna(subset=['fromdate', 'todate'])

today = datetime.today().strftime('%Y-%m-%d')
future_date = datetime.today() + timedelta(days=76)

future_date = future_date.strftime('%Y-%m-%d')
print(future_date)
print(today)
df_Promotion = df_Promotion.filter(F.col('todate') >= 'future_date')

#Tạo 2 bảng df_Promotion_storeid và df_Promotion_non_storeid:
df_Promotion_storeid = df_Promotion.select("promotionid","storeid").distinct()

df_Promotion_non_storeid = df_Promotion.drop("storeid")
df_Promotion_non_storeid = df_Promotion_non_storeid.distinct()
df_Promotion_non_storeid = df_Promotion_non_storeid.limit(df_Promotion_non_storeid.count())


#Loại bỏ các dòng Null và Rỗng StoreID của bảng df_Promotion_storeid:
df_Promotion_storeid = df_Promotion_storeid.filter(
    (col("storeid").isNotNull()) & (col("storeid") != "")
)

# Chuyển đổi kiểu dữ liệu của cột giftproductprice sang float
df_Promotion = df_Promotion.withColumn("giftproductprice", col("giftproductprice").cast("double"))

# Tính toán các giá trị tổng hợp
DistinctCounts = (
    df_Promotion_non_storeid.groupBy("promotionid")
    .agg(
        countDistinct("productidmain").alias("promotion_count"),
        avg("giftproductprice").alias("avg_giftproductprice"),
        avg("newsaleprice").alias("avg_newsaleprice"),
        countDistinct("giftproductid").alias("promotiongift_count")
    )
)

# Merge hai DataFrame bằng inner join
PromotionWithAgg = df_Promotion_non_storeid.join(DistinctCounts, on="promotionid", how="inner")

#Tính df_pre_PromotionBenefit:
PromotionWithAgg.createOrReplaceTempView("PromotionWithAgg")

query1 = f'''
SELECT
    *,
    CASE
        WHEN promotiontype = 8 AND promotiongifttype = 4 THEN
            CASE
                WHEN promotiongift_count = 1 THEN
                    CASE
                        WHEN totalorderpromotiontype = 4 THEN
                            CASE
                                WHEN giftproductid = productidmain THEN
                                    CASE
                                        WHEN totalquantitydonate4 IS NOT NULL THEN totalquantitydonate4 / 100
                                        WHEN totalquantitydonate2 IS NOT NULL THEN
                                            (giftproductprice * giftproductquantity) /
                                            (newsaleprice * (mintotalmoneyquantity + giftproductquantity))
                                    END
                                WHEN giftproductid != productidmain THEN
                                    (giftproductprice * giftproductquantity) /
                                    ((newsaleprice * mintotalmoneyquantity) + (giftproductprice * giftproductquantity))
                                ELSE NULL
                            END
                        WHEN totalorderpromotiontype = 2 THEN
                            CASE
                                WHEN totalquantitydonate4 IS NOT NULL THEN totalquantitydonate4 / 100
                                WHEN totalquantitydonate2 IS NOT NULL THEN
                                    (giftproductprice * giftproductquantity) /
                                    (mintotalmoneyquantity + (giftproductprice * giftproductquantity))
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN promotiongift_count > 1 OR promotion_count > 1 THEN
                    CASE
                        WHEN totalorderpromotiontype = 4 THEN
                            CASE
                                WHEN totalquantitydonate4 IS NOT NULL THEN totalquantitydonate4 / 100
                                WHEN totalquantitydonate2 IS NOT NULL THEN
                                    (avg_giftproductprice * giftproductquantity) /
                                    ((avg_newsaleprice * mintotalmoneyquantity) + (avg_giftproductprice * giftproductquantity))
                                ELSE NULL
                            END
                        WHEN totalorderpromotiontype = 2 THEN
                            CASE
                                WHEN totalquantitydonate4 IS NOT NULL THEN totalquantitydonate4 / 100
                                WHEN totalquantitydonate2 IS NOT NULL THEN
                                    (avg_giftproductprice * giftproductquantity) /
                                    (mintotalmoneyquantity + (giftproductprice * giftproductquantity))
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END
        ELSE NULL
    END AS promotionbenefit_gift
FROM
    PromotionWithAgg;
;'''

# Thực thi câu truy vấn
df_pre_PromotionBenefit = spark.sql(query1)
print("Query1 executed successfully.")
#Tính df_PromotionBenefit:
df_pre_PromotionBenefit.createOrReplaceTempView("df_pre_PromotionBenefit")
query2 = f'''
SELECT
    *,
    CASE
        WHEN (promotiontype = 4 AND promotiongifttype = 4 AND giftproductid = productidmain) THEN
            (giftproductprice*giftproductquantity)/ (newsaleprice*(mintotalmoney+giftproductquantity))
		WHEN (promotiontype = 4 AND promotiongifttype = 4 AND giftproductid != productidmain) THEN
			(giftproductprice*giftproductquantity)/ (newsaleprice*mintotalmoney + giftproductprice*giftproductquantity)
			
        WHEN (promotiontype = 4 AND promotiongifttype = 3 AND discounttype = 2) THEN
            discountvalue / (newsaleprice * mintotalmoney)
        WHEN (promotiontype = 4 AND promotiongifttype = 3 AND discounttype = 1) THEN
            discountvalue / 100
        WHEN (promotiontype = 4 AND promotiongifttype IS NULL AND discounttype = 2) THEN
            discountvalue / (newsaleprice * mintotalmoney)
			
		WHEN (promotiontype = 4 AND promotiongifttype = 5 AND discountpercent != 0) THEN
			discountpercent /100
		WHEN (promotiontype = 4 AND promotiongifttype = 5 AND discountpercent = 0) THEN	
			promotionprice / (newsaleprice*mintotalmoney)
			
        WHEN (promotiontype = 1 AND promotiongifttype = 3 AND discounttype = 2) THEN
            discountvalue / (mainproductquantity * newsaleprice)
        WHEN (promotiontype = 1 AND promotiongifttype = 3 AND discounttype = 1) THEN
            discountvalue / 100
        WHEN (promotiontype = 1 AND promotiongifttype = 4 AND giftproductprice IS NULL) THEN
            discountvalue / 100
        WHEN (promotiontype = 1 AND promotiongifttype = 4 AND giftproductprice IS NOT NULL AND giftproductid = productidmain) THEN
            (giftproductprice * giftproductquantity) / (newsaleprice * (mainproductquantity + giftproductquantity))
        WHEN (promotiontype = 1 AND promotiongifttype = 4 AND giftproductprice IS NOT NULL AND giftproductid != productidmain) THEN
            (giftproductprice * giftproductquantity) / (newsaleprice * mainproductquantity + giftproductprice * giftproductquantity)
        WHEN (promotiontype = 1 AND promotiongifttype = 5 AND discountpercent != 0) THEN
            discountpercent / 100
        WHEN (promotiontype = 1 AND promotiongifttype = 5 AND discountpercent = 0) THEN
            promotionprice / (newsaleprice * mainproductquantity)
			
		WHEN promotiontype = 8 AND promotiongifttype = 3 AND discounttype = 1 THEN 
		    discountvalue / 100
		WHEN promotiontype = 8 AND promotiongifttype = 3 AND discounttype = 3 AND newsaleprice IS NOT NULL THEN 
		    (newsaleprice - discountvalue)/ newsaleprice
			
		WHEN promotiontype = 8 AND promotiongifttype = 3 AND discounttype = 2 AND totalorderpromotiontype = 2 AND promotion_count = 1 
            THEN discountvalue / 
                CASE 
                    WHEN mintotalmoneyquantity > newsaleprice THEN mintotalmoneyquantity
                    ELSE newsaleprice
                END
		WHEN promotiontype = 8 AND promotiongifttype = 3 AND discounttype = 2 AND totalorderpromotiontype = 2 AND promotion_count > 1 
            THEN discountvalue / mintotalmoneyquantity	
			
		WHEN promotiontype = 8 AND promotiongifttype = 3 AND discounttype = 2 AND totalorderpromotiontype = 4
			THEN discountvalue / 
                CASE 
                    WHEN mintotalmoneyquantity < 1 THEN newsaleprice
                    ELSE newsaleprice * mintotalmoneyquantity
                END
			
		WHEN promotiontype = 8 AND promotiongifttype = 6 AND discounttype = 2 AND totalorderpromotiontype = 4 THEN 
		    discountvalue / (newsaleprice * mintotalmoneyquantity)
		WHEN promotiontype = 8 AND promotiongifttype = 6 AND discounttype = 2 AND totalorderpromotiontype = 2 THEN 
		    discountvalue / mintotalmoneyquantity
		WHEN promotiontype = 8 AND promotiongifttype = 6 AND discounttype = 1 THEN 
		    discountvalue / 100
		WHEN promotiontype = 8 AND promotiongifttype = 4 THEN
        CASE
            WHEN discounttype = 2 AND totalorderpromotiontype = 4 THEN
                ((discountvalue / (newsaleprice * mintotalmoneyquantity)) + promotionbenefit_gift) / 2
            WHEN discounttype = 2 AND totalorderpromotiontype = 2 THEN
                ((discountvalue / mintotalmoneyquantity) + promotionbenefit_gift) / 2
            WHEN discounttype = 1 THEN
                ((discountvalue / 100) + promotionbenefit_gift) / 2
            WHEN discounttype = 0 OR discounttype IS NULL THEN promotionbenefit_gift
        END

		WHEN promotiontype = 8 AND promotiongifttype = 5 AND giftproductid = productidmain AND promotionprice IS NULL AND discountpercent != 0  THEN
		    (1 - discountpercent/100) / (minquantity + 1)
		WHEN promotiontype = 8 AND promotiongifttype = 5 AND giftproductid = productidmain AND promotionprice IS NOT NULL AND discountpercent = 0 THEN
		    (newsaleprice - promotionprice)/ (newsaleprice * (minquantity + 1))

        ELSE
            NULL
    END AS promotionbenefit
FROM
    df_pre_PromotionBenefit
;'''

# Thực thi câu truy vấn
df_PromotionBenefit = spark.sql(query2)
print("Query2 executed successfully.")

# Loại bỏ các dòng trùng lặp
df_PromotionBenefit = df_PromotionBenefit.distinct()

# Điền giá trị mặc định cho cột 'giftproductid' nếu là null
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    'giftproductid',
    F.when(F.col('giftproductid').isNull(), F.lit(0)).otherwise(F.col('giftproductid'))
)

# Lọc các dòng có điều kiện 'giftproductid' != 0 và 'promotionbenefit' > 0.95
df_PromotionBenefit = df_PromotionBenefit.filter(
    ~((F.col('giftproductid') != 0) & (F.col('promotionbenefit') > 0.95))
)

# Lọc các dòng có 'promotionbenefit' trong khoảng [0, 2)

df_PromotionBenefit = df_PromotionBenefit.filter(
    (F.col('promotionbenefit') >= 0) & (F.col('promotionbenefit') < 2)
)
# Lọc các dòng có 'promotionid' nằm trong blacklist
df_PromotionBenefit = df_PromotionBenefit.join(
    df_Promotion_blacklist,
    on='promotionid',
    how='left_anti'
)
print("df_PromotionBenefitxxx executed successfully.")


# Thêm cột minproductquantity
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    "minproductquantity",
    when(col("promotiontype") == 1, col("mainproductquantity"))
    .when(col("promotiontype") == 4, col("mintotalmoney"))
    .when(col("promotiontype") == 8, col("mintotalmoneyquantity"))
)

# Áp dụng điều kiện lọc cho minproductquantity
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    "minproductquantity", 
    when((col("minproductquantity").cast("double").isNotNull()) & (col("minproductquantity") < 500), col("minproductquantity"))
    .otherwise(None)
)

# Thêm cột alpha
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    "alpha", 
    when(col("promotion_count") > 1, 1 / (0.5 * col("promotion_count"))).otherwise(lit(1))
)

# Sao lưu promotionbenefit trước khi nhân với alpha
df_PromotionBenefit = df_PromotionBenefit.withColumn("b4alpha_promotionbenefit", col("promotionbenefit"))

# Cập nhật giá trị promotionbenefit
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    "promotionbenefit", col("alpha") * col("promotionbenefit")
)
# Thêm cột 'is_giftproductprice_null' với logic tương tự apply
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    'is_giftproductprice_null',
    F.when(
        (F.col('promotiongifttype') == 4) &
        (F.col('promotionbenefit').isNull()) &
        ((F.coalesce(F.col('giftproductprice'), F.lit(-1)) < 500) | F.col('giftproductprice').isNull()),
        F.lit(1)
    ).otherwise(F.lit(0))
)

# Thêm cột 'is_promotionbenefit_null' dựa trên null của 'promotionbenefit'
df_PromotionBenefit = df_PromotionBenefit.withColumn(
    'is_promotionbenefit_null',
    F.when(F.col('promotionbenefit').isNull(), F.lit(1)).otherwise(F.lit(0))
)

# Lưu kết quả vào df_check_isnull
df_check_isnull = df_PromotionBenefit
print("df_check_isnull executed successfully.")

#Tạo bảng list ds storeid theo promotionID:
window_spec = Window.partitionBy("promotionid")

df_Promotion_storeidlist = df_Promotion_storeid.groupBy('promotionid').agg(concat_ws(",", collect_list("storeid")).alias('storeidlist'))



# Tạo df_PromotionBenefit_pre_merge
df_PromotionBenefit_pre_merge = df_Promotion_storeidlist.join(df_check_isnull, on="promotionid", how="left")

print("df_PromotionBenefit_pre_merge executed successfully.")


# Xuất file df_PromotionBenefit_pre
df_PromotionBenefit_pre_merge.write.format("parquet").mode("overwrite").save(r"hdfs://172.16.5.69:8020/forecast/check/promotion_check/df_PromotionBenefit_pre_daily/{today}.parquet")

# Tạo df_PromotionBenefit_merge
df_PromotionBenefit_merge = df_Promotion_storeid.join(df_check_isnull, on="promotionid", how="left")
# df_PromotionBenefit_merge.show()

# Lọc các cột cần thiết từ df_check_isnull
df_PromotionBenefit_avg = df_PromotionBenefit_merge.select(
    'itemid','productidmain','fromdate', 'todate', 
    'promotionid','promotiontype','promotiongifttype','discounttype', 
    'storeid', 'exchangequantity','minproductquantity', 'promotionbenefit', 
    'is_giftproductprice_null', 'is_promotionbenefit_null'
)


# Kiểm tra DataFrame có rỗng hay không
if df_PromotionBenefit_avg.rdd.isEmpty():
    df_PromotionBenefit_avg = df_PromotionBenefit_avg
else:
    # Thay thế null bằng 0
    df_PromotionBenefit_avg = df_PromotionBenefit_avg.fillna(0)

    # Xác định các cột nhóm
    group_columns = [
        col for col in df_PromotionBenefit_avg.columns
        if col not in ['promotionbenefit', 'is_giftproductprice_null', 'is_promotionbenefit_null']
    ]

    # Tính trung bình các cột 'promotionbenefit', 'is_giftproductprice_null', 'is_promotionbenefit_null'
    df_PromotionBenefit_avg = df_PromotionBenefit_avg.groupBy(*group_columns).agg(
        F.mean('promotionbenefit').alias('promotionbenefit'),
        F.mean('is_giftproductprice_null').alias('is_giftproductprice_null'),
        F.mean('is_promotionbenefit_null').alias('is_promotionbenefit_null')
    )

    # Chuyển giá trị > 0 của 'is_giftproductprice_null' và 'is_promotionbenefit_null' thành 1
    df_PromotionBenefit_avg = df_PromotionBenefit_avg.withColumn(
        'is_giftproductprice_null',
        F.when(F.col('is_giftproductprice_null') > 0, F.lit(1)).otherwise(F.col('is_giftproductprice_null'))
    ).withColumn(
        'is_promotionbenefit_null',
        F.when(F.col('is_promotionbenefit_null') > 0, F.lit(1)).otherwise(F.col('is_promotionbenefit_null'))
    )

#Chuyển fromdate, todate -> date_key
@F.udf(ArrayType(StringType()))
def date_ranges(start_date, end_date):
    # Chuyển đổi thành đối tượng datetime
    # print(start_date)
    # print(end_date)
    # start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    # end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    # Tạo danh sách ngày ở giữa
    current_date = start_date  # Bắt đầu từ ngày tiếp theo của ngày đầu
    dates_between = []

    # Lặp qua và tạo dãy ngày đến ngày cuối
    while current_date <= end_date:
        dates_between.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    # In ra kết quả
    # print(dates_between)
    return dates_between
df_PromotionBenefit_avg = df_PromotionBenefit_avg.withColumn('date_ranges', date_ranges(F.to_date(F.col('fromdate')), F.to_date(F.col('todate'))))\
                            .withColumn('date_key', F.explode(F.col('date_ranges')))\
                            .filter(F.col('promotiongifttype') != 5)\
                            .filter(F.col('promotionbenefit') <= 0.95)\
                            .filter(F.col('promotionbenefit').isNotNull())

print("df_PromotionBenefit_avg date_range executed successfully.")


# Loại bỏ các cột không cần thiết
df_PromotionBenefit_avg = df_PromotionBenefit_avg.drop("fromdate", "todate")

# Lọc discounttype == 3
df_PromotionBenefit_avg = df_PromotionBenefit_avg.withColumn(
    "discounttype_filter", 
    when(col("discounttype") == 3, True).otherwise(True)
)

# Tính toán cumulative_promotionbenefit
df_PromotionBenefit_avg = df_PromotionBenefit_avg.withColumn(
    "cumulative_promotionbenefit", col("promotionbenefit")
)
df_PromotionBenefit_avg = df_PromotionBenefit_avg.withColumn(
    "cumulative_promotionbenefit", expr("sum(promotionbenefit) over (partition by date_key, productidmain, itemid, storeid, exchangequantity order by date_key)")
)

# Tổng hợp dữ liệu
df_grouped = df_PromotionBenefit_avg.groupBy(
    "date_key", "productidmain", "itemid", "storeid", "exchangequantity", "minproductquantity"
).agg(
    spark_max("cumulative_promotionbenefit").alias("promotionbenefit"),
    spark_max("promotionbenefit").alias("maxpromotionbenefit"),
    spark_max("exchangequantity").alias("maxexchangequantity"),
    size(collect_list("itemid")).alias("countexchangequantity"),
    size(collect_list("productidmain")).alias("countpromoeachproduct"),
    spark_max("is_giftproductprice_null").alias("is_giftproductprice_null"),
    spark_max("is_promotionbenefit_null").alias("is_promotionbenefit_null")
)

# Lọc và tổng hợp promotionbenefit
df_sum_promotionbenefit = df_grouped.filter(
    ~(
        (col("countexchangequantity") > 1) &
        (col("exchangequantity") == col("maxexchangequantity")) &
        (col("promotionbenefit") != col("maxpromotionbenefit")) &
        ~(col("countpromoeachproduct") > 1)
    )
).groupBy("date_key", "itemid", "storeid").agg(
    avg("promotionbenefit").alias("promotionbenefit"),
    spark_max("promotionbenefit").alias("max_promotionbenefit"),
    spark_min("promotionbenefit").alias("min_promotionbenefit"),
    spark_max("is_giftproductprice_null").alias("is_giftproductprice_null"),
    spark_max("is_promotionbenefit_null").alias("is_promotionbenefit_null")
)

print("df_sum_promotionbenefit executed successfully.")
#Tính df_dc_promotionbenefit
df_sum_promotionbenefit.createOrReplaceTempView("df_sum_promotionbenefit")
df_dcStoreItem.createOrReplaceTempView("df_dcStoreItem")

query3 = f'''
SELECT
  dcid,
  tmp2.itemid, 
  df_sum_promotionbenefit.date_key, 
  countstore, 
  avg(df_sum_promotionbenefit.promotionbenefit) promotionbenefit,
  MAX(promotionbenefit) as max_promotionbenefit,
  MIN(promotionbenefit) as min_promotionbenefit,
  count(1) countpromoapplystore, 
  MAX(is_giftproductprice_null) AS is_giftproductprice_null,
  MAX(is_promotionbenefit_null) AS is_promotionbenefit_null
from df_sum_promotionbenefit
  join (
    Select 
      dcid, 
      storeid, 
      itemid, 
      count(1) over (PARTITION by dcid, itemid) countstore 
    from 
      df_dcStoreItem
  ) tmp2 
    on df_sum_promotionbenefit.storeid = tmp2.storeid 
    and df_sum_promotionbenefit.itemid = tmp2.itemid 
group by 
  dcid, 
  tmp2.itemid,
  df_sum_promotionbenefit.date_key, 
  countstore
;'''
# Thực thi câu truy vấn
df_dc_promotionbenefit = spark.sql(query3)
print("query3 executed successfully.")
# Xóa cột 'is_giftproductprice_null'
df_dc_promotionbenefit = df_dc_promotionbenefit.drop('is_giftproductprice_null')

# Thêm cột 'initial_promotionbenefit' bằng cách sao chép giá trị từ 'promotionbenefit'
df_dc_promotionbenefit = df_dc_promotionbenefit.withColumn(
    'initial_promotionbenefit', F.col('promotionbenefit')
)

# Cập nhật cột 'promotionbenefit' với điều kiện:
# Nếu 'initial_promotionbenefit' < 1 thì giữ nguyên, ngược lại gán giá trị là 1
df_dc_promotionbenefit = df_dc_promotionbenefit.withColumn(
    'promotionbenefit',
    F.when(F.col('initial_promotionbenefit') < 1, F.col('initial_promotionbenefit')).otherwise(F.lit(1))
)

#Tạo bảng df_store_promotionbenefit:
df_store_promotionbenefit = df_sum_promotionbenefit.select(
    'date_key', 'itemid', 'storeid', 'promotionbenefit', 'max_promotionbenefit', 'min_promotionbenefit', 'is_promotionbenefit_null'
    )

# Thêm cột 'initial_promotionbenefit' bằng cách sao chép giá trị từ cột 'promotionbenefit':
df_store_promotionbenefit = df_store_promotionbenefit.withColumn(
    'initial_promotionbenefit', F.col('promotionbenefit')
    )
# Cập nhật cột 'promotionbenefit' với điều kiện:
# Nếu 'initial_promotionbenefit < 1 thfi giữ nguyên, ngược lại thì gán bằng 1
df_store_promotionbenefit = df_store_promotionbenefit.withColumn(
    'promotionbenefit',
    F.when(F.col('initial_promotionbenefit') < 1, F.col('initial_promotionbenefit')).otherwise(F.lit(1))
)

#Tạo cột rundatekey:
df_dc_promotionbenefit = df_dc_promotionbenefit.withColumn("rundatekey", lit(today))
df_store_promotionbenefit = df_store_promotionbenefit.withColumn("rundatekey", lit(today))

df_dc_promotionbenefit = (
    df_dc_promotionbenefit
    .withColumn("date_key", df_dc_promotionbenefit["date_key"].cast("int"))
    .withColumn("rundatekey", df_dc_promotionbenefit["rundatekey"].cast("int"))
    .withColumn("itemid", df_dc_promotionbenefit["itemid"].cast("string"))    
    .withColumn("dcid", df_dc_promotionbenefit["dcid"].cast("int"))    
    .withColumn("countstore", df_dc_promotionbenefit["countstore"].cast("long"))
    .withColumn("promotionbenefit", df_dc_promotionbenefit["promotionbenefit"].cast("double"))
    .withColumn("initial_promotionbenefit", df_dc_promotionbenefit["initial_promotionbenefit"].cast("double"))
    .withColumn("max_promotionbenefit", df_dc_promotionbenefit["max_promotionbenefit"].cast("double"))
    .withColumn("min_promotionbenefit", df_dc_promotionbenefit["min_promotionbenefit"].cast("double"))
    .withColumn("countpromoapplystore", df_dc_promotionbenefit["countpromoapplystore"].cast("long"))
    .withColumn("is_promotionbenefit_null", df_dc_promotionbenefit["is_promotionbenefit_null"].cast("double"))
)

df_store_promotionbenefit = (
    df_store_promotionbenefit
    .withColumn("date_key", df_store_promotionbenefit["date_key"].cast("int"))
    .withColumn("rundatekey", df_store_promotionbenefit["rundatekey"].cast("int"))
    .withColumn("itemid", df_store_promotionbenefit["itemid"].cast("string"))
    .withColumn("storeid", df_store_promotionbenefit["storeid"].cast("int"))
    .withColumn("promotionbenefit", df_store_promotionbenefit["promotionbenefit"].cast("double"))
    .withColumn("initial_promotionbenefit", df_store_promotionbenefit["initial_promotionbenefit"].cast("double"))
    .withColumn("max_promotionbenefit", df_store_promotionbenefit["max_promotionbenefit"].cast("double"))
    .withColumn("min_promotionbenefit", df_store_promotionbenefit["min_promotionbenefit"].cast("double"))
    .withColumn("is_promotionbenefit_null", df_store_promotionbenefit["is_promotionbenefit_null"].cast("double"))
)



### OUTPUT ###
# Luu bang df_dc_promotionbenefit
begin = time.time()
df_dc_promotionbenefit.repartition(200).write.format("parquet").mode("overwrite").save(rf"hdfs://172.16.5.69:8020/forecast/check/promotion_check/df_dc_promotionbenefit_daily/{today}.parquet")
end = time.time()
timespent = end-begin
noti_log = f"Thoi gian chay df_dc_promotionbenefit: {timespent} seconds"
logging.info(noti_log)

# #Luu bang df_store_promotionbenefit
begin = time.time()
df_store_promotionbenefit.repartition(200).write.format("parquet").mode("overwrite").save(rf"hdfs://172.16.5.69:8020/forecast/check/promotion_check/df_store_promotionbenefit_daily/{today}.parquet")
end = time.time()
timespent = end-begin
noti_log = f"Thoi gian chay df_store_promotionbenefit: {timespent} seconds"
logging.info(noti_log)


print("Đã hoàn thành promotion_daily")