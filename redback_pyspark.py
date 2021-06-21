# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, isnan, when, trim, instr, split, isnan

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()


########################Changing the dates from UTC to New Zealand Daylight TimestampType


import glob
import pandas as pd
import csv
import numpy as np
from datetime import time
from datetime import datetime
from datetime import timedelta

from pyspark.sql.functions import col, udf

def utc_to_nzt(d):
    date_range_mapping = {
        '2016': ('2016-04-02T14:00:00.000Z','2016-09-24T14:00:00:000Z'),
        '2017': ('2017-04-01T14:00:00.000Z','2017-09-23T14:00:00:000Z'),
        '2018': ('2018-03-31T14:00:00.000Z','2018-09-29T14:00:00:000Z'),
        '2019': ('2019-04-06T14:00:00.000Z','2019-09-28T14:00:00:000Z'),
    }
    start_date, end_date = date_range_mapping[d[:4]]
    if d >= start_date:
        if d < end_date:
            tempd = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S.%fZ")
            dt = tempd + timedelta(hours = 12)
            newd = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return newd
        else:
            tempd = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S.%fZ")
            dt = tempd + timedelta(hours = 13)
            newd = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return newd
    else:
        tempd = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S.%fZ")
        dt = tempd + timedelta(hours = 13)
        newd = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return newd

utc_to_nzt_udf = udf(utc_to_nzt, StringType())

#Load the manually created table which contains ICP, InverterSN and Location collected from the Redback site
schema_icp_inveter = StructType([
    StructField("ICP", StringType(), True),
    StructField("Inverter_SN", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("GXP", StringType(), True),
])

icp_inverter = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_icp_inveter)
    .load("hdfs:///user/jpe116/output/ICP-Inverter.csv")
)
icp_inverter.cache()
icp_inverter.show(36, False)
icp_inverter.count()  ## 36

icp_inverter = icp_inverter.dropna(how='all')
icp_inverter.show(36)

+---------------+----------------+------------+-------+
|ICP            |Inverter_SN     |Location    |GXP    |
+---------------+----------------+------------+-------+
|0000155241TR0E1|RB17030701200105|Wellington  |CPK0331|
|0000176576TREFC|RB17041101200027|Wellington  |CPK0331|
|0006783759RNEF1|RB17041101200034|Christchurch|ISL0661|
|0000137630TR4E0|RB17041101200037|Wellington  |CPK0331|
|0000129747TR552|RB17041101200045|Wellington  |CPK0331|
|0001404973UN7F4|RB17041101200073|Wellington  |CPK0331|
|0000124262TRA6D|RB17041101200074|Wellington  |CPK0331|
|0000172950TR8AA|RB17041101200081|Wellington  |CPK0331|
|0000144357TRB86|RB17041101200087|Wellington  |CPK0331|
|0000132791TRE9E|RB17041101200090|Wellington  |CPK0331|
|0000133690TRA7F|RB17041101200069|Wellington  |CPK0331|
|0000153587TRC43|RB17041101200093|Wellington  |CPK0331|
|0000145147TRA8C|RB17041101200105|Wellington  |CPK0331|
|0000137070TR043|RB17041101200108|Wellington  |CPK0331|
|0000129915TRBD4|RB17041101200109|Wellington  |CPK0331|
|0000169665TRA13|RB17041101200123|Wellington  |CPK0331|
|0000133631TRE25|RB17041101200130|Wellington  |CPK0331|
|0000126737TR96F|RB17041101200132|Wellington  |CPK0331|
|0000130061TR2CB|RB17041101200144|Wellington  |CPK0331|
|0000132847TRD5C|RB17041101200160|Wellington  |CPK0331|
|0007160293RN6C9|RB17041101200161|Christchurch|ISL0661|
|0006783783RN122|RB17041101200163|Christchurch|ISL0661|
|0000137891TR6B1|RB17041101200164|Wellington  |CPK0331|
|0000139406TR050|RB17041101200179|Wellington  |CPK0331|
|0000171008TR95F|RB17041101200186|Wellington  |CPK0331|
|0006784488RN895|RB17041101200188|Christchurch|ISL0661|
|0007068166RN1FE|RB17041101200192|Christchurch|ISL0661|
|0007166729RN62F|RB17041101200199|Christchurch|ISL0661|
|0000124111TR0F3|RB17041101200201|Wellington  |CPK0331|
|0006783740RNA0D|RB17041101200210|Christchurch|ISL0661|
|0001437637UN898|RB17041101200212|Wellington  |CPK0331|
|0006783724RN4F7|RB17041101200230|Christchurch|ISL0661|
|0006783775RN0BA|RB17041101200237|Christchurch|ISL0661|
|0000131314TR87A|RB17052901200096|Wellington  |CPK0331|

# Load the redback csv
schema_redback_csv = StructType([
    StructField("id", StringType(), True),
    StructField("index", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("acLoadPower", DoubleType(), True),
    StructField("backupPower", DoubleType(), True),
    StructField("batteryAvailable", DoubleType(), True),
    StructField("batteryPower", DoubleType(), True),
    StructField("batterySoC", DoubleType(), True),
    StructField("cloudRecieveTimeStampUTC", StringType(), True),
    StructField("date", StringType(), True),
    StructField("friendlyName", StringType(), True),
    StructField("gridPower", DoubleType(), True),
    StructField("icp", StringType(), True),
    StructField("inserted", StringType(), True),    
    StructField("inverterMode", StringType(), True),
    StructField("inverterSN", StringType(), True),
    StructField("inverterSoftwareVersion", IntegerType(), True),
    StructField("localTimeStamp", StringType(), True),
    StructField("model", StringType(), True),    
    StructField("pvPower", DoubleType(), True),
    StructField("redbackSoftwareVersion", StringType(), True),
    StructField("type", StringType(), True),    
    StructField("batteryStatus", StringType(), True),
])
redback = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_redback_csv)
    .load("hdfs:///user/jpe116/hadooptest/redback.csv")
    # .load("hdfs:///data/epecentre/redback/**")

)
redback.cache()
redback.show(10, False)
redback.count()  #12,756,273

nodupli_redback = redback.dropDuplicates()
nodupli_redback.count() # 12,756,273

newredback = (
    nodupli_redback
    .filter(col("id").isNotNull())
 ) 
newredback.count() # 12,756,272
 
redback.select('icp').distinct().show() # 
         icp|
+---------------+
|0000137070TR043|
|0000171008TR95F|
|0000145147TRA8C|
|0000133631TRE25|
|0000137630TR4E0|
|0000155241TR0E1|
|0001404973UN7F4|
|0000153587TRC43|
|0000124111TR0F3|
|0006784488RN895|
|0000132791TRE9E|
|0000130061TR2CB|
|0000132995TR99F|
|0006783830RN05F|
|0000172950TR8AA|
|0006783759RNEF1|
|0007166729RN62F|
|           null|
|0000133690TRA7F|
|0000129915TRBD4|
|0006783740RNA0D|
|0006783724RN4F7|
|0007068166RN1FE|
|0007160293RN6C9|
|0006783775RN0BA|
|0000139406TR050|
|0000129747TR552|
|0001437637UN898|
|0006783783RN122|
|0000169665TRA13|
|0000132847TRD5C|
|0000176576TREFC|
|0000137891TR6B1|
|0000124262TRA6D|
|0000144357TRB86|
|0000131314TR87A|



####
#######RB17041101200132 none in redback
#######remove RB17041101200093 according to 2 icp

# convert utc time to nz timestamp

timechange_redback = (
    newredback
    .withColumn('DateTransformed', utc_to_nzt_udf(col('date'))) # convert timestamp from UTC to local New Zealand time stamp
)
timechange_redback.show(3,False)


+-------------------------------------+---------------+-----+-----------+-----------+----------------+------------+----------+------------------------+------------------------+---------------+---------+---------------+------------------------+------------+----------------+-----------------------+---------------------+------+-------+----------------------+----+-------------+---------------------------+
|id                                   |index          |score|acLoadPower|backupPower|batteryAvailable|batteryPower|batterySoC|cloudRecieveTimeStampUTC|date                    |friendlyName   |gridPower|icp            |inserted                |inverterMode|inverterSN      |inverterSoftwareVersion|localTimeStamp       |model |pvPower|redbackSoftwareVersion|type|batteryStatus|DateTransformed            |
+-------------------------------------+---------------+-----+-----------+-----------+----------------+------------+----------+------------------------+------------------------+---------------+---------+---------------+------------------------+------------+----------------+-----------------------+---------------------+------+-------+----------------------+----+-------------+---------------------------+
|RB1704110120009311/14/2017 4:33:13 AM|redback-2017-11|1.0  |239.0      |117.0      |0.0             |0.0         |19.0      |11/13/2017 3:33:17 PM   |2017-11-13T15:33:13.000Z|0000153587TRC43|-322.0   |0000153587TRC43|2017-11-13T15:34:13.971Z|Auto        |RB17041101200093|11111                  |11/14/2017 4:33:13 AM|SH4600|0.0    |ROSS v1, 5, 6, 3      |data|null         |2017-11-14T04:33:13.000000Z|
|RB1704110120021211/14/2017 4:33:31 AM|redback-2017-11|1.0  |109.0      |73.0       |0.0             |0.0         |18.0      |11/13/2017 3:33:55 PM   |2017-11-13T15:33:31.000Z|0001437637UN898|-151.0   |0001437637UN898|2017-11-13T15:34:14.325Z|Auto        |RB17041101200212|11111                  |11/14/2017 4:33:31 AM|SH4600|0.0    |ROSS v1, 5, 6, 3      |data|null         |2017-11-14T04:33:31.000000Z|
|RB1704110120013211/14/2017 4:34:10 AM|redback-2017-11|1.0  |63.0       |326.0      |0.291           |271.78479   |26.0      |11/13/2017 3:34:10 PM   |2017-11-13T15:34:10.000Z|null           |0.0      |null           |2017-11-13T15:34:14.485Z|Auto        |RB17041101200132|11111                  |11/14/2017 4:34:10 AM|SH4600|0.0    |ROSS v1, 5, 6, 3      |data|null         |2017-11-14T04:34:10.000000Z|
+-------------------------------------+---------------+-----+-----------+-----------+----------------+------------+----------+------------------------+------------------------+---------------+---------+---------------+------------------------+------------+----------------+-----------------------+---------------------+------+-------+----------------------+----+-------------+---------------------------+

################################### for final redback
final_redback = (
     timechange_redback
    .filter(col('inverterSN') != 'RB17052901200095') # not in invertericp
    .filter(col('inverterSN') != 'RB17041101200103') # not in invertericp
    .filter(col('inverterSN') != 'RB17041101200060') # not in invertericp  
)
final_redback.show(5,False)
+------------------------------------+---------------+-----+-----------+-----------+------------------+------------+----------+------------------------+------------------------+---------------+---------+---------------+------------------------+------------+----------------+-----------------------+--------------------+------+------------------+----------------------+----+-------------+---------------------------+
|id                                  |index          |score|acLoadPower|backupPower|batteryAvailable  |batteryPower|batterySoC|cloudRecieveTimeStampUTC|date                    |friendlyName   |gridPower|icp            |inserted                |inverterMode|inverterSN      |inverterSoftwareVersion|localTimeStamp      |model |pvPower           |redbackSoftwareVersion|type|batteryStatus|DateTransformed            |
+------------------------------------+---------------+-----+-----------+-----------+------------------+------------+----------+------------------------+------------------------+---------------+---------+---------------+------------------------+------------+----------------+-----------------------+--------------------+------+------------------+----------------------+----+-------------+---------------------------+
|RB170307012001051/2/2019 10:35:57 AM|redback-2019-01|1.0  |314.0      |0.0        |0.8640000000000001|-906.66937  |28.0      |1/1/2019 10:00:01 PM    |2019-01-01T21:35:57.000Z|0000155241TR0E1|0.0      |0000155241TR0E1|2019-01-01T22:00:05.066Z|Auto        |RB17030701200105|11111                  |1/2/2019 10:35:57 AM|SH4600|1216.4400634765625|ROSS v1, 5, 16, 0     |data|null         |2019-01-02T10:35:57.000000Z|
|RB170411012000271/1/2019 11:00:50 PM|redback-2019-01|1.0  |104.0      |332.0      |0.816             |236.52      |64.0      |1/1/2019 10:01:00 AM    |2019-01-01T10:00:50.000Z|0000176576TREFC|0.0      |0000176576TREFC|2019-01-01T10:02:05.233Z|Auto        |RB17041101200027|11111                  |1/1/2019 11:00:50 PM|SH4600|0.0               |ROSS v1, 5, 16, 0     |data|null         |2019-01-01T23:00:50.000000Z|
|RB170411012000871/1/2019 11:01:11 PM|redback-2019-01|1.0  |2.0        |438.0      |1.008             |275.21201   |41.0      |1/1/2019 10:01:19 AM    |2019-01-01T10:01:11.000Z|0000144357TRB86|0.0      |0000144357TRB86|2019-01-01T10:02:05.845Z|Auto        |RB17041101200087|11111                  |1/1/2019 11:01:11 PM|SH4600|0.0               |ROSS v1, 5, 16, 0     |data|null         |2019-01-01T23:01:11.000000Z|
|RB170411012002301/1/2019 11:01:17 PM|redback-2019-01|1.0  |3.0        |312.0      |6.816             |219.40601   |91.0      |1/1/2019 10:01:23 AM    |2019-01-01T10:01:17.000Z|0006783724RN4F7|0.0      |0006783724RN4F7|2019-01-01T10:02:05.763Z|Auto        |RB17041101200230|11111                  |1/1/2019 11:01:17 PM|SH4600|0.0               |ROSS v1, 5, 16, 0     |data|null         |2019-01-01T23:01:17.000000Z|
|RB170411012000871/2/2019 11:01:21 AM|redback-2019-01|1.0  |102.0      |444.0      |2.208             |-1401.42578 |66.0      |1/1/2019 10:01:28 PM    |2019-01-01T22:01:21.000Z|0000144357TRB86|0.0      |0000144357TRB86|2019-01-01T22:02:05.164Z|Auto        |RB17041101200087|11111                  |1/2/2019 11:01:21 AM|SH4600|2769.2000122070312|ROSS v1, 5, 16, 0     |data|null         |2019-01-02T11:01:21.000000Z|
+------------------------------------+---------------+-----+-----------+-----------+------------------+------------+----------+------------------------+------------------------+---------------+---------+---------------+------------------------+------------+----------------+-----------------------+--------------------+------+------------------+----------------------+----+-------------+---------------------------+

final_redback.count() # 12,189,341

final_redback.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/finaldata/final_redback')

hdfs dfs -ls /user/jpe116/output/finaldata

##################################



#Join the two tables to fill the empty ICP in redback CSV file
new_redback = (
    #Renaming the necessary variables in the CSV and select only the columns needed
    timechange_redback
    # Select only the columns that are needed
    .select(['icp_rb','inverterSN', 'pvPower', 'Date','acLoadPower','backupPower','batteryAvailable',
    'batteryPower','batterySoC','gridPower','inverterMode','batteryStatus','DateTransformed'])    
)

dropdupli_new_redback = (
      new_redback
      .dropDuplicates()
)
dropdupli_new_redback.count() # 12,756,250


newredback.select('inverterSN').distinct().show(36,False)

|inverterSN      |
+----------------+
|RB17041101200179|
|RB17041101200199|
|RB17041101200188|
|RB17041101200045|
|RB17052901200095|
|RB17041101200105|
|RB17041101200164|
|RB17041101200034|
|RB17041101200103|
|RB17041101200144|
|RB17041101200069|
|RB17041101200037|
|RB17041101200237|
|RB17041101200163|
|RB17041101200090|
|RB17041101200108|
|RB17041101200060|
|RB17041101200160|
|RB17041101200073|
|RB17041101200093|
|RB17041101200130|
|RB17041101200201|
|RB17041101200132|
|RB17041101200087|
|RB17041101200210|
|RB17041101200192|
|RB17030701200105|
|RB17041101200027|
|RB17041101200230|
|RB17041101200161|
|RB17052901200096|
|RB17041101200074|
|RB17041101200186|
|RB17041101200109|
|RB17041101200123|
|RB17041101200212|
+----------------+


redback_icpinlist = (
     dropdupli_new_redback
    .filter(col('inverterSN') != 'RB17052901200095') # not in invertericp
    .filter(col('inverterSN') != 'RB17041101200103') # not in invertericp
    .filter(col('inverterSN') != 'RB17041101200060') # not in invertericp  
)
redback_icpinlist.count() # 12189320



joinredback = (
     redback_icpinlist
    .join(
        icp_inverter
        .withColumnRenamed('ICP','ICP1')
        .select(['ICP1', 'Inverter_SN']),
        redback_icpinlist.inverterSN == icp_inverter.Inverter_SN,
        how='left'
    )
)
joinredback.show(5, False)

+----+----------------+-------+------------------------+-----------+-----------+----------------+------------+----------+---------+------------+-------------+---------------------------+---------------+----------------+
|icp |inverterSN      |pvPower|Date                    |acLoadPower|backupPower|batteryAvailable|batteryPower|batterySoC|gridPower|inverterMode|batteryStatus|DateTransformed            |ICP1            |Inverter_SN     |
+----+----------------+-------+------------------------+-----------+-----------+----------------+------------+----------+---------+------------+-------------+---------------------------+---------------+----------------+
|null|RB17041101200027|0.0    |2017-09-10T07:37:58.000Z|4171.0     |3746.0     |0.0             |null        |null      |-7917.0  |Auto        |null         |2017-09-10T19:37:58.000000Z|0000176576TREFC|RB17041101200027|
|null|RB17041101200027|0.0    |2017-09-10T07:35:56.000Z|4037.0     |3580.0     |0.0             |null        |null      |-7617.0  |Auto        |null         |2017-09-10T19:35:56.000000Z|0000176576TREFC|RB17041101200027|
|null|RB17041101200027|0.0    |2017-09-10T07:32:54.000Z|3753.0     |2969.0     |0.0             |null        |null      |-6722.0  |Auto        |null         |2017-09-10T19:32:54.000000Z|0000176576TREFC|RB17041101200027|
|null|RB17041101200027|0.0    |2017-09-10T07:48:06.000Z|2504.0     |3970.0     |0.0             |null        |null      |-6474.0  |Auto        |null         |2017-09-10T19:48:06.000000Z|0000176576TREFC|RB17041101200027|
|null|RB17041101200027|0.0    |2017-09-10T07:56:12.000Z|4251.0     |1446.0     |0.0             |null        |null      |-5697.0  |Auto        |null         |2017-09-10T19:56:12.000000Z|0000176576TREFC|RB17041101200027|
+----+----------------+-------+------------------------+-----------+-----------+----------------+------------+----------+---------+------------+-------------+---------------------------+---------------+----------------+


joinredback.count()   # 12189320

joinredback = joinredback.drop('icp')
joinredback.show(5, False)

+----------------+-------+------------------------+-----------+-----------+----------------+------------+----------+---------+------------+-------------+---------------------------+---------------+----------------+
|inverterSN      |pvPower|Date                    |acLoadPower|backupPower|batteryAvailable|batteryPower|batterySoC|gridPower|inverterMode|batteryStatus|DateTransformed            |ICP1           |Inverter_SN     |
+----------------+-------+------------------------+-----------+-----------+----------------+------------+----------+---------+------------+-------------+---------------------------+---------------+----------------+
|RB17041101200027|0.0    |2017-09-10T07:37:58.000Z|4171.0     |3746.0     |0.0             |null        |null      |-7917.0  |Auto        |null         |2017-09-10T19:37:58.000000Z|0000176576TREFC|RB17041101200027|
|RB17041101200027|0.0    |2017-09-10T07:35:56.000Z|4037.0     |3580.0     |0.0             |null        |null      |-7617.0  |Auto        |null         |2017-09-10T19:35:56.000000Z|0000176576TREFC|RB17041101200027|
|RB17041101200027|0.0    |2017-09-10T07:32:54.000Z|3753.0     |2969.0     |0.0             |null        |null      |-6722.0  |Auto        |null         |2017-09-10T19:32:54.000000Z|0000176576TREFC|RB17041101200027|
|RB17041101200027|0.0    |2017-09-10T07:48:06.000Z|2504.0     |3970.0     |0.0             |null        |null      |-6474.0  |Auto        |null         |2017-09-10T19:48:06.000000Z|0000176576TREFC|RB17041101200027|
|RB17041101200027|0.0    |2017-09-10T07:56:12.000Z|4251.0     |1446.0     |0.0             |null        |null      |-5697.0  |Auto        |null         |2017-09-10T19:56:12.000000Z|0000176576TREFC|RB17041101200027|
+----------------+-------+------------------------+-----------+-----------+----------------+------------+----------+---------+------------+-------------+---------------------------+---------------+----------------+

joinredback.select('ICP1').distinct().show(36,False)
|ICP1           |
+---------------+
|0000145147TRA8C|
|0000137070TR043|
|0000133631TRE25|
|0000124111TR0F3|
|0000137630TR4E0|
|0000153587TRC43|
|0000155241TR0E1|
|0000171008TR95F|
|0001404973UN7F4|
|0007166729RN62F|
|0006783759RNEF1|
|0000130061TR2CB|
|0000132791TRE9E|
|0000172950TR8AA|
|0006784488RN895|
|0000129747TR552|
|0000129915TRBD4|
|0007068166RN1FE|
|0007160293RN6C9|
|0000133690TRA7F|
|0000139406TR050|
|0001437637UN898|
|0006783724RN4F7|
|0006783740RNA0D|
|0006783775RN0BA|
|0000176576TREFC|
|0000126737TR96F|
|0006783783RN122|
|0000124262TRA6D|
|0000131314TR87A|
|0000132847TRD5C|
|0000137891TR6B1|
|0000144357TRB86|
|0000169665TRA13|
+---------------+


dropdupli_new_redback.select('icp').distinct().show(40,False)
|icp            |
+---------------+
|0000124111TR0F3|
|0000133631TRE25|
|0000137070TR043|
|0000137630TR4E0|
|0000145147TRA8C|
|0000153587TRC43|
|0000155241TR0E1|
|0000171008TR95F|
|0001404973UN7F4|
|0000130061TR2CB|
|0000132791TRE9E|
|0000172950TR8AA|
|0006783759RNEF1|
|0006783830RN05F|
|0006784488RN895|
|0007166729RN62F|
|0000132995TR99F|
|null           |
|0000129747TR552|
|0000129915TRBD4|
|0000133690TRA7F|
|0000139406TR050|
|0006783724RN4F7|
|0006783740RNA0D|
|0006783775RN0BA|
|0007068166RN1FE|
|0007160293RN6C9|
|0001437637UN898|
|0000124262TRA6D|
|0000131314TR87A|
|0000132847TRD5C|
|0000137891TR6B1|
|0000144357TRB86|
|0000169665TRA13|
|0000176576TREFC|
|0006783783RN122|
+---------------+


redback_icpinlist.select('icp').distinct().show(36,False)

|icp            |
+---------------+
|0000124111TR0F3|
|0000133631TRE25|
|0000137070TR043|
|0000137630TR4E0|
|0000145147TRA8C|
|0000153587TRC43|
|0000155241TR0E1|
|0000171008TR95F|
|0001404973UN7F4|
|0000130061TR2CB|
|0000132791TRE9E|
|0000172950TR8AA|
|0006783759RNEF1|
|0006784488RN895|
|0007166729RN62F|
|0000132995TR99F|
|null           |
|0000129747TR552|
|0000129915TRBD4|
|0000133690TRA7F|
|0000139406TR050|
|0006783724RN4F7|
|0006783740RNA0D|
|0006783775RN0BA|
|0007068166RN1FE|
|0007160293RN6C9|
|0001437637UN898|
|0000124262TRA6D|
|0000131314TR87A|
|0000132847TRD5C|
|0000137891TR6B1|
|0000144357TRB86|
|0000169665TRA13|
|0000176576TREFC|
|0006783783RN122|
+---------------+



joinredback.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/final3redback')

hdfs dfs -ls /user/jpe116/output

hdfs dfs -copyToLocal /user/jpe116/output/final3redback ~/projectoutput/final3redback


############################## Load the Embrium csv
schema_embrium_csv = StructType([
    StructField("ID", StringType(), True),
    StructField("Index", StringType(), True),
    StructField("Score", DoubleType(), True),
    StructField("Channel", StringType(), True),
    StructField("Connector", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Delta", StringType(), True),
    StructField("Device", StringType(), True),
    StructField("ICP", StringType(), True),
    StructField("InsertedDate", TimestampType(), True),
    StructField("KinesisSeq", StringType(), True),
    StructField("Property", StringType(), True),
    StructField("PropertyName", StringType(), True),
    StructField("SiteName", StringType(), True),    
    StructField("StreamName", StringType(), True),
    StructField("Synthetic", StringType(), True),
    StructField("Unit", StringType(), True),
    StructField("UnitType", StringType(), True),
    StructField("Value", DoubleType(), True),    
    StructField("Type", StringType(), True),
    StructField("Latest_id", StringType(), True),
    StructField("t", DoubleType(), True),    
    StructField("tsr_actual", TimestampType(), True),
    StructField("tsr_end", TimestampType(), True),
    StructField("tsrEnd", TimestampType(), True),
])
embrium_csv = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(schema_embrium_csv)
    #.load("hdfs:///data/epecentre/embrium/**")
    #.load("hdfs:///data/epecentre/embrium/embrium2017/**")
    #.load("hdfs:///data/epecentre/embrium/embrium2018/**")
    #.load("hdfs:///data/epecentre/embrium/embrium2019/**")
    .load("hdfs:///user/jpe116/project/embrium.csv")
)
embrium_csv.cache()
embrium_csv.show(10, False)
+--------------------------------------------------------------------------+---------------+-----+------------------------+------------------------+------------------------+-----+------------------------+---------------+-----------------------+--------------------------------------------------------+--------------------+--------------------+---------------+--------------+---------+----+---------+-------+--------+---------+----+----------+-------+------+
|ID                                                                        |Index          |Score|Channel                 |Connector               |Date                    |Delta|Device                  |ICP            |InsertedDate           |KinesisSeq                                              |Property            |PropertyName        |SiteName       |StreamName    |Synthetic|Unit|UnitType |Value  |Type    |Latest_id|t   |tsr_actual|tsr_end|tsrEnd|
+--------------------------------------------------------------------------+---------------+-----+------------------------+------------------------+------------------------+-----+------------------------+---------------+-----------------------+--------------------------------------------------------+--------------------+--------------------+---------------+--------------+---------+----+---------+-------+--------+---------+----+----------+-------+------+
|59dd19c07209173f0f91ff28:energy_active_export:market_export:1512579000000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T16:50:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:30.69 |49578518380789469614958149816951530072054744384064716802|energy_active_export|Energy Active Export|0000132847TRD5C|market_export |null     |kWh |quantity |635.443|readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_export:market_export:1512579600000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:00:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:30.69 |49578518380789469614958149816951530072054744384064716802|energy_active_export|Energy Active Export|0000132847TRD5C|market_export |null     |kWh |quantity |635.444|readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_export:market_export:1512580500000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:15:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:30.69 |49578518380789469614958149816951530072054744384064716802|energy_active_export|Energy Active Export|0000132847TRD5C|market_export |null     |kWh |quantity |635.444|readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512578400000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T16:40:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import |null     |kWh |quantity |1749.26|readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512579000000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T16:50:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import |null     |kWh |quantity |1749.4 |readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512580800000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:20:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import |null     |kWh |quantity |1749.82|readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512581100000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:25:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import |null     |kWh |quantity |1749.89|readings|null     |null|null      |null   |null  |
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512581400000 |embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:30:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import |null     |kWh |quantity |1749.96|readings|null     |null|null      |null   |null  |
|5a0ca5e094ba750156a47e74:energy_active_export:battery_export:1512583200000|embrium-2017-12|1.0  |5a0ca5e094ba750156a47e74|5a0ca4b7285a000c286657cd|2017-12-06T18:00:00.000Z|null |5a0ca5e094ba750156a47e73|0006783775RN0BA|2017-12-07 07:01:32.241|49578518380789469614958149817390370144574854843202535426|energy_active_export|Energy Active Export|0006783775RN0BA|battery_export|null     |kWh |quantity |386.218|readings|null     |null|null      |null   |null  |
|589a3875b624487b77dfc34e:power_active:market_active:1512583200000         |embrium-2017-12|1.0  |589a3875b624487b77dfc34e|589a313eb624487b77dfc340|2017-12-06T18:00:00.000Z|null |589a3847b624487b77dfc34d|0000150476TRBF5|2017-12-07 07:01:33.274|49578518380789469614958149820422356100168345500560392194|power_active        |Power Active        |0000150476TRBF5|market_active |null     |kW  |magnitude|0.2475 |readings|null     |null|null      |null   |null  |
+--------------------------------------------------------------------------+---------------+-----+------------------------+------------------------+------------------------+-----+------------------------+---------------+-----------------------+--------------------------------------------------------+--------------------+--------------------+---------------+--------------+---------+----+---------+-------+--------+---------+----+----------+-------+------+
only showing top 10 rows

embrium_csv.count()  # Embrium folder: 56,809,495
# Embrium.csv: 56,809,495

timechange_embrium = (
     embrium_csv
    .withColumn("DateTransformed", utc_to_nzt_udf(col("Date")))
)

timechange_embrium.show(5,False)
+-------------------------------------------------------------------------+---------------+-----+------------------------+------------------------+------------------------+-----+------------------------+---------------+-----------------------+--------------------------------------------------------+--------------------+--------------------+---------------+-------------+---------+----+--------+-------+--------+---------+----+----------+-------+------+---------------------------+
|ID                                                                       |Index          |Score|Channel                 |Connector               |Date                    |Delta|Device                  |ICP            |InsertedDate           |KinesisSeq                                              |Property            |PropertyName        |SiteName       |StreamName   |Synthetic|Unit|UnitType|Value  |Type    |Latest_id|t   |tsr_actual|tsr_end|tsrEnd|DateTransformed            |
+-------------------------------------------------------------------------+---------------+-----+------------------------+------------------------+------------------------+-----+------------------------+---------------+-----------------------+--------------------------------------------------------+--------------------+--------------------+---------------+-------------+---------+----+--------+-------+--------+---------+----+----------+-------+------+---------------------------+
|59dd19c07209173f0f91ff28:energy_active_export:market_export:1512579000000|embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T16:50:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:30.69 |49578518380789469614958149816951530072054744384064716802|energy_active_export|Energy Active Export|0000132847TRD5C|market_export|null     |kWh |quantity|635.443|readings|null     |null|null      |null   |null  |2017-12-07T05:50:00.000000Z|
|59dd19c07209173f0f91ff28:energy_active_export:market_export:1512579600000|embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:00:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:30.69 |49578518380789469614958149816951530072054744384064716802|energy_active_export|Energy Active Export|0000132847TRD5C|market_export|null     |kWh |quantity|635.444|readings|null     |null|null      |null   |null  |2017-12-07T06:00:00.000000Z|
|59dd19c07209173f0f91ff28:energy_active_export:market_export:1512580500000|embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T17:15:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:30.69 |49578518380789469614958149816951530072054744384064716802|energy_active_export|Energy Active Export|0000132847TRD5C|market_export|null     |kWh |quantity|635.444|readings|null     |null|null      |null   |null  |2017-12-07T06:15:00.000000Z|
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512578400000|embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T16:40:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import|null     |kWh |quantity|1749.26|readings|null     |null|null      |null   |null  |2017-12-07T05:40:00.000000Z|
|59dd19c07209173f0f91ff28:energy_active_import:market_import:1512579000000|embrium-2017-12|1.0  |59dd19c07209173f0f91ff28|59dd18dfeae7f93a2b84d11a|2017-12-06T16:50:00.000Z|null |59dd19c07209173f0f91ff27|0000132847TRD5C|2017-12-07 07:01:31.724|49578518380789469614958149817298491782284143025924866050|energy_active_import|Energy Active Import|0000132847TRD5C|market_import|null     |kWh |quantity|1749.4 |readings|null     |null|null      |null   |null  |2017-12-07T05:50:00.000000Z|
+-------------------------------------------------------------------------+---------------+-----+------------------------+------------------------+------------------------+-----+------------------------+---------------+-----------------------+--------------------------------------------------------+--------------------+--------------------+---------------+-------------+---------+----+--------+-------+--------+---------+----+----------+-------+------+---------------------------+
only showing top 5 rows

final_embrium = (
    timechange_embrium
    .filter(col('ICP') != 'powerco')
    .filter(col('ICP') != 'Insert ICP here')
)

check_embrium = (
    final_embrium
    .filter(col('Date') != '2018-01-10T00:15:00Z')    # this date format is not correct.
    .filter(col('Date') != '2018-05-02T02:45:00')     # this date format is not correct.
)

check_embrium.show(5, False)
check_embrium.count() # 55100548

#final_embrium.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/finaldata/final_embrium')
check_embrium.write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/finaldata/final_embrium')

hdfs dfs -ls /user/jpe116/output/finaldata

hdfs dfs -copyToLocal /user/jpe116/output/finaldata ~/finaldata
########################################################### final embrium


#########################################################


dupli_embr_csv = check_embrium.dropDuplicates()
dupli_embr_csv.count()  ## 56,809,495       No duplicates

#Select only the columns that are needed
new_embrium = (
    check_embrium
    # Select only the columns that are needed
    .select(['ICP', 'Delta', 'Date','Property','PropertyName', 'Value', 'Unit', 'UnitType','Type','DateTransformed'])
)
new_embrium.show(3, False)

+---------------+-----+------------------------+--------------------+--------------------+-------+----+--------+--------+---------------------------+
|ICP            |Delta|Date                    |Property            |PropertyName        |Value  |Unit|UnitType|Type    |DateTransformed            |
+---------------+-----+------------------------+--------------------+--------------------+-------+----+--------+--------+---------------------------+
|0000132847TRD5C|null |2017-12-06T16:50:00.000Z|energy_active_export|Energy Active Export|635.443|kWh |quantity|readings|2017-12-07T05:50:00.000000Z|
|0000132847TRD5C|null |2017-12-06T17:00:00.000Z|energy_active_export|Energy Active Export|635.444|kWh |quantity|readings|2017-12-07T06:00:00.000000Z|
|0000132847TRD5C|null |2017-12-06T17:15:00.000Z|energy_active_export|Energy Active Export|635.444|kWh |quantity|readings|2017-12-07T06:15:00.000000Z|
+---------------+-----+------------------------+--------------------+--------------------+-------+----+--------+--------+---------------------------+
only showing top 3 rows



new_embrium.count()   #55100548
##Embrium.csv : 56,809,495

new_embrium.select('icp').distinct().count() #35
new_embrium.select('icp').distinct().show(35, False)

+---------------+
|icp            |
+---------------+
|0000153587TRC43|
|1001154207CK638|
|0000171008TR95F|
|0000150476TRBF5|
|0000137070TR043|
|0000133631TRE25|
|0000130061TR2CB|
|1001154016CKAD7|
|0000132791TRE9E|
|0000141629TRF65|
|0006783759RNEF1|
|0006784488RN895|
|0000172950TR8AA|
|0006783830RN05F|
|orion_feeders  |
|0007166729RN62F|
|0007068166RN1FE|
|0007160293RN6C9|
|0000129915TRBD4|
|0006783775RN0BA|
|0006783740RNA0D|
|0000139406TR050|
|0001437637UN898|
|0006783724RN4F7|
|0000154707CK9CB|
|0000129747TR552|
|0000133690TRA7F|
|powerco        |
|0006783783RN122|
|0000132847TRD5C|
|0000158016TR504|
|0000126737TR96F|
|0000133598TRB68|
|0000169665TRA13|
|0000176576TREFC|
|Insert ICP here|
+---------------+


dupli_new_embr = new_embrium.dropDuplicates()
dupli_new_embr.count()  ##  51,517,107       3,665,872 Duplicates found when few columns are dropped

        
new_embrium.where(new_embrium['Property'].isNull()).count()  ## 5,087,388
new_embrium.where(new_embrium['PropertyName'].isNull()).count()  ##  5,182,022



join_prop = dupli_new_embr.withColumn('Property_Name', (
                    when(col("Property").isNotNull() & col("PropertyName").isNull(), col("Property"))
                   .when(col("Property").isNull() & col("PropertyName").isNotNull(), col("PropertyName"))
                   .when(col("Property").isNotNull() & col("PropertyName").isNotNull(), col("Property"))
                )
)



join_prop.where(join_prop['Property_Name'].isNotNull()).count()  

### Dropped na in the newly joined property column
dropna_emb = join_prop.dropna(subset=('Property_Name'))
dropna_emb.show(5,False)
dropna_emb.count()   ##  47,579,683   

      
import pyspark.sql.functions as f

gxp_embrium = (
    #Renaming the necessary variables in the CSV and select only the columns needed
    dropna_emb
    .select(['ICP','Delta', 'Date','Property_Name', 'Value', 'Unit', 'UnitType','DateTransformed','Type'])
    .filter(f.col('Type') == 'readings')
    .join(
        icp_inverter
        .select(['ICP', 'Location']),
        on='ICP',
        how='left'
    )
)
gxp_embrium.show(3, False)
+---------------+-----+------------------------+-------------+------+----+--------+---------------------------+--------+--------+
|ICP            |Delta|Date                    |Property_Name|Value |Unit|UnitType|DateTransformed            |Type    |Location|
+---------------+-----+------------------------+-------------+------+----+--------+---------------------------+--------+--------+
|0000133598TRB68|null |2017-07-04T21:35:00.056Z|power_factor |-0.998|null|null    |2017-07-05T09:35:00.056000Z|readings|null    |
|0000133598TRB68|null |2017-07-06T21:05:00.144Z|power_factor |-0.998|null|null    |2017-07-07T09:05:00.144000Z|readings|null    |
|0000133598TRB68|null |2017-07-08T20:55:00.132Z|power_factor |-0.998|null|null    |2017-07-09T08:55:00.132000Z|readings|null    |
|0000133598TRB68|null |2017-07-09T08:45:00.045Z|power_factor |-0.998|null|null    |2017-07-09T20:45:00.045000Z|readings|null    |
|0000133598TRB68|null |2017-07-11T22:15:00.034Z|power_factor |-0.998|null|null    |2017-07-12T10:15:00.034000Z|readings|null    |
|0000133598TRB68|null |2017-07-12T22:00:00.645Z|power_factor |-0.998|null|null    |2017-07-13T10:00:00.645000Z|readings|null    |
|0000133598TRB68|null |2017-07-08T05:55:00.137Z|power_factor |-0.997|null|null    |2017-07-08T17:55:00.137000Z|readings|null    |
|0000133598TRB68|null |2017-07-08T06:00:00.095Z|power_factor |-0.997|null|null    |2017-07-08T18:00:00.095000Z|readings|null    |
|0000133598TRB68|null |2017-07-10T22:20:00.082Z|power_factor |-0.997|null|null    |2017-07-11T10:20:00.082000Z|readings|null    |
|0000133598TRB68|null |2017-07-11T07:30:00.069Z|power_factor |-0.997|null|null    |2017-07-11T19:30:00.069000Z|readings|null    |
+---------------+-----+------------------------+-------------+------+----+--------+---------------------------+--------+--------+

gxp_embrium.count()  ##  47,579,683


##### Dropping na in Location to get rid of other ICPS outside 35 homes. But not using now because filter by location is enough
Location_not_null_embrium = gxp_embrium.dropna(subset=('Location'))
Location_not_null_embrium.count()  
Location_not_null_embrium.show(35, False)

gxp_embrium.select('Property_Name').distinct().show()

gxp_embrium.select('Unit').distinct().show()
+-------+
|   Unit|
+-------+
|     Hz|
| Inputs|
|     PF|
|      V|
|   null|
|Minutes|
|      A|
|    DRM|
|    kWh|
|     kW|
| Output|
|   kvar|
|      %|
+-------+

##########################################################
# temp = final_emb.limit(10)
# temp.cache()
# temp.count()



# get the Dredmode value in Christchurch

NewDredmode_ChCh = (
     gxp_embrium
    .filter(col("Location")=='Christchurch')
    .filter((col("Property_Name")=='dred_output') | (col("Property_Name")=='DRED mode'))
    .orderBy(col("Date"), ascending = False)
)
NewDredmode_ChCh.show(25, False)
+---------------+-----+------------------------+-------------+-----+----+---------+---------------------------+--------+------------+
|ICP            |Delta|Date                    |Property_Name|Value|Unit|UnitType |DateTransformed            |Type    |Location    |
+---------------+-----+------------------------+-------------+-----+----+---------+---------------------------+--------+------------+
|0006783724RN4F7|False|2019-04-08T02:35:00.000Z|dred_output  |0.0  |DRM |magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0007160293RN6C9|False|2019-04-08T02:35:00.000Z|dred_output  |0.0  |DRM |magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0007166729RN62F|False|2019-04-08T02:35:00.000Z|dred_output  |0.0  |DRM |magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006783775RN0BA|False|2019-04-08T02:35:00.000Z|dred_output  |0.0  |DRM |magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006783740RNA0D|False|2019-04-08T02:35:00.000Z|dred_output  |0.0  |DRM |magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006784488RN895|False|2019-04-08T02:35:00.000Z|dred_output  |0.0  |DRM |magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|



NewDredmode_ChCh.count()   ## 967,965

NewDredmode_ChCh.write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/finaldata/Dredmode_Chch.csv')

# save as one single csv
NewDredmode_ChCh.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/finaldata/NewDredmode_ChCh')

hdfs dfs -ls /user/jpe116/output

hdfs dfs -copyToLocal /user/jpe116/output/NewDredmode_ChCh ~/projectoutput/NewDredmode_ChCh



# get the input value in Christchurch
Input_ChCh = (
     gxp_embrium
    .filter(col("Location")=='Christchurch')
    .filter((col("Property_Name")=='Inputs') | (col("Property_Name")=='digital_register'))
    .orderBy(col("Date"), ascending = False)
)
Input_ChCh.show(25, False)
Input_ChCh.count()   ## 968,989
+---------------+-----+------------------------+----------------+-----+------+---------+---------------------------+--------+------------+
|ICP            |Delta|Date                    |Property_Name   |Value|Unit  |UnitType |DateTransformed            |Type    |Location    |
+---------------+-----+------------------------+----------------+-----+------+---------+---------------------------+--------+------------+
|0006783724RN4F7|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006783775RN0BA|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0007166729RN62F|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0007068166RN1FE|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006783783RN122|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006783740RNA0D|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0007160293RN6C9|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006784488RN895|False|2019-04-08T02:35:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:35:00.000000Z|readings|Christchurch|
|0006783775RN0BA|False|2019-04-08T02:30:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:30:00.000000Z|readings|Christchurch|
|0006783740RNA0D|False|2019-04-08T02:30:00.000Z|digital_register|2.0  |Inputs|magnitude|2019-04-08T14:30:00.000000Z|readings|Christchurch|



Input_ChCh.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/finaldata/NewInput_ChCh')

hdfs dfs -ls /user/jpe116/output

hdfs dfs -copyToLocal /user/jpe116/output/NewInput_ChCh ~/projectoutput/NewInput_ChCh

# get the Dredmode value in Wellington

Dredmode_Wellington = (
     gxp_embrium
    .filter(col("Location")=='Wellington')
    .filter((col("Property_Name")=='dred_output') | (col("Property_Name")=='DRED mode'))
    .orderBy(col("Date"), ascending = False)
)
Dredmode_Wellington.show(25, False)
Dredmode_Wellington.count()   


Dredmode_Wellington.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/Dredmode_Wellington.csv')

hdfs dfs -ls /user/jpe116/output

hdfs dfs -copyToLocal /user/jpe116/output/Dredmode_Wellington.csv ~/projectoutput/Dredmode_Wellington.csv


# get the inputs value in Wellington
Input_Wellington = (
     gxp_embrium
    .filter(col("Location")=='Wellington')
    .filter((col("Property_Name")=='Inputs') | (col("Property_Name")=='digital_register'))
    .orderBy(col("Date"), ascending = False)
)
Input_Wellington.show(25, False)
Input_Wellington.count()   ## 


Input_Wellington.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/jpe116/output/Input_Wellington.csv')

hdfs dfs -ls /user/jpe116/output

hdfs dfs -copyToLocal /user/jpe116/output/Input_Wellington.csv ~/projectoutput/Input_Wellington.csv


# check delta
delta_df = (
     join_prop
     .filter(f.col('Delta') == 'True')
)
delta_df.show()
delta_df.count() 

#821525  with new_embrium
#633987 join_prop

delta_df.select('Property_Name').distinct().show()

# only energy_active_export| energy_active_import


