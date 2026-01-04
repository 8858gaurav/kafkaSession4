from pyspark.sql.functions import *
from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'W5T2APVWR6CK5PZR',
        'sasl.password': 'cfltMbHZdd+X9y4C//n09g91QZP43UQtbsQp3qWvUtb8HKs9g4eMII2hA5g/G+EA',
        'client.id': 'ccloud-python-client-922e35f1-9bca-46cc-9b6b-ec4bacf7a904'}
producer = Producer(conf)

# get these details from confluent kafka, search it on google.
confluentBootstrapServers = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
confluentApiKey = 'W5T2APVWR6CK5PZR'
confluentSecret = 'cfltMbHZdd+X9y4C//n09g91QZP43UQtbsQp3qWvUtb8HKs9g4eMII2hA5g/G+EA'
# we created the topic in confluent kafka.
confluentTopicName = 'topic_0'

landing_zone = '/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_data'
orders_data = landing_zone + 'orders_data'
checkpoint_path = landing_zone + 'orders_cp_new_2'

%sql
GRANT USE CATALOG ON CATALOG misgaurav_databricks_ws_7405616800162977 TO `gauravmishra7080@gmail.com`;
GRANT USE SCHEMA ON SCHEMA misgaurav_databricks_ws_7405616800162977.default TO `gauravmishra7080@gmail.com`;
GRANT CREATE TABLE ON SCHEMA misgaurav_databricks_ws_7405616800162977.default TO `gauravmishra7080@gmail.com`;

%sql
show catalogs;

%sql
use catalog misgaurav_databricks_ws_7405616800162977;
show tables;
-- database     tableName       isTemporary
--              _sqldf     true

show schemas;
-- databaseName
-- default
-- information_schema
use schema default;

#============================
# Reading from a kakfka topic =
#============================

# first create a pipeline to read the data from the streaming sources, then create a pipeline to write the data somewhere else.
  
orders_df = spark \
   .readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers",confluentBootstrapServers) \
   .option("kafka.security.protocol","SASL_SSL") \
   .option("kafka.sasl.mechanism","PLAIN") \
   .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
   .option("kafka.ssl.endpoint.identification.algorithm","https") \
   .option("subscribe",confluentTopicName) \
   .option("startingOffsets", "earliest") \
   .option("failOnDataLoss", 'false') \
   .option("maxOffsetsPerTrigger", 50) \
   .load()
orders_df.printSchema()

# root
#  |-- key: binary (nullable = true)
#  |-- value: binary (nullable = true)
#  |-- topic: string (nullable = true)
#  |-- partition: integer (nullable = true)
#  |-- offset: long (nullable = true)
#  |-- timestamp: timestamp (nullable = true)
#  |-- timestampType: integer (nullable = true)

   # it read the topic from the very starting - startingTimestamp
   # microbatches are of same size - maxOffsetsPerTrigger


converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")
# converted_orders_df is a complete string, don't treat it like a json format.


orders_schema = "order_id long,customer_id long,customer_fname \
        string,customer_lname string,city string,state string,pincode long,line_items \
        array<struct<order_item_id: long,order_item_product_id: \
        long,order_item_quantity: long,order_item_product_price: \
        float,order_item_subtotal: float>>"
    
parsed_orders_df = converted_orders_df.select("key", from_json("value", orders_schema).alias("value"), "topic", "partition", "offset","timestamp","timestampType")
    # with this we can get the json format to get the columns values. we imposed the json structure on converted_ordes_df.

parsed_orders_df.createOrReplaceTempView("orders")
    
exploded_orders = spark.sql("""select key, value.order_id as order_id, value.customer_id as customer_id, 
                value.customer_fname as customer_fname, value.customer_lname as customer_lname,
                value.city as city, value.state as state, value.pincode as pincode, explode(value.line_items) as 
                lines from orders""")
    
    # for one customer you'll get a multiple line items in the form of json, we've to split it again.

exploded_orders.createOrReplaceTempView("exploded_orders")


flattened_orders = spark.sql("""select  order_id, customer_id, customer_fname, customer_lname
                ,city, state, pincode, lines.order_item_id as item_id, lines.order_item_product_id as product_id,
                lines.order_item_quantity as quantity, lines.order_item_product_price as price,
                lines.order_item_subtotal as subtotal from exploded_orders""")
    
    # key, & customer_id will remain same. we can remove it. 


# now we are writing this data to a persistent storage. 
flattened_orders \
    .writeStream \
    .queryName("ingestionquery") \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",checkpoint_path) \
    .toTable("misgauravorderstablenew")
    # .toTable is an actions, rest were transaformation while writing the df.
    # delta table will persist at some locations (in harddrisk), kakfka topic data are available only gfor 7 days by default.
    # to create this table (orderstablenew), it needs as hive warehouse directory.
    # databricks have already spark session available to them. 

    # In Kafka we can trasnfered the data in th form of key value pairs only.

# first publish the data to a topic, so that spark stream job keeps running.
#========================================================
# Spark read stream jb keep runing, now placed the files.
#========================================================

spark.sql("select * from misgauravorderstablenew limit 5").show()
# +--------+-----------+--------------+--------------+----+-----+-------+-------+----------+--------+-----+--------+
# |order_id|customer_id|customer_fname|customer_lname|city|state|pincode|item_id|product_id|quantity|price|subtotal|
# +--------+-----------+--------------+--------------+----+-----+-------+-------+----------+--------+-----+--------+
# +--------+-----------+--------------+--------------+----+-----+-------+-------+----------+--------+-----+--------+


## run this code for orders_input.json file. (has 2 rows)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_data/orders_data/orders_input.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()


spark.sql("select * from misgauravorderstablenew limit 5").show()
# +--------+-----------+--------------+--------------+-------+-----+-------+-------+----------+--------+------+--------+
# |order_id|customer_id|customer_fname|customer_lname|   city|state|pincode|item_id|product_id|quantity| price|subtotal|
# +--------+-----------+--------------+--------------+-------+-----+-------+-------+----------+--------+------+--------+
# |       2|        256|         David|     Rodriguez|Chicago|   IL|  60625|      2|      1073|       1|199.99|  199.99|
# |       2|        256|         David|     Rodriguez|Chicago|   IL|  60625|      3|       502|       5|  50.0|   250.0|
# |       2|        256|         David|     Rodriguez|Chicago|   IL|  60625|      4|       403|       1|129.99|  129.99|
# |       1|      11599|          Mary|        Malone|Hickory|   NC|  28601|      1|       957|       1|299.98|  299.98|
# +--------+-----------+--------------+--------------+-------+-----+-------+-------+----------+--------+------+--------+

%sql
describe detail misgauravorderstablenew; -- records -> 2 files.

-- format       delta
-- id   ea8caabe-5f4a-4401-867f-2d278c12ad3c
-- name misgaurav_databricks_ws_7405616800162977.default.misgauravorderstablenew
-- description  null
-- location     abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405616800162977/__unitystorage/catalogs/5871b843-ff2f-45b7-ad9a-797069d3b507/tables/9997d2b1-704f-4761-8688-564183e6c20b
-- createdAt    2026-01-04T06:45:31.701Z
-- lastModified 2026-01-04T06:45:50Z
-- partitionColumns     []
-- clusteringColumns    []
-- numFiles     2
-- sizeInBytes  6196
-- properties   {"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true","delta.writePartitionColumnsToParquet":"true","delta.enableRowTracking":"true","delta.rowTracking.materializedRowCommitVersionColumnName":"_row-commit-version-col-bef11d19-e0e9-4ce5-bb31-65d1ba45334b","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-b728d6c3-1d45-4d55-8083-1bab49149d65"}
-- minReaderVersion     3
-- minWriterVersion     7
-- tableFeatures        ["appendOnly","deletionVectors","domainMetadata","invariants","rowTracking"]
-- statistics   {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto        FALSE


spark.sql("select count(*) from misgauravorderstablenew").show()
## +--------+
# |count(1)|
# +--------+
# |     4  |
# +--------+


### re run  only this piece of code by placing a new file: order_input.json (has 2 rows)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_data/orders_data/order_input.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()


%sql
describe detail misgauravorderstablenew; -- again 2 rows, so 2 more files.
-- format       delta
-- id   ea8caabe-5f4a-4401-867f-2d278c12ad3c
-- name misgaurav_databricks_ws_7405616800162977.default.misgauravorderstablenew
-- description  null
-- location     abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405616800162977/__unitystorage/catalogs/5871b843-ff2f-45b7-ad9a-797069d3b507/tables/9997d2b1-704f-4761-8688-564183e6c20b
-- createdAt    2026-01-04T06:45:31.701Z
-- lastModified 2026-01-04T06:48:34Z
-- partitionColumns     []
-- clusteringColumns    []
-- numFiles     4
-- sizeInBytes  12392
-- properties   {"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true","delta.writePartitionColumnsToParquet":"true","delta.enableRowTracking":"true","delta.rowTracking.materializedRowCommitVersionColumnName":"_row-commit-version-col-bef11d19-e0e9-4ce5-bb31-65d1ba45334b","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-b728d6c3-1d45-4d55-8083-1bab49149d65"}
-- minReaderVersion     3
-- minWriterVersion     7
-- tableFeatures        ["appendOnly","deletionVectors","domainMetadata","invariants","rowTracking"]
-- statistics   {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto        FALSE


spark.sql("select count(*) from misgauravorderstablenew").show()
## +--------+
# |count(1)|
# +--------+
# |     8  |
# +--------+


### re run  only this piece of code by placing a new file: order_input_new.json (has 2 rows)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_data/orders_data/order_input_new.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()


%sql
describe detail misgauravorderstablenew; -- again 2 new rows, so 2 more files.
-- format       delta
-- id   ea8caabe-5f4a-4401-867f-2d278c12ad3c
-- name misgaurav_databricks_ws_7405616800162977.default.misgauravorderstablenew
-- description  null
-- location     abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405616800162977/__unitystorage/catalogs/5871b843-ff2f-45b7-ad9a-797069d3b507/tables/9997d2b1-704f-4761-8688-564183e6c20b
-- createdAt    2026-01-04T06:45:31.701Z
-- lastModified 2026-01-04T06:50:17Z
-- partitionColumns     []
-- clusteringColumns    []
-- numFiles     6
-- sizeInBytes  18588
-- properties   {"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true","delta.writePartitionColumnsToParquet":"true","delta.enableRowTracking":"true","delta.rowTracking.materializedRowCommitVersionColumnName":"_row-commit-version-col-bef11d19-e0e9-4ce5-bb31-65d1ba45334b","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-b728d6c3-1d45-4d55-8083-1bab49149d65"}
-- minReaderVersion     3
-- minWriterVersion     7
-- tableFeatures        ["appendOnly","deletionVectors","domainMetadata","invariants","rowTracking"]
-- statistics   {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto        FALSE



spark.sql("select count(*) from misgauravorderstablenew").show()
## +--------+
# |count(1)|
# +--------+
# |     12 |
# +--------+


### re run  only this piece of code by placing a new file: order_new.json (has 2 rows)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_data/orders_data/order_new.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()


%sql
describe detail misgauravorderstablenew; -- again 2 new rows, so 2 more files.
-- format       delta
-- id   ea8caabe-5f4a-4401-867f-2d278c12ad3c
-- name misgaurav_databricks_ws_7405616800162977.default.misgauravorderstablenew
-- description  null
-- location     abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405616800162977/__unitystorage/catalogs/5871b843-ff2f-45b7-ad9a-797069d3b507/tables/9997d2b1-704f-4761-8688-564183e6c20b
-- createdAt    2026-01-04T06:45:31.701Z
-- lastModified 2026-01-04T06:52:38Z
-- partitionColumns     []
-- clusteringColumns    []
-- numFiles     8
-- sizeInBytes  24784
-- properties   {"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true","delta.writePartitionColumnsToParquet":"true","delta.enableRowTracking":"true","delta.rowTracking.materializedRowCommitVersionColumnName":"_row-commit-version-col-bef11d19-e0e9-4ce5-bb31-65d1ba45334b","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-b728d6c3-1d45-4d55-8083-1bab49149d65"}
-- minReaderVersion     3
-- minWriterVersion     7
-- tableFeatures        ["appendOnly","deletionVectors","domainMetadata","invariants","rowTracking"]
-- statistics   {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto        FALSE


spark.sql("select count(*) from misgauravorderstablenew").show()
# +--------+
# |count(1)|
# +--------+
# |     16 |
# +--------+


%fs
ls /Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/

path                                                                                                    name                           size    modificationTime
dbfs:/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_data/                 retail_data/                    0       1767507342000
dbfs:/Volumes/misgaurav_databricks_ws_7405616800162977/default/misgaurav_v/retail_dataorders_cp_new_2/  retail_dataorders_cp_new_2/     0       1767509132000
