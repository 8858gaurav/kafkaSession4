# trigger kafka batch job from the kakfka topics.
# Install this libraries in databricks.
# run this code in databricks.

from pyspark.sql.functions import *
from confluent_kafka import Producer
import json


# get these details from confluent kafka, search it on google.
confluentBootstrapServers = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
confluentApiKey = 'XRH2SVIJU7FYNBWV'
confluentSecret = 'kl7k55Ri9mLD+UhI9crfnw146w+WamTumbaLoHP2+YnJJP+s3Dl5u8b2ED+8ILBX'
# we created the topic in confluent kafka.
confluentTopicName = 'topic_kafka_vs'

# https://confluent.cloud/environments/env-v8jrqz/clusters/lkc-mo88oq/settings/kafka
conf = {'bootstrap.servers': confluentBootstrapServers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': confluentApiKey,
            'sasl.password': confluentSecret,
            'client.id': 'Gaurav McBook'}

producer = Producer(conf)



if __name__ == '__main__':
   
   # ==================================================#
   # Writing to a Topic ===============================#

    def acked(err, msg):
        if err is not None:
            print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
        else:
            print('msg produced: %s' % (str(msg)))
            print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
            print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

    with open('/Users/gauravmishra/Desktop/SparkSession4/Data/orders_input.json', mode= 'r' ) as files:
        for line in files:
            order = json.loads(line)
            customer_id = str(order['customer_id'])
            producer.produce(topic = 'topic_kafka_vs', key = customer_id, value = line, callback = acked)
            producer.poll(1)
            producer.flush()
    
   # ==================================================#
   # reading from a Topic ===============================#
  
    orders_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",confluentBootstrapServers) \
    .option("kafka.security.protocol","SASL_SSL") \
    .option("kafka.sasl.mechanism","PLAIN") \
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm","https") \
    .option("subscribe",confluentTopicName) \
    .option("startingTimestamp", 1) \
    .option("maxOffsetsPerTrigger", 50) \
    .load()
    # it read the topic from the very starting - startingTimestamp
    # microbatches are of same size - maxOffsetsPerTrigger


    print(orders_df.head(10))


    converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")
    # converted_orders_df is a complete string, don't treat it like a json format.

    print(converted_orders_df.head(10))

    orders_schema = "order_id long,customer_id long,customer_fname \
        string,customer_lname string,city string,state string,pincode long,line_items \
        array<struct<order_item_id: long,order_item_product_id: \
        long,order_item_quantity: long,order_item_product_price: \
        float,order_item_subtotal: float>>"
    
    parsed_orders_df = converted_orders_df.select("key", from_json("value", orders_schema).alias("value"), "topic", "partition", "offset","timestamp","timestampType")
    # with this we can get the json format to get the columns values. we imposed the json structure on converted_ordes_df.
    print(parsed_orders_df.head(10))

    parsed_orders_df.createOrReplaceTempView("orders")
    
    exploded_orders = spark.sql("""select key, value.order_id as order_id, value.customer_id as customer_id, 
                value.customer_fname as customer_fname, value.customer_lname as customer_lname,
                value.city as city, value.state as state, value.pinode as pincode, explode(value.line_items) as 
                lines from orders""")
    
    # for one customer you'll get a multiple line items in the form of json, we've to split it again.

    exploded_orders.createOrReplaceTempView("exploded_orders")
    print(exploded_orders.head(10))

    flattened_orders = spark.sql("""select  order_id, customer_id, customer_fname, customer_lname
                ,city, state, pincode, lines.order_item_id as item_id, lines.order_item_product_id as product_id,
                lines.order_item_quantity as quantity, lines.order_item_product_price as price,
                lines.order_item_subtotal as subtotal from exploded_orders""")
    
    
    print(flattened_orders.head(10))
    # key, & customer_id will remain same. we can remove it. 


    # now we are writing this data to a persistent storage. 
    flattened_orders \
    .writeStream \
    .queryName("ingestionquery") \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation","checkpointdir301") \
    .toTable("orderstablenew301")
    # .toTable is an actions, rest were transaformation while writing the df.
    # delta table will persist at some locations (in harddrisk), kakfka topic data are available only gfor 7 days by default.
    # to create this table (orderstablenew301), it needs as hive warehouse directory.
    # databricks have already spark session available to them. 

    # In Kafka we can trasnfered the data in th form of key value pairs only.
    spark.sql("select * from orderstablenew301").show()





