from pyspark.sql import SparkSession, Window
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("Spark-Notebook") \
    .getOrCreate()


BOOTSTRAP_SERVERS = 'pkc-l7q2j.europe-north1.gcp.confluent.cloud:9092'
CLUSTER_API_KEY='DIBJAZKW5GDZFM5J'
CLUSTER_API_SECRET='LiGBZPDKegT/Hn1mh8Y89etvFW5FDASVSq94VUVPAhZ14T/IAjIA6LXoqrri5Ols'


def sink_kafka(df, topic, output_mode='append'):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{CLUSTER_API_KEY}" password="{CLUSTER_API_SECRET}";') \
        .outputMode(output_mode) \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


df_kafka_raw_fhv = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", "rides_fhv") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{CLUSTER_API_KEY}" password="{CLUSTER_API_SECRET}";') \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .load()


df_kafka_raw_green = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", "rides_green") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{CLUSTER_API_KEY}" password="{CLUSTER_API_SECRET}";') \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .load()

df_kafka_fhv_encoded = df_kafka_raw_fhv.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
df_kafka_green_encoded = df_kafka_raw_green.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")


# write data from both streams to one topic rides_all
write2kafka_from_fhv_q = sink_kafka(df_kafka_fhv_encoded, 'rides_all')
write2kafka_from_green_q = sink_kafka(df_kafka_green_encoded, 'rides_all') 

# read it back
df_rides_all_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", "rides_all") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{CLUSTER_API_KEY}" password="{CLUSTER_API_SECRET}";') \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .load()

df_rides_all = df_rides_all_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 

grouped_df = df_rides_all.groupBy(F.col("key")).count()

# Find the key with the maximum count using a window function
windowed_df = grouped_df.withColumn("max_count", max(F.col("count")).over(Window.partitionBy()))

# Filter out the keys that have less than max_count
filtered_df = windowed_df.filter(F.col("count") == F.col("max_count")).drop(F.col("max_count"))

query = filtered_df.writeStream \
.outputMode("complete") \
.format("console") \
.start()