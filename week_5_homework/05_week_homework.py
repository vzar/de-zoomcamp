#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
import pandas as pd


# In[3]:


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


# In[4]:


spark.version


# In[9]:


schema = types.StructType([ 
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropoff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
    ])


# In[10]:


df = spark.read \
     .option("header","true") \
     .schema(schema) \
     .csv('fhvhv_tripdata_2021-06.csv')


# In[12]:


df.repartition(12).write.parquet('fhvhv/2021/06/',mode='overwrite')


# In[25]:


df.filter(df.pickup_datetime >= '2021-06-15 00:00:00') \
  .filter(df.pickup_datetime <= '2021-06-15 23:59:59').count()


# In[29]:


df.registerTempTable('trip_data')


# In[43]:


df0=spark.sql("""select date(pickup_datetime),
                 max(
                    bigint(to_timestamp(dropoff_datetime))-
                    bigint(to_timestamp(pickup_datetime)))/3600.0 
                 from trip_data
                 group by 1 order by 2 desc""").show()


# In[33]:


schema_zone=types.StructType([ 
    types.StructField('LocationID', types.IntegerType(), True), 
    types.StructField('Borough', types.StringType(), True), 
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
    ])


# In[34]:


df_zone = spark.read \
     .option("header","true") \
     .schema(schema_zone) \
     .csv('taxi_zone_lookup.csv')


# In[35]:


df_zone.printSchema()


# In[36]:


df_zone.show()


# In[37]:


df_zone.registerTempTable("zones")


# In[40]:


spark.sql("""select Zone,count(1) from 
                   trip_data join zones on PULocationID=LocationID 
                   group by 1 
                   order by 2 desc 
                   limit 1""").show()


# In[ ]:




