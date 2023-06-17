#!/usr/bin/env python
# coding: utf-8

# ## Migrating from Spark to BigQuery via Dataproc -- Part 1
# 
# * [Part 1](01_spark.ipynb): The original Spark code, now running on Dataproc (lift-and-shift).
# * [Part 2](02_gcs.ipynb): Replace HDFS by Google Cloud Storage. This enables job-specific-clusters. (cloud-native)
# * [Part 3](03_automate.ipynb): Automate everything, so that we can run in a job-specific cluster. (cloud-optimized)
# * [Part 4](04_bigquery.ipynb): Load CSV into BigQuery, use BigQuery. (modernize)
# * [Part 5](05_functions.ipynb): Using Cloud Functions, launch analysis every time there is a new file in the bucket. (serverless)
# 

# ### Copy data to HDFS
# 
# 
# 
# The data itself comes from the 1999 KDD competition. Let's grab 10% of the data to use as an illustration.

# In[1]:


get_ipython().system('wget http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz')


# In[2]:


get_ipython().system('hadoop fs -put kddcup* /')


# In[3]:


get_ipython().system('hadoop fs -ls /')


# ### Reading in data
# 
# The data are CSV files. In Spark, these can be read using textFile and splitting rows on commas.

# In[6]:


from pyspark.sql import SparkSession, SQLContext, Row

spark = SparkSession.builder.appName("kdd").getOrCreate()
sc = spark.sparkContext
data_file = "hdfs:///kddcup.data_10_percent.gz"
raw_rdd = sc.textFile(data_file).cache()
raw_rdd.take(5)


# In[7]:


csv_rdd = raw_rdd.map(lambda row: row.split(","))
parsed_rdd = csv_rdd.map(lambda r: Row(
    duration=int(r[0]), 
    protocol_type=r[1],
    service=r[2],
    flag=r[3],
    src_bytes=int(r[4]),
    dst_bytes=int(r[5]),
    wrong_fragment=int(r[7]),
    urgent=int(r[8]),
    hot=int(r[9]),
    num_failed_logins=int(r[10]),
    num_compromised=int(r[12]),
    su_attempted=r[14],
    num_root=int(r[15]),
    num_file_creations=int(r[16]),
    label=r[-1]
    )
)
parsed_rdd.take(5)


# ### Spark analysis
# 
# One way to analyze data in Spark is to call methods on a dataframe.

# In[6]:


sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(parsed_rdd)
connections_by_protocol = df.groupBy('protocol_type').count().orderBy('count', ascending=False)
connections_by_protocol.show()


# Another way is to use Spark SQL

# In[7]:


df.registerTempTable("connections")
attack_stats = sqlContext.sql("""
                           SELECT 
                             protocol_type, 
                             CASE label
                               WHEN 'normal.' THEN 'no attack'
                               ELSE 'attack'
                             END AS state,
                             COUNT(*) as total_freq,
                             ROUND(AVG(src_bytes), 2) as mean_src_bytes,
                             ROUND(AVG(dst_bytes), 2) as mean_dst_bytes,
                             ROUND(AVG(duration), 2) as mean_duration,
                             SUM(num_failed_logins) as total_failed_logins,
                             SUM(num_compromised) as total_compromised,
                             SUM(num_file_creations) as total_file_creations,
                             SUM(su_attempted) as total_root_attempts,
                             SUM(num_root) as total_root_acceses
                           FROM connections
                           GROUP BY protocol_type, state
                           ORDER BY 3 DESC
                           """)
attack_stats.show()


# In[8]:


get_ipython().run_line_magic('matplotlib', 'inline')
ax = attack_stats.toPandas().plot.bar(x='protocol_type', subplots=True, figsize=(10,25))


# Copyright 2019 Google Inc. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
