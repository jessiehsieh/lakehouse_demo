# %%

## Set up spark session 

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os


## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1" ## Nessie Server URI
WAREHOUSE = "s3://test/" ## S3 Address to Write to
STORAGE_URI = "http://172.18.0.5:9000"


conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
        .set("spark.driver.memory", "10g") \
        #packages
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.82.0,org.apache.iceberg:iceberg-aws-bundle:1.5.2')
        #SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        #Configuring Catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')

)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

# %%

# %timeit 
spark.sql("SELECT count(distinct user) FROM nessie.interactions_PartBy_date").show()

# 11.6 s ± 965 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
# %%


# %timeit 
spark.sql("SELECT count(distinct user) FROM nessie.interactions_PartBy_date where date = '2020-02-17'").show()
# 2.14 s ± 550 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

