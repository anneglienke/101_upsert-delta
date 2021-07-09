# Incremental snapshot ingestion

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Create Spark session
    spark = SparkSession \
      .builder \
      .appName("Job - Increment Staging-zone") \
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
      .getOrCreate()    

    from delta.tables import * 

    # Read staging data
    staging_data = DeltaTable.forPath(spark,"staging-zone/")

    # Read delta
    delta_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic3.csv")

    # Merge tables
    staging_data.alias("s") \
    .merge(delta_data.alias("d"),
    "s.PassengerId = d.PassengerId") \
    .whenMatchedDelete(condition = "d.CHANGE_TYPE = 'D'") \
    .whenMatchedUpdateAll(condition = "d.CHANGE_TYPE ='A'") \
    .whenNotMatchedInsertAll(condition = "d.CHANGE_TYPE = 'I'") \
    .execute() 

    # Delete historical delta files
    staging_zone = DeltaTable.forPath(spark, "staging-zone/")
    staging_zone.vacuum(0.000001)

    # Stop Spark session
    spark.stop()