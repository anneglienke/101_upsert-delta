# Snapshot of the current db and creation of staging-zone

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Create Spark session
    spark = SparkSession \
      .builder \
      .appName("Job - Ingestion Staging-Zone") \
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
      .getOrCreate()    

    from delta.tables import *    

    # Read delta
    delta_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic2.csv")

    # Create delta view
    delta_data.createOrReplaceTempView("deltaView")

    # Read raw-zone and create raw view
    raw_data = spark.read.format("parquet").load("raw-zone/")
    raw_data.createOrReplaceTempView("rawView")
   
   # Create incremented view with only new data 
    incrementedView = spark.sql(
        """ SELECT A.* 
        FROM rawView as A
        INNER JOIN (SELECT   
                        PassengerId, 
                        MAX(CHANGE_TIMESTAMP) as MAX_CHANGE_TIMESTAMP
                    FROM rawView
                    GROUP BY PassengerId
                    ) B  
        ON A.PassengerId = B.PassengerId AND A.CHANGE_TIMESTAMP = B.MAX_CHANGE_TIMESTAMP
        WHERE CHANGE_TYPE <> 'D'
            """)

    incrementedView.show(truncate=False)  

    # Append incremented data to raw-zone
    incrementedView.write.format("delta").save("staging-zone/")
    
    # Delete historical delta files
    staging_zone = DeltaTable.forPath(spark, "staging-zone/")
    staging_zone.vacuum(0.000001)

    # Stop Spark session
    spark.stop()












