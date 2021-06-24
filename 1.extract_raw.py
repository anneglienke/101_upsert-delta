from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Create Spark Session
    spark = SparkSession \
      .builder \
      .appName("Job - Raw-zone") \
      .getOrCreate()    

    # Read raw data
    raw_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic.csv")

    # Create raw view
    raw_data.createOrReplaceTempView("rawView")
   
    rawView = spark.sql(
        """select *            
            from rawView
            """)

    # Write data to raw-zone
    rawView.write.parquet("raw-zone/")

    # Stop Spark session
    spark.stop()











