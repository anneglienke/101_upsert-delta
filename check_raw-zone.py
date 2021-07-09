from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Create Spark Session
    spark = SparkSession \
      .builder \
      .appName("Job - Raw-zone") \
      .getOrCreate()    

    # Read and show data on raw-zone
    b_df = spark.read.format("parquet").load("raw-zone/")
    b_df.show(truncate=False)  

    # Stop Spark session
    spark.stop()