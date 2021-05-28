from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Criar a sessão Spark
    spark = SparkSession \
      .builder \
      .appName("Job - Raw-zone") \
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .getOrCreate()    

    from delta.tables import * 

    # Ler tabelas alterada
    h_df = spark.read.format("delta").load("delta/historical/")
    h_df.show(truncate=False)  

    n_df = spark.read.format("delta").load("delta/updates/")
    n_df.show(truncate=False)  

    # Parar a sessão Spark
    spark.stop()