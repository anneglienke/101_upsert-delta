from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Criar a sessão Spark
    spark = SparkSession \
      .builder \
      .appName("Job - Raw-zone") \
      .getOrCreate()    

    # Ler bronze alterada
    b_df = spark.read.format("parquet").load("raw-zone/")
    b_df.show(truncate=False)  

    # Parar a sessão Spark
    spark.stop()