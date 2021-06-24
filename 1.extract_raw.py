from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Criar a sessão Spark
    spark = SparkSession \
      .builder \
      .appName("Job - Raw-zone") \
      .getOrCreate()    

    # Ler os dados para carga inicial
    raw_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic.csv")

    # Criar view bronze
    raw_data.createOrReplaceTempView("rawView")
   
   # Criar view
    rawView = spark.sql(
        """select *            
            from rawView
            """)

    # Gravar dados na bronze-zone
    rawView.write.parquet("raw-zone/")












