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

    # Ler os dados para carga inicial
    raw_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic.csv")

    # Criar view bronze
    raw_data.createOrReplaceTempView("rawView")
   
   # Selecionar só CHANGE_TYPE <> 'D'
    rawView = spark.sql(
        """select *            
            from rawView
            where CHANGE_TYPE <> 'D'
            """)

    # rawView.show(truncate=False)

    # Gravar dados na bronze-zone
    rawView.write.format("delta").save("bronze-zone/")












