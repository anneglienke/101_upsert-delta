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

    # Ler os dados 
    historical_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic.csv")

    # Criar view
    historical_data.createOrReplaceTempView("historical_data")
   
   # Criar colunas: delta_flag com valor I e current timestamp
    historical_data = spark.sql(
        """select 
                historical_data.*,
                'I' as delta_flag,
                current_timestamp() as delta_timestamp              
            from 
                historical_data""")

    # Criar a tabela histórica
    historical_data.write.format("delta").save("delta/historical") 

    # Visualizar tabela criada
    h_df = spark.read.format("delta").load("delta/historical/")
    h_df.show()

    # Parar a sessão Spark
    spark.stop()












