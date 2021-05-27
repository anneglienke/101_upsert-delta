import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv, find_dotenv
from pyspark.sql.functions import *
from IPython.display import display
from pyspark.sql import SQLContext

load_dotenv()

jars = os.environ.get("JARS_PATH")

if __name__ == '__main__':
    
    # Criar a sessão Spark
    spark = SparkSession \
      .builder \
      .appName("Job - Raw-zone") \
      .config("spark.jars", jars) \
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .getOrCreate()    

    from delta.tables import * 

    # Ler os dados
    new_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic2.csv")
   
   # Criar view para adicionar colunas
    new_data.createOrReplaceTempView("new_data")

   # Criar colunas: delta_flag com valor I e current timestamp
    new_data = spark.sql(
        """select
                new_data.*,
                'U' as delta_flag, 
                current_timestamp() as delta_timestamp   
            from 
                new_data""")
    
    # Ler tabela histórica 
    h_df = DeltaTable.forPath(spark,"delta/historical/") 
    
    # Criar a tabela nova
    new_df = new_data.write.format("delta").save("delta/updates") 
    n_df = spark.read.format("delta").load("delta/updates/")

    # Merge
    h_df.alias("h") \
    .merge(n_df.alias("n"),
    "h.PassengerId = n.PassengerId") \
    .whenNotMatchedInsertAll() \
    .whenMatchedUpdateAll() \
    .execute() 

    # # Parar a sessão Spark
    # spark.stop()


