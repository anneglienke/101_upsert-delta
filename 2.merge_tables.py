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

    # Ler os dados atuais
    new_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic2.csv")
     
    # Ler bronze
    b_df = DeltaTable.forPath(spark,"bronze-zone/") 
    
    # Criar a tabela nova
    new_df = new_data.write.format("delta").save("landing-zone") 
    n_df = spark.read.format("delta").load("landing-zone/")

    # Fazer merge
    b_df.alias("b") \
    .merge(n_df.alias("n"),
    "b.PassengerId = n.PassengerId") \
    .whenMatchedDelete(condition = "n.CHANGE_TYPE = 'D'") \
    .whenMatchedUpdate(
      condition = "n.CHANGE_TYPE ='A' OR b.CHANGE_TYPE = 'I'",
      set = {
        "PassengerId":"n.PassengerId",
        "Survived":"n.Survived",
        "Pclass":"n.Pclass",
        "Name":"n.Name",
        "Sex":"n.Sex",
        "Age":"n.Age",
        "Embarked":"n.Embarked",
        "CHANGE_TYPE":"n.CHANGE_TYPE",
        "CHANGE_TIMESTAMP":"n.CHANGE_TIMESTAMP"
        }) \
    .whenNotMatchedInsertAll(condition = "n.CHANGE_TYPE = 'I'") \
    .execute() 

    # Parar a sessão Spark
    spark.stop()