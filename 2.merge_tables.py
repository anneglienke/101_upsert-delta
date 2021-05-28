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
    new_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic2.csv")
   
   # Criar view para adicionar colunas
    new_data.createOrReplaceTempView("new_data")

   # Criar colunas: delta_flag com valor I, update_flag com valor U e current timestamp
    new_data = spark.sql(
        """select
                new_data.*,
                'I' as delta_flag, 
                'U' as update_flag,
                current_timestamp() as delta_timestamp   
            from 
                new_data""")
    
    # Ler tabela histórica 
    h_df = DeltaTable.forPath(spark,"delta/historical/") 
    
    # Criar a tabela nova (se criar view, ele retorna 'AttributeError: 'NoneType' object has no attribute 'alias')
    new_df = new_data.write.format("delta").save("delta/updates") 
    n_df = spark.read.format("delta").load("delta/updates/")

    # Merge
    # Faz update, adiciona flag U e novo timestamp para todos os registros que se repetirem nas duas tabelas, independente de haver mudança. Adiciona registros, flag e novo timestamp I para todos os registros novos. Os registros deletados permanecem com flag I, porém com timestamp desatualizada.
    h_df.alias("h") \
    .merge(n_df.alias("n"),
    "h.PassengerId = n.PassengerId") \
    .whenMatchedUpdate(
      set = {
        "PassengerId":"n.PassengerId",
        "Survived":"n.Survived",
        "Pclass":"n.Pclass",
        "Name":"n.Name",
        "Sex":"n.Sex",
        "Age":"n.Age",
        "Embarked":"n.Embarked",
        "delta_flag":"n.update_flag",
        "delta_timestamp":"n.delta_timestamp"
      }) \
      .whenNotMatchedInsert( 
        values = {
          "PassengerId":"n.PassengerId",
          "Survived":"n.Survived",
          "Pclass":"n.Pclass",
          "Name":"n.Name",
          "Sex":"n.Sex",
          "Age":"n.Age",
          "Embarked":"n.Embarked",
          "delta_flag":"n.delta_flag",
          "delta_timestamp":"n.delta_timestamp"   
        }) \
    .execute() 

    # Parar a sessão Spark
    spark.stop()