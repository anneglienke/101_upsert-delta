from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

if __name__ == '__main__':
    
    # Criar a sessão Spark
    spark = SparkSession \
      .builder \
      .appName("Job - increment") \
      .getOrCreate()    

    # Ler os dados delta
    delta_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .load("titanic2.csv")

    # Creates delta view
    delta_data.createOrReplaceTempView("deltaView")

    # Reads raw-zone and creates raw view
    raw_data = spark.read.format("parquet").load("raw-zone/")
    raw_data.createOrReplaceTempView("rawView")
   
    incrementedView = spark.sql(
        """select * 
        from deltaView as d
        where d.CHANGE_TIMESTAMP > (select max(CHANGE_TIMESTAMP)
                                    from rawView as r)
            """)
    #incrementedView.show(truncate=False)  

    # Gravar dados na bronze-zone
    incrementedView.write.mode("append").parquet("raw-zone/")












