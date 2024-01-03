from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLExample").getOrCreate()

#---------------------------# CSV file read garera raw ma rakhni
csv_file_path = "z1SearchByArtist.csv"
raw_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

#--------------------------------------Temporary raw table banauni
raw_df.createOrReplaceTempView("raw_table")

#-------------------------------------stage table lai query lekhni
query_stage = """
    SELECT 
"""
stage_df = spark.sql(query_stage)

stage_df.createOrReplaceTempView("stage_table")

#----------------------------------------dim table banauni query haru
query_dim = """
    SELECT
"""
dim_df = spark.sql(query_dim)

#-----------------------------------------fact table banauni query haru
query_fact = """
    SELECT 
"""
fact_df = spark.sql(query_fact)


dim_df.write.mode("overwrite").parquet("path/to/dim_table")
fact_df.write.mode("overwrite").parquet("path/to/fact_table")

spark.stop()
