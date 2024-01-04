from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SQLExample").getOrCreate()

#---------------------------# CSV file read garera raw ma rakhni
csv_file_path = "z1SearchByArtist.csv"
raw_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

raw_df = raw_df.drop("href")
raw_df = raw_df.dropna()

#--------------------------------------Temporary raw table banauni
raw_df.createOrReplaceTempView("raw_table")

# -------------------------------------stage table lai query lekhni


query_stage = """
    SELECT *
    from raw_table;
"""
stage_df = spark.sql(query_stage)

stage_df.createOrReplaceTempView("stage_table")


schema = StructType([
    StructField("artist_id", StringType(), True),
    StructField("artist_name", StringType(), True)
])

# Create an empty DataFrame with the specified schema
empty_data = []
empty_df = spark.createDataFrame(empty_data, schema)

# Register the empty DataFrame as a temporary view
empty_df.createOrReplaceTempView("dim_artist_name")

query = '''
    SELECT s.id as artist_id, s.artist_name
    FROM stage_table s
    LEFT JOIN dim_artist_name dan
    ON s.id = dan.artist_id
    WHERE dan.artist_id IS NULL
'''
dim_artist_name_df = spark.sql(query)

dim_artist_name_df.createOrReplaceTempView("dim_artist_name")


empty_schema = StructType([
    StructField("genre_id", IntegerType(), False),
    StructField("genres", StringType(), True)
])

empty_data_genres = []
empty_df_genres = spark.createDataFrame(empty_data_genres, empty_schema)

empty_df_genres.createOrReplaceTempView("dim_genres")

query_genres = '''
    SELECT monotonically_increasing_id() as genre_id, s.genres
    FROM stage_table s
    LEFT JOIN dim_genres dg
    ON s.genres = dg.genres
    WHERE dg.genres IS NULL
'''

dim_genres_df = spark.sql(query_genres)
dim_genres_df.createOrReplaceTempView("dim_genres")



# #-----------------------------------------fact table banauni query haru
fact_schema = StructType([
    StructField("artist_id", StringType(), True),
    #yo bhanya artist_id column string type ani null value ni allow garcha
    StructField("popularity", StringType(), True),
    StructField("genre_id", IntegerType(), True)
])

empty_data_fact = []
empty_df_fact = spark.createDataFrame(empty_data_fact, fact_schema)
#empty_df_fact bhanni ley euta empty data frame banaucha jaha fact_schema ma bhayeka fields khali hunchan
empty_df_fact.createOrReplaceTempView("fact_table")

# Insert data into fact_table1 from stage_table and dim_genres
query_insert_fact = """
    SELECT dam.artist_id, s.popularity, dg.genre_id
    FROM stage_table s
    join dim_artist_name dam
    on s.id = dam.artist_id
    JOIN dim_genres dg 
    ON s.genres = dg.genres
"""
spark.sql(query_insert_fact)
# Display the contents of fact_table1
fact_df = spark.sql(query_insert_fact)
fact_df.createOrReplaceTempView("fact_table")
result = spark.sql("select * from fact_table")
result.show()

# dim_df.write.mode("overwrite").parquet("path/to/dim_table_artist")
# fact_df.write.mode("overwrite").parquet("path/to/fact_table_artist")

spark.stop()
