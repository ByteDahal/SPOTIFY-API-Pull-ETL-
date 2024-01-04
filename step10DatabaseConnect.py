from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Set your MySQL database configuration
mysql_config = {
    "url": "jdbc:mysql://localhost:3306/spotifydb3",
    "driver": "com.mysql.cj.jdbc.Driver",  
    "user": "root",
    "password": ""
}

# Create a Spark session
spark = SparkSession.builder \
    .appName("SQLExample") \
    .config("spark.jars", "C:\\mysql-connector-j-8.2.0.jar") \
    .getOrCreate()

# CSV file read and create a temporary raw table
csv_file_path = "z1SearchByArtist.csv"
raw_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
raw_df = raw_df.drop("href").dropna()
raw_df.createOrReplaceTempView("raw_table")

# Create an empty DataFrame with the specified schema for dim_artist_name
schema = StructType([
    StructField("artist_id", StringType(), True),
    StructField("artist_name", StringType(), True)
])
empty_data = []
dim_artist_name_df = spark.createDataFrame(empty_data, schema)
dim_artist_name_df.createOrReplaceTempView("dim_artist_name")

# Query for dim_artist_name
query_dim_artist_name = '''
    SELECT s.id as artist_id, s.artist_name
    FROM raw_table s
    LEFT JOIN dim_artist_name dan
    ON s.id = dan.artist_id
    WHERE dan.artist_id IS NULL
'''
dim_artist_name_df = spark.sql(query_dim_artist_name)
dim_artist_name_df.createOrReplaceTempView("dim_artist_name")

# Create an empty DataFrame with the specified schema for dim_genres
empty_schema_genres = StructType([
    StructField("genre_id", IntegerType(), False),
    StructField("genres", StringType(), True)
])
empty_data_genres = []
dim_genres_df = spark.createDataFrame(empty_data_genres, empty_schema_genres)
dim_genres_df.createOrReplaceTempView("dim_genres")

# Query for dim_genres
query_dim_genres = '''
    SELECT monotonically_increasing_id() as genre_id, s.genres
    FROM raw_table s
    LEFT JOIN dim_genres dg
    ON s.genres = dg.genres
    WHERE dg.genres IS NULL
'''
dim_genres_df = spark.sql(query_dim_genres)
dim_genres_df.createOrReplaceTempView("dim_genres")

# Create an empty DataFrame with the specified schema for fact_table
fact_schema = StructType([
    StructField("artist_id", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("genre_id", IntegerType(), True)
])
empty_data_fact = []
fact_df = spark.createDataFrame(empty_data_fact, fact_schema)
fact_df.createOrReplaceTempView("fact_table")

# Insert data into fact_table from raw_table and dim_genres
query_insert_fact = """
    SELECT dan.artist_id, s.popularity, dg.genre_id
    FROM raw_table s
    JOIN dim_artist_name dan
    ON s.id = dan.artist_id
    JOIN dim_genres dg 
    ON s.genres = dg.genres
"""
fact_df = spark.sql(query_insert_fact)
fact_df.createOrReplaceTempView("fact_table")

# Write to MySQL database
dim_artist_name_df.write.format("jdbc").option("url", mysql_config["url"]) \
    .option("driver", mysql_config["driver"]) \
    .option("dbtable", "dim_artist_name") \
    .option("user", mysql_config["user"]) \
    .option("password", mysql_config["password"]) \
    .mode("overwrite") \
    .save()
    
#mathiko bujham hai-----------------------------------
#.write.format("jdbc"):Specifies that you want to write the DataFrame using the JDBC data source
#option("url", mysql_config["url"]):Sets the JDBC URL for the MySQL database. This URL includes information such as 
# the server address (localhost), port (3306), and the specific database (spotifydb2).
#.option("driver", mysql_config["driver"]):Specifies the JDBC driver class for MySQL. This is necessary for Spark to connect to
# the MySQL database. In this case, the driver is "com.mysql.cj.jdbc.Driver".
#.option("dbtable", "dim_artist_name"):Sets the name of the MySQL table where the data will be written. In this case,
# it's "dim_artist_name".
#.option("user", mysql_config["user"]):Provides the MySQL username. In your configuration, the username is "root".
#.option("password", mysql_config["password"]):Provides the MySQL password. Note that the password is empty in your configuration (""). In a 
# production environment, it's recommended to secure the password.
#.mode("overwrite"):Specifies the mode for writing data. In this case, it's set to "overwrite", meaning that if the table already exists, it 
# will be overwritten.
#.save():Initiates the saving process. The DataFrame will be written to the specified MySQL table using the provided configurations.

dim_genres_df.write.format("jdbc").option("url", mysql_config["url"]) \
    .option("driver", mysql_config["driver"]) \
    .option("dbtable", "dim_genres") \
    .option("user", mysql_config["user"]) \
    .option("password", mysql_config["password"]) \
    .mode("overwrite") \
    .save()

fact_df.write.format("jdbc").option("url", mysql_config["url"]) \
    .option("driver", mysql_config["driver"]) \
    .option("dbtable", "fact_table") \
    .option("user", mysql_config["user"]) \
    .option("password", mysql_config["password"]) \
    .mode("overwrite") \
    .save()


