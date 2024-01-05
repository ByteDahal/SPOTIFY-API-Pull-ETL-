import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


#------------------------------------------CONFIGRATION STAGE------------------------------------------
#mysql database connnection
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="spotify12"
)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

# Set your MySQL database configuration for spark
mysql_config = {
    "url": "jdbc:mysql://localhost:3306/spotify12",
    "driver": "com.mysql.cj.jdbc.Driver",  
    "user": "root",
    "password": ""
}

# Create a Spark session  
spark = SparkSession.builder \
    .appName("SQLExample") \
    .config("spark.jars", "C:\\mysql-connector-j-8.2.0.jar") \
    .getOrCreate()

#csv file path
csv_file_path = "z1SearchByArtist.csv"

#------------------------------------------DATA IMPORT STAGE------------------------------------------
#Create tables in database if it doesnot already exist
def create_table():
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS dim_artist_name (
            artist_id VARCHAR(255),
            artist_name VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_genres (
            genre_id INT AUTO_INCREMENT PRIMARY KEY,
            genres VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_table (
            artist_id INT,
            popularity VARCHAR(255),
            genre_id INT
        );
        """]
    # Execute each CREATE TABLE statement in MySQL
    for query in create_table_queries:
        cursor.execute(query)
    connection.commit()

# Execute the function to create tables in MySQL
create_table()


#get data from DB table and store in temp view
def import_table(table_name):
    spark.read.format("jdbc") \
    .option("url", mysql_config["url"]) \
    .option("driver", mysql_config["driver"]) \
    .option("dbtable", f"{table_name}") \
    .option("user", mysql_config["user"]) \
    .option("password", mysql_config["password"]) \
    .load() \
    .createOrReplaceTempView(table_name)
import_table("dim_artist_name")
import_table("dim_genres")
import_table("fact_table")

# CSV file read and create a temporary raw table
csv_file_path = "z1SearchByArtist.csv"
raw_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
raw_df = raw_df.drop("href").dropna()
raw_df.createOrReplaceTempView("stage_table")

#------------------------------------------TABLE MANIPULATION STAGE------------------------------------------
# Query for dim_artist_name
query_dim_artist_name = '''
    SELECT s.id as artist_id, s.artist_name
    FROM stage_table s
    LEFT JOIN dim_artist_name dan
    ON s.id = dan.artist_id
    WHERE dan.artist_id IS NULL
'''
dim_artist_name_df = spark.sql(query_dim_artist_name)

# Query for dim_genres
query_dim_genre = '''
    SELECT monotonically_increasing_id() as genre_id, s.genres
    FROM stage_table s
    LEFT JOIN dim_genres dg
    ON s.genres = dg.genres
    WHERE dg.genres IS NULL
'''
dim_genre_df = spark.sql(query_dim_genre)

# Insert data into fact_table from stage_table and dim_genres
query_insert_fact = """
    SELECT dan.artist_id, s.popularity, dg.genre_id
    FROM stage_table s
    JOIN dim_artist_name dan
    ON s.id = dan.artist_id
    JOIN dim_genres dg 
    ON s.genres = dg.genres
"""
fact_df = spark.sql(query_insert_fact)

#------------------------------------------TABLE INSERTION STAGE------------------------------------------
# Write to MySQL database
def insert_into_db(df_name,table_name):
    df_name.write.format("jdbc").option("url", mysql_config["url"]) \
        .option("driver", mysql_config["driver"]) \
        .option("dbtable",table_name) \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .mode("overwrite") \
        .save()
insert_into_db(dim_artist_name_df,"dim_artist_name")
insert_into_db(dim_genre_df,"dim_genres")
insert_into_db(fact_df,"fact_table")