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

# CSV file read and create a temporary raw table
csv_file_path = "z1SearchByArtist.csv"
raw_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
raw_df = raw_df.drop("href").dropna()
raw_df.createOrReplaceTempView("stage_table")
spark.sql("select * from stage_table").show()

#------------------------------------------DATA IMPORT STAGE------------------------------------------
# Create tables in the database if they do not already exist
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
            artist_id varchar(255),
            popularity VARCHAR(255),
            genre_id INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_artist_name_generator(
            artist_id VARCHAR(255)
        );
        """
    ]
    # Execute each CREATE TABLE statement in MySQL
    for query in create_table_queries:
        cursor.execute(query)

# Execute the function to create tables in MySQL
create_table()


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
    
import_table("dim_artist_name_generator")
import_table("dim_artist_name")
import_table("dim_genres")
import_table("fact_table")


#------------------------------------------TABLE MANIPULATION STAGE------------------------------------------
# Query for creating a temporary table if it doesn't exist
temp_dim_artist_name_table_query = '''
SELECT s.id AS artist_id, s.artist_name AS artist_name
FROM stage_table s
GROUP BY s.id, s.artist_name
'''

temp_dim_artist_name_table_df = spark.sql(temp_dim_artist_name_table_query)
temp_dim_artist_name_table_df.createOrReplaceTempView("temp_dim_artist_name_table")
temp_dim_artist_name_table_df.show()

# query for dim artist generator
query_dim_artist_name_generator = '''
    SELECT tda.artist_id
    FROM temp_dim_artist_name_table tda
    LEFT JOIN dim_artist_name_generator dang
    ON tda.artist_id = dang.artist_id
    WHERE dang.artist_id IS NULL
'''
dim_artist_name_generator_df = spark.sql(query_dim_artist_name_generator)
dim_artist_name_generator_df.createOrReplaceTempView("temp_dim_artist_name_generator")
dim_artist_name_generator_df.show()


try:
    delete_query = '''
    DELETE FROM dim_artist_name
    WHERE artist_id IN (SELECT artist_id FROM temp_dim_artist_name_generator)
    '''
    spark.sql(delete_query)
except :
    # Handle a specific exception
    print("First time, so there is no data to delete from dim_artist_name")


query_dim_artist_name = '''
select tda.artist_id, tda.artist_name
from temp_dim_artist_name_table tda
inner join temp_dim_artist_name_generator dang
on tda.artist_id = dang.artist_id
'''
dim_artist_name_df = spark.sql(query_dim_artist_name)
dim_artist_name_df.createOrReplaceTempView("dim_artist_name")
dim_artist_name_df.show()

# Query for dim_genres
query_dim_genres = '''
    SELECT monotonically_increasing_id() as genre_id, s.genres
    FROM stage_table s
    LEFT JOIN dim_genres dg
    ON s.genres = dg.genres
    WHERE dg.genres IS NULL
'''
dim_genre_df = spark.sql(query_dim_genres)
dim_genre_df.createOrReplaceTempView("dim_genres")

# Insert data into fact_table from stage_table and dim_genres
# Insert data into fact_table from stage_table and dim_genres
query_insert_fact = """
    SELECT dan.artist_id, MAX(s.popularity) as popularity, MAX(dg.genre_id) as genre_id
    FROM stage_table s
    JOIN dim_artist_name dan
    ON s.id = dan.artist_id
    JOIN dim_genres dg 
    ON s.genres = dg.genres
    GROUP BY dan.artist_id, popularity, genre_id
"""
fact_df = spark.sql(query_insert_fact)
fact_df.show()



#------------------------------------------TABLE INSERTION STAGE------------------------------------------
# Write to MySQL database
def insert_into_db(df_name, table_name, mode="overwrite"):
    df_name.write.format("jdbc").option("url", mysql_config["url"]) \
        .option("driver", mysql_config["driver"]) \
        .option("dbtable", table_name) \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .mode(mode) \
        .save()
    print(f"Data successfully inserted into {table_name} table.")

# Use append mode for updating existing records
insert_into_db(fact_df, "fact_table", "append")
insert_into_db(dim_genre_df, "dim_genres", "append")
insert_into_db(dim_artist_name_df, "dim_artist_name", "append")
insert_into_db(dim_artist_name_generator_df, "dim_artist_name_generator", "append")

