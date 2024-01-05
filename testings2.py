from step10DatabaseConnect import spark, mysql_config

# Function to import a table and convert it into a DataFrame
def import_table_to_df(table_name):
    df = spark.read.format("jdbc") \
        .option("url", mysql_config["url"]) \
        .option("driver", mysql_config["driver"]) \
        .option("dbtable", f"{table_name}") \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .load()
    return df

# Import dim_artist_name_generator table
dim_artist_name_generator_df = import_table_to_df("dim_artist_name_generator")

# Import dim_genres_generator table
dim_genre_generator_df = import_table_to_df("dim_genres_generator")

# Create a temporary view instead of a temporary table
temp_dim_artist_name_table_query = '''
CREATE TEMPORARY VIEW temp_dim_artist_name_table
AS 
SELECT s.id AS artist_id 
FROM stage_table s
GROUP BY s.id
'''
spark.sql(temp_dim_artist_name_table_query)
spark.sql("SELECT * FROM temp_dim_artist_name_table").show()


query_dim_artist_name = '''
    SELECT tda.id as artist_id
    FROM temp_dim_artist_name_table tda
    LEFT JOIN dim_artist_name dan
    ON tda.id = dan.artist_id
    WHERE dan.artist_id IS NULL
'''

spark.sql(query_dim_artist_name).show()
