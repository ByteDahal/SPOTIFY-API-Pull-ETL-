from dotenv import load_dotenv
import os
from step6ClassAndInsert import SPOTIFY

load_dotenv("config.env")

host = os.getenv('HOST', 'localhost')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
database = os.getenv('DATABASE')

spy = SPOTIFY(host, user, password, database)

spy.create_database(database)

spy.connect_to_database(database)
create_stage_table_query = '''
CREATE TABLE IF NOT EXISTS stage_table (
    artist_name_artist VARCHAR(255),
    id VARCHAR(255),
    populartity_artist INT,
    genres VARCHAR(255),
    album_name_albums VARCHAR(255),
    artist_name_albums VARCHAR(255),
    release_year_albums INT
);
'''

spy.create_stage_table(create_stage_table_query)

csv_file_path = 'z7Combined_data3.csv'
table_name = 'stage_table'
spy.load_data_from_csv(csv_file_path, table_name)

# delete_csv_file_path = 'stage_table.csv'

# spy.delete_data(delete_csv_file_path, table_name)

# Load new data from CSV
# new_csv_file_path = 'z7Combined_data3.csv'

# spy.load_data_from_csv(new_csv_file_path, table_name)

spy.dim_artist()
spy.dim_album()
spy.dim_genre()
