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

table_name = 'stage_table'
folder_path = "z7Combined_data"

# CSV extension bhako list of files rakhni
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

if not csv_files:
    raise ValueError("No CSV files found in the specified folder")

# Surukai csv chaiyo rey
csv_file_name = csv_files[0]
file_path = r"C:\Users\amrit\OneDrive\Desktop\InternGBD\SPOTIFY API Pull\z7Combined_data"
csv_file_path = f"{file_path}\\{csv_file_name}"
spy.load_data_from_csv(csv_file_path, table_name)

spy.dim_artist()
spy.dim_album()
spy.dim_genre()
