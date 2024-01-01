import mysql.connector
import csv

class SPOTIFY( ):
    def __init__(self, host, user, password, database):
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        ) 
        self.cursor = self.conn.cursor()
         
    def create_database(self, db_name):
        create_database_query = f'create database if not exists {db_name}'
        self.cursor.execute(create_database_query)
        self.conn.commit()
        
    def connect_to_database(self, db_name):
        self.conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database=db_name
        )
        self.cursor = self.conn.cursor()
    
    def create_stage_table(self, create_stage_table_query):
        self.cursor.execute(create_stage_table_query)
        self.conn.commit()
        
    
    def load_data_from_csv(self, csv_file_path, table_name):
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            data_list = list(csv_reader)

        for row in data_list:
            artist_name_artist = row["artist_name_artist"]
            id = row["id"]
            populartity_artist = row["populartity_artist"]
            genres = row["genres"]
            album_name_albums = row["album_name_albums"]
            artist_name_albums = row["artist_name_albums"]
            release_year_albums = row["release_year_albums"]

            self.cursor.execute(f'''
                INSERT INTO {table_name} (
                    artist_name_artist, id, populartity_artist, genres,
                    album_name_albums, artist_name_albums, release_year_albums
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            ''', (
                artist_name_artist, id, populartity_artist, genres,
                album_name_albums, artist_name_albums, release_year_albums
            ))

            self.conn.commit()

    def dim_artist(self):
        query = '''
            INSERT INTO dim_artist (artist_id, artist_name_artist)
            SELECT s.id, s.artist_name_artist
            FROM stage_table s
            LEFT JOIN dim_artist da ON s.id = da.artist_id 
            WHERE da.artist_id IS NULL
            GROUP BY s.id, s.artist_name_artist;
        '''
        self.cursor.execute(query)
        self.conn.commit()
        
    def dim_genre(self):
        query = '''insert into dim_genre (genres)
        select s.genres 
        from stage_table s
        left join dim_genre dg 
        on s.genres = dg.genres
        where dg.genres is null;
        '''
        self.cursor.execute(query)
        self.conn.commit()
        
    def dim_album(self):
        query = '''insert into dim_album(album_name_albums)
        select s.album_name_albums
        from stage_table s
        left join dim_album dal
        on s.album_name_albums = dal.album_name_albums
        where dal.album_name_albums is null;
        '''
        self.cursor.execute(query)
        self.conn.commit()
        
    def create_fact_table(self, fact_table_query):
        self.cursor.execute(fact_table_query)
        self.conn.commit()
        
    def insert_into_fact_table(self, table_name):
        query = f''' 
            INSERT INTO {table_name} (artist_id, popularity_artist, genre_id, album_id, release_year_albums)
            SELECT s.id, s.populartity_artist, dg.genre_id, dal.album_id, s.release_year_albums
            FROM stage_table s
            JOIN dim_genre dg ON s.genres = dg.genres
            JOIN dim_album dal ON s.album_name_albums = dal.album_name_albums
        '''
        self.cursor.execute(query)
        self.conn.commit()
