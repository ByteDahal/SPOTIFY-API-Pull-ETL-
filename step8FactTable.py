from step7StageDimTable import spy
fact_table_query = '''
CREATE TABLE IF NOT EXISTS fact_table(
   artist_id varchar(255),
   popularity_artist int,
   genre_id int,
   album_id int,
   release_year_albums int,
   FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id),
   FOREIGN KEY (album_id) REFERENCES dim_album(album_id)
);
'''

spy.create_fact_table(fact_table_query)
table_name = 'fact_table'
spy.insert_into_fact_table(table_name)
