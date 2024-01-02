from step4Pyspark import *
result_df = df_artist \
    .join(df_album, col("artist_name_artist") == col("artist_name_albums"), "left") \
    .join(df_playlist, col("artist_name_artist") == col("playlist_name"), "left") \
    .join(df_track, col("artist_name_artist") == col("artist_names"), "left")

result_df.write.csv('z7Combined_data.csv', header=True, mode='overwrite')


#----------------------------Explain-------------------------
# df_artist \
#     .join(df_album, col("artist_name_artist") == col("artist_name_albums"), "left") \
#     .join(df_playlist, col("artist_name_artist") == col("playlist_name"), "left") \
#     .join(df_track, col("artist_name_artist") == col("artist_names"), "left")
#---suruma artist ra album ko left join based on matching column
#---ani tes bata aayeko result ra playlist ko left join  based on matching column
#---ani tesbata aayeko result ra track ko left join track ko based on matching column
# Assuming 'result_df' is your DataFrame


result_df = result_df.drop("playlist_name","owner_name","total_track","track_name","artist_names","album_name","available_markets","total_tracks","duration_ms","populartity_track","release_year_tracks")
#mathiko columns haruma null value thiye so hataye
result_df = result_df.dropna()
#aba row hataucha null bhayeka
result_df.write.csv('z7Combined_data.csv', header=True, mode='overwrite')

