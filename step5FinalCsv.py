from step4Pyspark import *
result = df_artist \
    .join(df_album, col("artist_name_artist") == col("artist_name_albums"), "left") \
    .join(df_playlist, col("artist_name_artist") == col("playlist_name"), "left") \
    .join(df_track, col("artist_name_artist") == col("artist_names"), "left")

result.show()

result.write.csv('z7Combined_data.csv', header=True, mode='overwrite')

spark.stop()
#----------------------------Explain-------------------------
# df_artist \
#     .join(df_album, col("artist_name_artist") == col("artist_name_albums"), "left") \
#     .join(df_playlist, col("artist_name_artist") == col("playlist_name"), "left") \
#     .join(df_track, col("artist_name_artist") == col("artist_names"), "left")
#---suruma artist ra album ko left join based on matching column
#---ani tes bata aayeko result ra playlist ko left join  based on matching column
#---ani tesbata aayeko result ra track ko left join track ko based on matching column