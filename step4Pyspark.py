from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

# Spark session banako
spark = SparkSession.builder.appName("CSVConversion").getOrCreate()

#dataframe ma csv load garya
df_artist = spark.read.csv("z1SearchByArtist.csv", header=True)
df_album = spark.read.csv("z2SearchByAlbum.csv", header=True)
df_playlist = spark.read.csv("z3SearchByPlaylist.csv", header=True)
df_track = spark.read.csv("z4SearchByTrack.csv", header=True)

#-----------------------------------------------------for df_artist------------------------------------------------------------------------
df_artist = df_artist.withColumnRenamed("artist_name", "artist_name_artist")
df_artist = df_artist.withColumnRenamed("popularity", "populartity_artist")
df_artist = df_artist.drop("href")
df_artist = df_artist.dropna()

#-----------------------------------------------------for df_album------------------------------------------------------------------------
df_album = df_album.withColumnRenamed("artist_name", "artist_name_albums")
df_album = df_album.withColumnRenamed("release_date", "release_date_albums")
df_album = df_album.withColumnRenamed("album_name", "album_name_albums")
df_album = df_album.drop("spotify_url")
df_album = df_album.withColumn(
    "release_year_albums",
         substring(df_album["release_date_albums"], 1, 4)
    )
    #naya column banyo with name "release_year"
    #(1,4) bhanya pailo position ma bhako 4 ota value lini bhanya as yearcontains 4 so; substring ley 4 ota string liyo .....
df_album = df_album.drop("release_date_albums")
    #aba drop garya release_date bhanni column
    
df_album = df_album.dropna()

#-----------------------------------------------------for df_playlist------------------------------------------------------------------------
df_playlist = df_playlist.drop("playlist_url")
df_playlist = df_playlist.filter(col("owner_name") != "Spotify")
    #owner_name : spotify nabhako rakhya matlab spotify bhako row hatako
df_playlist = df_playlist.dropna()
df_playlist = df_playlist.withColumnRenamed("artist_name", "artist_name_playlists")

#-----------------------------------------------------for df_track------------------------------------------------------------------------

df_track = df_track.withColumn(
    "release_year",
    substring(df_track["release_date"], 1, 4)
)

df_track = df_track.drop("preview_url", "release_date", "track_url")
df_track = df_track.dropna()
df_track = df_track.withColumnRenamed("artist_name", "artist_name_tracks")
df_track = df_track.withColumnRenamed("popularity", "populartity_track")
df_track = df_track.withColumnRenamed("release_year", "release_year_tracks")


