from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

# Create a Spark session
spark = SparkSession.builder.appName("CSVConversion").getOrCreate()

# Load CSV files into DataFrames
df_artist = spark.read.csv("z1SearchByArtist.csv", header=True)
df_album = spark.read.csv("z2SearchByAlbum.csv", header=True)
df_playlist = spark.read.csv("z3SearchByPlaylist.csv", header=True)
df_track = spark.read.csv("z4SearchByTrack.csv", header=True)
df_show = spark.read.csv("z5SearchByShow.csv", header=True)
df_episodes = spark.read.csv("z6SearchBySpisode.csv", header=True)

#----------------------------------------------------for df_artist---------------------------------------------------------------------------
df_artist = df_artist.drop("href")
print("df_artist pyspark manipulation completed")
df_album.drop("spotify_url")
df_artist = df_artist.dropna()
df_album.show()

# ----------------------------------------------------for df_album----------------------------------------------------------------------------

df_album = df_album.drop("spotify_url")
df_album = df_album.withColumn(
    "release_year",
         substring(df_album["release_date"], 1, 4)
    )
    #naya column banyo with name "release_year"
    #(1,4) bhanya pailo position ma bhako 4 ota value lini bhanya as yearcontains 4 so; substring ley 4 ota string liyo .....
df_album = df_album.drop("release_date")
    #aba drop garya release_date bhanni column
    
df_album = df_album.dropna()
df_album.show()


#-----------------------------------------------------for df_playlist------------------------------------------------------------------------

df_playlist = df_playlist.drop("playlist_url")
df_playlist = df_playlist.filter(col("owner_name") != "Spotify")
    #owner_name : spotify nabhako rakhya matlab spotify bhako row hatako
df_playlist = df_playlist.dropna()
df_playlist.show()

#-----------------------------------------------------for df_track--------------------------------------------------------------------------

df_track = df_track.withColumn(
    "release_year",
    substring(df_track["release_date"], 1, 4)
)

df_track = df_track.drop("preview_url", "release_date", "track_url")
df_track = df_track.dropna()
df_track.show()

