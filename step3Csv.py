import csv
from step2ForSearch import artists, albums, playlists, tracks, shows, episodes

file_path = "z1.csv"
artist_data = artists["artists"]["items"]
#agadi bata aayeko artists ley display garauni data bhitra ko artist ani artist tyo bhitra ko item(curly braces bhitrako)
field_names1 = ["artist_name", "id", "href", "popularity", "genres"]
with open(file_path, 'w', newline = '', encoding = 'utf-8') as csv_file1:
    csv_writer = csv.DictWriter(csv_file1, fieldnames=field_names1)
    csv_writer.writeheader()
    
    for artist in artist_data:
        csv_writer.writerow({
            'artist_name': artist.get('name', ''),
            #name bhaye pathauni natra empty''
            'id': artist.get('id', ''),
            'href': artist.get('href', ''),
            'popularity': artist.get('popularity', ''),
            'genres': ', '.join(artist.get('genres', [])) if artist.get('genres') else ''
            #yo bhanya genres dherai bhaye comma ley separate garera dekhauni natra khali bhaye '' khali chadni
            
        })
    print("search_for_artist data is written to csv")
    
file_path2 = "z2.csv"    
field_names2 = ["album_name", "artist_name", "release_date", "spotify_url"]
album_data = albums["albums"]["items"]
with open(file_path2, 'w', newline = '', encoding = "utf-8") as csv_file2:
    csv_writer = csv.DictWriter(csv_file2, fieldnames=field_names2)
    csv_writer.writeheader()
    
    for album in album_data:
        csv_writer.writerow({
            'album_name': album.get("name", ''),
            "artist_name": album.get("artists", [{}])[0].get("name", ""),
            #artists ko pailo index lai lini tesko name lini
            "release_date": album.get("release_date", ''),
            "spotify_url": album.get("external_urls", {}).get("spotify", "")
            #external_url bhaye tyo bhitra ko spotify ko dini natra {};empty
            #if spotify ni nabhaye ''
        })
    print("search_for_album data is written to csv")    
    
file_path3 = "z3.csv"
field_names3 = ["playlist_name", "owner_name", "total_track", "playlist_url"]
playlists_data = playlists["playlists"]["items"]
with open(file_path3, 'w', newline = '', encoding="utf-8") as csv_file3:
    csv_writer = csv.DictWriter(csv_file3, fieldnames=field_names3)
    csv_writer.writeheader()
    
    for playlist in playlists_data:
        csv_writer.writerow({
            "playlist_name": playlist.get("name",''),
            "owner_name": playlist.get("owner", {}).get("display_name",''),
            "total_track": playlist.get("tracks", {}).get("total", 0),
            #total nabhaye 0
            "playlist_url": playlist.get("external_urls", {}).get("spotify",'')
        })
    print("search_for_playlists is written to csv")


file_path4 = "z4.csv"
field_names4 = ['track_name', 'artist_names', 'album_name', 'available_markets', 'release_date', 'total_tracks', 'duration_ms', 'popularity', 'preview_url', 'track_url']
track_data = tracks['tracks']['items']
with open(file_path4, 'w', newline = '', encoding="utf-8") as csv_file4:
    csv_writer = csv.DictWriter(csv_file4, fieldnames=field_names4)
    csv_writer.writeheader()
    
    for track in track_data:
        csv_writer.writerow({
            "track_name": track.get('name', ''),
            "artist_names": [artist['name'] for artist in track['artists']],
            "album_name": track.get('album', {}).get('name', ''),
            'available_markets': ', '.join(track.get('available_markets', [])),
            "release_date": track.get('album', {}).get('release_date', ''),
            "total_tracks": track.get('album', {}).get('total_tracks', 0),
            "duration_ms": track.get('duration_ms', 0),
            "popularity": track.get('popularity', 0),
            "preview_url": track.get('preview_url', ''),
            "track_url": track.get('external_urls', {}).get('spotify', '')

        })
    print("search_for_tracks is written to csv")
    

file_path5 = "z5.csv"
field_names5 = ['items', 'total']
shows_data = shows["shows"]

with open(file_path5, 'w', newline='', encoding="utf-8") as csv_file5:
    csv_writer = csv.DictWriter(csv_file5, fieldnames=field_names5)
    csv_writer.writeheader()

    csv_writer.writerow({
        'items': shows_data.get("items", ''),
        'total': shows_data.get("total", '')
    })

    print("search_for_show is written to csv")
    
    
file_path5 = "z6.csv"
field_names5 = ['items', 'total']
episodes_data = episodes["episodes"]

with open(file_path5, 'w', newline='', encoding="utf-8") as csv_file5:
    csv_writer = csv.DictWriter(csv_file5, fieldnames=field_names5)
    csv_writer.writeheader()

    csv_writer.writerow({
        'items': episodes_data.get("items", ''),
        'total': episodes_data.get("total", '')
    })

    print("search_for_episode is written to csv")