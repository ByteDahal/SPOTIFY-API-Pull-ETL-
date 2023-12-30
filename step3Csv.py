import csv
from step2ForSearch import artists, albums, playlists, track, show, episode

file_path = "z1.csv"
artist_data = artists["artists"]["items"]
field_names1 = ["name", "id", "href", "popularity", "genres"]
with open(file_path, 'w', newline = '', encoding = 'utf-8') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=field_names1)
    csv_writer.writeheader()
    
    for artist in artist_data:
        csv_writer.writerow({
            'name': artist.get('name', ''),
            'id': artist.get('id', ''),
            'href': artist.get('href', ''),
            'popularity': artist.get('popularity', ''),
            'genres': ', '.join(artist.get('genres', [])) if artist.get('genres') else ''

            #yo bhanya genres dherai bhaye comma ley separate garera dekhauni natra khali bhaye '' khali chadni
            
        })
    print("search_for_artist data is written to csv")
    
    
field_names2 = [""]