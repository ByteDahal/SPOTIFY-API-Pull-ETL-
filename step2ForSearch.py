from step1ForToken import token
from requests import post, get
import json
import csv


def get_auth_token(token):
    return {"Authorization": "Bearer " + token}
    
url = "https://api.spotify.com/v1/search"
headers = get_auth_token(token)

def search_for_artist(ids):
    #documentation ma search by item ma gayera herni
    query = f'?q={ids}&type=artist'
    #yedi artist matrai haina track ni chaiye comma ley separate like artist, track, playlist and so on
    #limit = 1 ley pailo artist that pop out dekhaucha
    query_url = url + query
    #mathi ko q agadiko ? yesma + lekhera joddda pani hunthyo
    result = get(query_url, headers = headers)
    #get request garya(yaad cha ni postman wala? same)
    json_result = json.loads(result.content)
    #result ko content json string ma huncha teslai python dictionary ma lagni
    return json_result


def search_for_album(ids):
    query = f'?q={ids}&type=album'
    query_url = url + query
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result

def search_for_playlist(ids):
    query = f'?q={ids}&type=playlist'
    query_url = url + query
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result

def search_for_track(ids):
    query = f'?q={ids}&type=track'
    query_url = url + query
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result

def search_for_show(ids):
    query = f'?q={ids}&type=show'
    query_url = url + query
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result
    
def search_for_episode(ids):
    query = f'?q={ids}&type=episode'
    query_url = url + query
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result    



ids = "remaster%2520track%3ADoxy%2520artist%3AMiles%2520Davis"  
#yo encoded ho ; it means track = doxy and artist = miles
artists = search_for_artist(ids)
albums = search_for_album(ids)
playlists = search_for_playlist(ids)
track = search_for_track(ids)
show = search_for_show(ids)
episode = search_for_episode(ids)



    


