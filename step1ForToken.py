import base64
from config import client_id, client_secret
from requests import post
import json



def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    #string banaidiuncha ani tyo use garchau pachi headers ma
    url = "https://accounts.spotify.com/api/token"
    #this is url that we send request to, token lina
    
    headers = {
        "Authorization": "Basic "+ auth_base64,
        #Basic pachi make sure to leave a space
        "Content-Type": "application/x-www-form-urlencoded"
     
    }
    
    data = {
        "grant_type": "client_credentials",
        
    }
    result = post(url, headers= headers, data= data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token    


def get_auth_token(token):
    return {"Authorization": "Bearer " + token}

    
token = get_token()
get_auth_token = get_auth_token(token)
    



    
        
    