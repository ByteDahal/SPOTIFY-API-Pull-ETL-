import requests 
import urllib.parse

from flask import Flask, redirect, request, jsonify, session
from datetime import datetime, timedelta


from config import client_id, client_secret
from config import auth_url, token_url, api_base_url

app = Flask(__name__)

app.secret_key = "4324cc4e2ec94affbe260ff07c142a36"
redirect_uri = "http://localhost:7477/callback"

@app.route('/')
def index():
    return"Welcome to my Spotify App<a href = '/login'>Login With Spotify</a>"
#jo aaucha login with spotify button click garesi hamro app ko endpoint ma jancha
#jun aba hami create gardai cham


#lets create a login endpoint ; this is the endpoint where we need to redirect spotify login page
#ani mainly permission anusar endpoints huncha ni feri
#certain endpoints required certain permissions
#we need to request to spotify api saying that the user login spotify through my app
#get these permissions and spotify gives back access token

@app.route('/login')
def login():
    scope = "user-read-private user-read-email"
    
    params = {
        "client_id": client_id,
        "response_type": 'code',
        "scope": scope,
        "redirect_uri": redirect_uri
    }
    global auth_url
    auth_url = f"{auth_url}?{urllib.parse.urlencode(params)}"
    
    return redirect(auth_url)

#aba user ley successfully log in nagarda, 
#they might cancel the process and all 
#testo bela ko redirect lai code garam hai ta i.e spotify call callback endpoint

@app.route('/callback')
def callback():
    if 'error' in request.args:
        return jsonify({"error": request.args['error']})
    #aba access token pauna ko lagi
    if 'code' in request.args:
        req_body = {
            "code": request.args["code"],
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        response = requests.post(token_url, data = req_body)
        #spotify ley token info dincha, we have to convert it into json object
        token_info = response.json()
        
        session["access_token"] = token_info['access_token']#we use to make request to spotify api
        
        session["refresh_token"] = token_info['refersh_token']#refresh token is used when access token gets expired
        #user disruption bina background ma run hunu paryo if access token gets expired and regenerate
        
        session["expires_at"] = datetime.now().timestamp() + token_info['expires_in']
        #yesley current date time bata time liyera, expires_in ko time jodera expire garni time nikalcha
        
        return redirect('/playlists')
        
@app.route('/playlists')
def get_playlists():
    #yo chaleko session ma access_token cha ta check!
    if "access_token" not in session:
        return redirect('/login')
    #yedi access_token ta cha tara expire bhayo ki nai check garni parey
    
    
    if datetime.now().timestamp() > session['expires_at']:
        return redirect('/refresh_token')
    
    headers = {
       "Authorization": f'Bearer {session["access_token"]}'
#postman ma header ma lekhya thyo ni yo
    }

#now create response variable which is going to store the result of making the request
    response = requests. get(api_base_url + 'me/playlists', headers= headers)
    #headers contains authorization token
    #if once we get response well now
    
    playlists = response.json()
    
    return jsonify(playlists)

#aba lets write code for refreshing the token
#its the last endpoint 

@app.route('/refresh_token')
def refresh_token():
    if 'refresh_token' not in session:
       return  redirect('/login')
   
    if datetime.now().timestamp() > session['expires_at']:
        req_body= {
            'grant_type': 'refresh_token',
            'refresh_token': session['refresh_token'],
            "client_id": client_id,
            "client_secret": client_secret
            
        }
        
        response = requests.post(token_url, data = req_body)
        new_token_info = response.json()
        #aba yo new_token_info ko access_token is new access_token of session
        session['access_token'] = new_token_info['access_token']
        
        session["expires_at"] = datetime.now().timestamp() + new_token_info['expires_in']
        
        return redirect('/playlists')
        
        
if __name__ == '__main__':
    app.run(port = 7477, debug = True)