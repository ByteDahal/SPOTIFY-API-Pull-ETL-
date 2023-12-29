import requests
import urllib.parse
from flask import Flask, redirect, request, jsonify, session, redirect
from flask_restful import Api, Resource

from datetime import datetime, timedelta
from config import client_id, client_secret
from config import auth_url, token_url, api_base_url

app = Flask(__name__)
api = Api(app)

redirect_uri = "http://localhost:7479/callback"
app.secret_key = "4324cc4e2ec94affbe260ff07c142a36"

class Index(Resource):
    def get(self):
        return "Welcome to my Spotify App <a href='/login'>Login With Spotify</a>"
        return redirect('/login')

class Login(Resource):
    def get(self):
        scope = "user-read-private user-read-email"
        params = {
            "client_id": client_id,
            "response_type": 'code',
            "scope": scope,
            "redirect_uri": redirect_uri
        }
        auth_url_with_params = f"{auth_url}?{urllib.parse.urlencode(params)}"
        return redirect(auth_url_with_params)

class Callback(Resource):
    def get(self):
        if 'error' in request.args:
            return jsonify({"error": request.args['error']})

        if 'code' in request.args:
            req_body = {
                "code": request.args["code"],
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret
            }

            response = requests.post(token_url, data=req_body)
            token_info = response.json()

            session["access_token"] = token_info['access_token']
            session["refresh_token"] = token_info['refresh_token']
            session["expires_at"] = datetime.now().timestamp() + token_info['expires_in']

            return redirect('/playlists')

class Playlists(Resource):
    def get(self):
        if "access_token" not in session:
            return redirect('/login')

        if datetime.now().timestamp() > session['expires_at']:
            return redirect('/refresh_token')

        headers = {
            "Authorization": f'Bearer {session["access_token"]}'
        }

        response = requests.get(api_base_url + 'me/playlists', headers=headers)
        playlists = response.json()
        return jsonify(playlists)

class RefreshToken(Resource):
    def get(self):
        if 'refresh_token' not in session:
            return redirect('/login')

        if datetime.now().timestamp() > session['expires_at']:
            req_body = {
                'grant_type': 'refresh_token',
                'refresh_token': session['refresh_token'],
                "client_id": client_id,
                "client_secret": client_secret
            }

            response = requests.post(token_url, data=req_body)
            new_token_info = response.json()
            session['access_token'] = new_token_info['access_token']
            session["expires_at"] = datetime.now().timestamp() + new_token_info['expires_in']

            return redirect('/playlists')

api.add_resource(Index, '/')
api.add_resource(Login, '/login')
api.add_resource(Callback, '/callback')
api.add_resource(Playlists, '/playlists')
api.add_resource(RefreshToken, '/refresh_token')

if __name__ == '__main__':
    app.run(host='localhost', port=7479, debug=True)