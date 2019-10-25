import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyAPI:
    def __init__(self):
        with open('credential.json') as f:
            data = json.load(f)
            client_id = data['SPOTIFY_CLIENT_ID']
            client_secret = data['SPOTIFY_CLIENT_SECRET']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager)
    
    def get_track_information(self,id):
        return self.sp.track('3eHkFA3StDR9BU7EVrUFLs')['artists'][0]['name']