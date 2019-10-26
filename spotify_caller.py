import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyAPI:
    # create connection to Spotify API
    def __init__(self):
        with open('credential.json') as f:
            data = json.load(f)
            client_id = data['SPOTIFY_CLIENT_ID']
            client_secret = data['SPOTIFY_CLIENT_SECRET']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # extract artist name by track id
    def get_track_information(self,id):
        try:
            artist = self.sp.track(id)['artists'][0]['name']
            return artist
        except:
            return None
