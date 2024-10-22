from snowflake.connector.pandas_tools import write_pandas
from flask import Flask, request, jsonify
import snowflake.connector
import pandas as pd
import json
import os
import firebase_admin
from firebase_admin import credentials, auth, firestore
from dotenv import load_dotenv
import requests
import time
import sys

load_dotenv()

# Load the Firebase service account key from environment variable
firebase_service_account_key = json.loads(os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY"))
forth_api_key = json.loads(os.getenv("FORTH_API_KEY"))
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
CHANNEL_ID = os.getenv('CHANNEL_ID')

# Initialize Firebase Admin SDK
cred = credentials.Certificate(firebase_service_account_key)
firebase_admin.initialize_app(cred)
db = firestore.client()

# Function to verify Firebase ID token
def verify_firebase_token(token):
    try:
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception as e:
        print(e)
        return None

# Function to check user and contact id in Firestore
def check_user_contact(uid, contact_id):
    try:
        user_doc = db.collection('users_test').document(uid).get()
        if user_doc.exists:
            if user_doc.get('contact_id') == str(contact_id):
                return True
        return False

    except Exception as e:
        print(f"Error: {e}")
        return False

def authenticate(token, contact_id, check_contact_id):
    decoded_token = verify_firebase_token(token)
    
    if not decoded_token:
        return 401

    uid = decoded_token.get('uid')
    if check_contact_id and not check_user_contact(uid, contact_id):
        return 401
    return 200

# Function to pull data from Snowflake
def snow_data_pull(sql_statement, snowflake_instance):
    conn = snowflake.connector.connect(
        user=os.getenv(f'{snowflake_instance}_snow_username'.upper()),
        password=os.getenv(f'{snowflake_instance}_snow_password'.upper()),
        account=os.getenv(f'{snowflake_instance}_snow_account'.upper()),
        warehouse=os.getenv(f'{snowflake_instance}_snow_warehouse'.upper()),
        role=os.getenv(f'{snowflake_instance}_snow_role'.upper())
    )
    cur = conn.cursor()
    cur.execute(sql_statement)
    df = cur.fetch_pandas_all()
    return df

def request_check(headers, json_body, endpoint):
    code = None
    code_discription = None
    lookup_field = None
    lookup_value = None
    contact_id = None

    headers = {key: value for key, value in headers.items()}
    if (endpoint == 'get_payment_plan') or (endpoint == 'get_debts'):
        if 'contact_id' not in json_body:
            code = 400
            code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
        try:
            contact_id = int(json_body['contact_id'])
        except:
            code = 400
            code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
    if (endpoint == 'get_contact') and ('lookup_field' not in json_body) and ('lookup_value' not in json_body):
        code = 400
        code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
    if (endpoint == 'get_contact') and ('lookup_field' in json_body) and ('lookup_value' in json_body):
        lookup_field = json_body['lookup_field']
        lookup_value = json_body['lookup_value']
        field_confirmed = False
        for field in ['contact_id', 'ssn', 'hash_value']:
            if (field == lookup_field) and (lookup_value != '*') and (lookup_value != '_') and (lookup_value != '%'):
                field_confirmed = True
        if not field_confirmed:
            code = 400
            code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
    if(authenticate(headers.get('Authorization', ''), contact_id, not(endpoint == 'get_contact')) == 401):
        code = 401
        code_discription = 'Unauthorized: The client must authenticate itself to get the requested response.'
    else:
        code = 200
        code_discription = 'OK: The request was successful.'
    return headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value

app = Flask(__name__)
@app.route('/get_contact', methods=['POST'])
def get_contact():
    headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value = request_check(request.headers, request.get_json(), 'get_contact')
    if code != 200:
        return jsonify({"Response": code_discription}), code
    json_data = []
    attempt = 0
    attempts = 10

    while(len(json_data) == 0 and attempt < attempts):
        df = snow_data_pull(f"SELECT * FROM KORE_AI.DATA.TBL_CONTACT_ID_LIST WHERE {lookup_field} = '{lookup_value}'", 'ENCS')
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.05)

    if(len(json_data) == 0):
        code = 404

    print(f'returning in get contact after {attempt} attempts with length {len(json_data)}: {json_data}', file=sys.stderr)

    return json_data, code

@app.route('/get_payment_plan', methods=['POST'])
def get_payment_plan():
    headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value = request_check(request.headers, request.get_json(), 'get_payment_plan')
    if code != 200:
        return jsonify({"Response": code_discription}), code
    json_data = []
    attempt = 0
    attempts = 10

    while(len(json_data) == 0 and attempt < attempts):
        df = snow_data_pull(f'SELECT * FROM KORE_AI.DATA.TBL_PAYMENT_PLAN2 WHERE CONTACT_ID = {contact_id}', 'ENCS')
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.05)

    if(len(json_data) == 0):
        code = 404

    return json_data, code

@app.route('/get_debts', methods=['POST'])
def get_debts():
    headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value = request_check(request.headers, request.get_json(), 'get_debts')
    if code != 200:
        return jsonify({"Response": code_discription}), code
    json_data = []
    attempt = 0
    attempts = 10

    while(len(json_data) == 0 and attempt < attempts):
        df = snow_data_pull(f'SELECT * FROM KORE_AI.DATA.TBL_DEBTS WHERE CONTACT_ID = {contact_id}', 'ENCS')
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.05)

    if(len(json_data) == 0):
        code = 404

    return json_data, code
cached_videos = None
last_fetch_time = 0
CACHE_EXPIRATION = 3600  # 1 hour in seconds

@app.route('/videos', methods=['GET'])
def get_videos():
    global cached_videos, last_fetch_time

    # Check if the cache is still valid (within 1 hour)
    current_time = time.time()
    if cached_videos and (current_time - last_fetch_time) < CACHE_EXPIRATION:
        return jsonify({'videos': cached_videos}), 200

    # Step 1: Retrieve the playlist ID for the channel's uploaded videos
    channel_url = 'https://www.googleapis.com/youtube/v3/channels'
    channel_params = {
        'part': 'contentDetails',
        'id': CHANNEL_ID,
        'key': YOUTUBE_API_KEY,
    }
    channel_response = requests.get(channel_url, params=channel_params)
    if channel_response.status_code != 200:
        print(f'error: {channel_response.json()}', file=sys.stderr)
        return jsonify({'error': 'Failed to fetch channel details'}), 500
    channel_data = channel_response.json()
    playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Step 2: Retrieve videos from the playlist
    playlist_items_url = 'https://www.googleapis.com/youtube/v3/playlistItems'
    playlist_params = {
        'part': 'snippet',
        'playlistId': playlist_id,
        'maxResults': 10,  # Adjust as needed
        'key': YOUTUBE_API_KEY,
    }
    playlist_response = requests.get(playlist_items_url, params=playlist_params)
    if playlist_response.status_code != 200:
        return jsonify({'error': 'Failed to fetch playlist items'}), 500
    playlist_data = playlist_response.json()

    # Parse the JSON response to extract video details
    videos = []
    for item in playlist_data.get('items', []):
        video = {
            'videoId': item['snippet']['resourceId']['videoId'],
            'title': item['snippet']['title'],
            'description': item['snippet']['description'],
            'thumbnail': item['snippet']['thumbnails']['high']['url'],
            'publishedAt': item['snippet']['publishedAt'],
        }
        videos.append(video)

    # Update the cache
    cached_videos = videos
    last_fetch_time = current_time

    # Return the list of videos as JSON
    return jsonify({'videos': videos}), 200
if __name__ == '__main__':
    app.run(debug = False,host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))