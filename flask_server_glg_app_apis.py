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
from datetime import datetime

load_dotenv(override=True)

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
        if user_doc.exists and user_doc.get('contact_id') == str(contact_id):
            return True
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def authenticate(token, contact_id, check_contact_id):
    decoded = verify_firebase_token(token)
    print(f'Decoded token: {decoded}')
    print('Contact ID:', contact_id)
    if not decoded:
        return 401
    uid = decoded.get('uid')
    if check_contact_id and not check_user_contact(uid, contact_id):
        return 401
    return 200

def snow_data_pull(sql_statement, snowflake_instance):
    conn = cur = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv(f'{snowflake_instance}_snow_username'.upper()),
            password=os.getenv(f'{snowflake_instance}_snow_password'.upper()),
            account=os.getenv(f'{snowflake_instance}_snow_account'.upper()),
            warehouse=os.getenv(f'{snowflake_instance}_snow_warehouse'.upper()),
            role=os.getenv(f'{snowflake_instance}_snow_role'.upper())
        )
        cur = conn.cursor()
        cur.execute(sql_statement)
        # If this is a SELECT, fetch and return a DataFrame:
        if sql_statement.strip().split()[0].upper() == 'SELECT':
            return cur.fetch_pandas_all()
        # Otherwise, return affected rows
        return cur.rowcount
    except Exception as e:
        print(f"Error in snow_data_pull: {e}", file=sys.stderr)
        return None
    finally:
        if cur: cur.close()
        if conn: conn.close()

# === Modular request validator ===

class ValidationError(Exception):
    pass

def validate_lookup_field(field, value):
    allowed = {'contact_id', 'ssn', 'hash_value'}
    if field not in allowed or value in {'*', '_', '%'}:
        raise ValidationError(f"Invalid lookup: {field} = '{value}'")

ENDPOINT_RULES = {
    'get_contact': {
        'required': ['lookup_field', 'lookup_value'],
        'optional': [],
        'convert': {},
        'validators': [validate_lookup_field],
        'check_contact_id': False,
    },
    'get_payment_plan': {
        'required': ['contact_id'],
        'optional': [],
        'convert': {'contact_id': int},
        'validators': [],
        'check_contact_id': True,
    },
    'get_debts': {
        'required': ['contact_id'],
        'optional': [],
        'convert': {'contact_id': int},
        'validators': [],
        'check_contact_id': True,
    },
    'get_settlement_offer': {
        'required': ['contact_id'],
        'optional': [],
        'convert': {'contact_id': int},
        'validators': [],
        'check_contact_id': True,
    },
    'accept_settlement_offer': {
        'required': ['contact_id', 'debt_id', 'offer_id'],
        'optional': ['reason'],
        'convert': {'contact_id': int, 'debt_id': int, 'offer_id': int},
        'validators': [],
        'check_contact_id': True,
    },
    'reject_settlement_offer': {
        'required': ['contact_id', 'debt_id', 'offer_id'],
        'optional': ['reason'],
        'convert': {'contact_id': int, 'debt_id': int, 'offer_id': int},
        'validators': [],
        'check_contact_id': True,
    },
}

def request_check(headers, body, endpoint):
    rules = ENDPOINT_RULES.get(endpoint)
    if not rules:
        return {'status_code': 400, 'message': f"Unknown endpoint: {endpoint}", 'params': {}}

    params = {}
    errors = []

    # Required fields
    for key in rules['required']:
        if key not in body:
            errors.append(f"Missing required field: {key}")
        else:
            try:
                conv = rules['convert'].get(key)
                params[key] = conv(body[key]) if conv else body[key]
            except Exception:
                errors.append(f"Invalid type for {key}")

    # Optional fields
    for key in rules['optional']:
        if key in body:
            try:
                conv = rules['convert'].get(key)
                params[key] = conv(body[key]) if conv else body[key]
            except Exception:
                errors.append(f"Invalid type for {key}")

    # Custom validators
    for validator in rules['validators']:
        try:
            # validator expects two args: field name & value
            # here only used for lookup_field/lookup_value pair
            if validator is validate_lookup_field:
                validator(params.get('lookup_field'), params.get('lookup_value'))
        except ValidationError as e:
            errors.append(str(e))

    # Authentication
    token = headers.get('Authorization', '')
    contact_id = params.get('contact_id')
    if authenticate(token, contact_id, rules['check_contact_id']) == 401:
        errors.append('Unauthorized')

    # Determine status
    if errors:
        code = 400 if any(e.startswith('Missing') or e.startswith('Invalid') for e in errors) else 401
        return {'status_code': code, 'message': errors[0], 'params': {}}

    return {'status_code': 200, 'message': 'OK', 'params': params}


# ===== Flask App & Endpoints =====

app = Flask(__name__)

@app.route('/get_contact', methods=['POST'])
def get_contact():
    req = request_check(request.headers, request.get_json(), 'get_contact')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    lookup_field = req['params']['lookup_field']
    lookup_value = req['params']['lookup_value']
    attempts, attempt = 10, 0

    df = snow_data_pull(
        f"SELECT ADDRESS,CITY,STATE,ZIP,CELL_PHONE_NUMBER,CONTACT_ID "
        f"FROM GUARDIAN_APP.DATA.TBL_CONTACT_ID_LIST WHERE {lookup_field} = '{lookup_value}'",
        'ENCS'
    )
    json_data = df.to_json(orient='records')

    while (json_data is None or len(json_data) == 2) and attempt < attempts:
        df = snow_data_pull(
            f"SELECT ADDRESS,CITY,STATE,ZIP,CELL_PHONE_NUMBER,CONTACT_ID "
            f"FROM GUARDIAN_APP.DATA.TBL_CONTACT_ID_LIST WHERE {lookup_field} = '{lookup_value}'",
            'ENCS'
        )
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.25)

    status = 404 if len(json_data) == 0 else 200
    return json_data, status

@app.route('/get_payment_plan', methods=['POST'])
def get_payment_plan():
    req = request_check(request.headers, request.get_json(), 'get_payment_plan')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    contact_id = req['params']['contact_id']
    attempts, attempt = 10, 0

    df = snow_data_pull(
        f"SELECT * FROM GUARDIAN_APP.DATA.TBL_UNPROCESSED_TRANSACTIONS "
        f"WHERE CONTACT_ID = {contact_id} ORDER BY PROCESS_DATE ASC",
        'ENCS'
    )
    json_data = df.to_json(orient='records')

    while (json_data is None or len(json_data) == 2) and attempt < attempts:
        df = snow_data_pull(
            f"SELECT * FROM GUARDIAN_APP.DATA.TBL_UNPROCESSED_TRANSACTIONS "
            f"WHERE CONTACT_ID = {contact_id} ORDER BY PROCESS_DATE ASC",
            'ENCS'
        )
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.05)

    status = 404 if len(json_data) == 0 else 200
    return json_data, status

@app.route('/get_payment_plan_prev', methods=['POST'])
def get_payment_plan_prev():
    # same rules as get_payment_plan
    req = request_check(request.headers, request.get_json(), 'get_payment_plan')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    contact_id = req['params']['contact_id']
    attempts, attempt = 10, 0

    df = snow_data_pull(
        f"SELECT * FROM GUARDIAN_APP.DATA.TBL_ALL_TRANSACTIONS "
        f"WHERE CONTACT_ID = {contact_id} ORDER BY PROCESS_DATE ASC",
        'ENCS'
    )
    json_data = df.to_json(orient='records')

    while len(json_data) == 2 and attempt < attempts:
        df = snow_data_pull(
            f"SELECT * FROM GUARDIAN_APP.DATA.TBL_ALL_TRANSACTIONS "
            f"WHERE CONTACT_ID = {contact_id} ORDER BY PROCESS_DATE ASC",
            'ENCS'
        )
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.05)

    status = 404 if len(json_data) == 0 else 200
    return json_data, status

@app.route('/get_debts', methods=['POST'])
def get_debts():
    req = request_check(request.headers, request.get_json(), 'get_debts')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    contact_id = req['params']['contact_id']
    attempts, attempt = 10, 0

    df = snow_data_pull(
        f"SELECT * FROM GUARDIAN_APP.DATA.TBL_DEBTS WHERE CONTACT_ID = {contact_id}",
        'ENCS'
    )
    # Hotfix replacements
    if 'SETTLEMENT_AMOUNT' in df.columns:
        df['SETTLEMENT_AMOUNT'] = df['SETTLEMENT_AMOUNT'].replace(0.0, None)
    for col in ['CLIENT_AUTH_OBTAINED', 'IN_REVIEW', 'QC_PASS', 'COMPLETED']:
        if col in df.columns:
            df[col] = df[col].replace(False, None)

    json_data = df.to_json(orient='records')
    while (json_data is None or len(json_data) == 2) and attempt < attempts:
        df = snow_data_pull(
            f"SELECT * FROM GUARDIAN_APP.DATA.TBL_DEBTS WHERE CONTACT_ID = {contact_id}",
            'ENCS'
        )
        if 'SETTLEMENT_AMOUNT' in df.columns:
            df['SETTLEMENT_AMOUNT'] = df['SETTLEMENT_AMOUNT'].replace(0.0, None)
        for col in ['CLIENT_AUTH_OBTAINED', 'IN_REVIEW', 'QC_PASS']:
            if col in df.columns:
                df[col] = df[col].replace(False, None)
        json_data = df.to_json(orient='records')
        attempt += 1
        time.sleep(.05)

    status = 404 if len(json_data) == 0 else 200
    return json_data, status

@app.route('/get_settlement_offer', methods=['POST'])
def get_settlement_offer():
    req = request_check(request.headers, request.get_json(), 'get_settlement_offer')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    contact_id = req['params']['contact_id']
    attempts, attempt = 10, 0

    df = snow_data_pull(
        f"SELECT * FROM DATA_ALPS.DATA_VAULT.VW_CONTACT_AUTH_SETTLEMENT_OFFERS "
        f"WHERE CONTACT_ID = {contact_id}",
        'ENCS'
    )
    while df.to_json(orient='records') == '[]' and attempt < attempts:
        df = snow_data_pull(
            f"SELECT * FROM DATA_ALPS.DATA_VAULT.VW_CONTACT_AUTH_SETTLEMENT_OFFERS "
            f"WHERE CONTACT_ID = {contact_id}",
            'ENCS'
        )
        attempt += 1
        time.sleep(.05)

    res = []
    for entry in df.to_dict(orient="records"):
        debt_id   = entry.get("DEBT_ID")
        offer_id  = entry.get("OFFER_ID1")
        message1  = entry.get("AUTHORIZATION_MESSAGE_PART1")
        message2  = entry.get("AUTHORIZATION_MESSAGE_PART2")
        settAmt   = entry.get("SETTLEMENT_AMOUNT")
        plan_df   = snow_data_pull(
            f"SELECT * FROM DATA_ALPS.DATA_VAULT.VW_SETTLEMENT_OFFER_PAYMENT_PLAN WHERE ID = {offer_id}",
            "ENCS"
        )
        plan      = plan_df.to_dict(orient="records")
        # retry plan if needed
        plan_attempt = 0
        while len(plan) == 2 and plan_attempt < attempts:
            time.sleep(.05)
            plan_df = snow_data_pull(
                f"SELECT * FROM DATA_ALPS.DATA_VAULT.VW_SETTLEMENT_OFFER_PAYMENT_PLAN WHERE ID = {offer_id}",
                "ENCS"
            )
            plan = plan_df.to_dict(orient="records")
            plan_attempt += 1

        resp_df  = snow_data_pull(
            f"SELECT * FROM GUARDIAN_APP.DATA.TBL_SETTLEMENT_OFFER_RESPONSES WHERE OFFER_ID = {offer_id}",
            "ENCS"
        )
        responses = resp_df.to_dict(orient="records")

        res.append({
            "debtIDsOffer":  debt_id,
            "offerID":       offer_id,
            "message1":      message1,
            "message2":      message2,
            "settAmount":    settAmt,
            "plan":          plan,
            "offerResponses": responses,
        })

    status = 404 if not res else 200
    return json.dumps(res, ensure_ascii=False), status

@app.route('/accept_settlement_offer', methods=['POST'])
def accept_settlement_offer():
    req = request_check(request.headers, request.get_json(), 'accept_settlement_offer')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    p = req['params']
    now_ts = datetime.now().strftime("%Y-%m-%d")
    query = (
        f"INSERT INTO GUARDIAN_APP.DATA.TBL_SETTLEMENT_OFFER_RESPONSES "
        f"(TIMESTAMP1, DEBT_ID, OFFER_ID, CONTACT_ID, RESPONSE) "
        f"VALUES (DATE '{now_ts}', {p['debt_id']}, {p['offer_id']}, {p['contact_id']}, 'YES');"
    )
    df = snow_data_pull(query, 'ENCS')
    return jsonify([{'rows': df}]), 200

@app.route('/reject_settlement_offer', methods=['POST'])
def reject_settlement_offer():
    req = request_check(request.headers, request.get_json(), 'reject_settlement_offer')
    if req['status_code'] != 200:
        return jsonify({"Response": req['message']}), req['status_code']

    p = req['params']
    now_ts = datetime.now().strftime("%Y-%m-%d")
    reason = p.get('reason', 'null')
    query = (
        f"INSERT INTO GUARDIAN_APP.DATA.TBL_SETTLEMENT_OFFER_RESPONSES "
        f"(TIMESTAMP1, DEBT_ID, OFFER_ID, CONTACT_ID, RESPONSE, REJECT_REASON) "
        f"VALUES (DATE '{now_ts}', {p['debt_id']}, {p['offer_id']}, {p['contact_id']}, 'NO', {reason});"
    )
    df = snow_data_pull(query, 'ENCS')
    return jsonify([{'rows': df}]), 200

cached_videos = None
last_fetch_time = 0
CACHE_EXPIRATION = 3600  # 1 hour in seconds

@app.route('/videos', methods=['GET'])
def get_videos():
    global cached_videos, last_fetch_time
    current_time = time.time()
    if cached_videos and (current_time - last_fetch_time) < CACHE_EXPIRATION:
        return jsonify({'videos': cached_videos}), 200

    # fetch channel's upload playlist
    ch_resp = requests.get(
        'https://www.googleapis.com/youtube/v3/channels',
        params={'part':'contentDetails','id':CHANNEL_ID,'key':YOUTUBE_API_KEY}
    )
    if ch_resp.status_code != 200:
        return jsonify({'error':'Failed to fetch channel details'}), 500
    playlist_id = ch_resp.json()['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # fetch videos
    pl_resp = requests.get(
        'https://www.googleapis.com/youtube/v3/playlistItems',
        params={'part':'snippet','playlistId':playlist_id,'maxResults':10,'key':YOUTUBE_API_KEY}
    )
    if pl_resp.status_code != 200:
        return jsonify({'error':'Failed to fetch playlist items'}), 500

    videos = []
    for item in pl_resp.json().get('items', []):
        videos.append({
            'videoId':    item['snippet']['resourceId']['videoId'],
            'title':      item['snippet']['title'],
            'description':item['snippet']['description'],
            'thumbnail':  item['snippet']['thumbnails']['high']['url'],
            'publishedAt':item['snippet']['publishedAt'],
        })

    cached_videos = videos
    last_fetch_time = current_time
    return jsonify({'videos': videos}), 200

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
