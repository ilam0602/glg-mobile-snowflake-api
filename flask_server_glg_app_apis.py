from snowflake.connector.pandas_tools import write_pandas
from flask import Flask, request, jsonify
import snowflake.connector
from time import sleep
import pandas as pd
import json
import os
import firebase_admin
from firebase_admin import credentials, auth, firestore
from dotenv import load_dotenv


load_dotenv()



# Load the Firebase service account key from environment variable
# print(os.getenv("Hello"))
firebase_service_account_key = json.loads(os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY"))
forth_api_key = json.loads(os.getenv("FORTH_API_KEY"))

# # Initialize Firebase Admin SDK
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
        users_ref = db.collection('users')
        query = users_ref.where('uid', '==', uid).stream()
    
        for user in query:
            user_data = user.to_dict()
            # print(f'{type(contact_id.toString)} == {type(user_data.get("contact_id"))}')

            if user_data.get('contact_id') == str(contact_id):
                return True
        return False
    except Exception as e:
        return False

def authenticate(token,contact_id,check_contact_id):
    print(token)
    decoded_token = verify_firebase_token(token)
    print(decoded_token)
    
    if not decoded_token:
        return 401

    uid = decoded_token.get('uid')
    print(f'contact_id: {check_contact_id}')
    if check_contact_id and not check_user_contact(uid, contact_id):
        return 401
    return 200
    
    




'''
### Success Responses (200-299)
- **200 OK**: The request was successful.

### Client Error Responses (400-499)
- **400 Bad Request**: The server cannot process the request due to client error (e.g., malformed request).
- **401 Unauthorized**: The client must authenticate itself to get the requested response.

### Server Error Responses (500-599)
- **500 Internal Server Error**: The server has encountered a situation it doesn't know how to handle.
'''

def snow_data_pull(sql_statement, snowflake_instance):
    snowflake_instance = snowflake_instance.lower()
    with open('snow_credentials.json', 'r') as file: f = json.loads(file.read())
    conn = snowflake.connector.connect(
        user= f[f'{snowflake_instance}_snow_username'],
        password= f[f'{snowflake_instance}_snow_password'],
        account= f[f'{snowflake_instance}_snow_account'],
        warehouse= f[f'{snowflake_instance}_snow_warehouse'],
        role = f[f'{snowflake_instance}_snow_role']
    )
    cur = conn.cursor()
    cur.execute(f'{sql_statement}')
    df = cur.fetch_pandas_all()
    return df

def request_check(headers, json_body, endpoint):
    code = None
    code_discription = None
    lookup_field = None
    lookup_value = None
    contact_id = None

    headers = {key: value for key, value in headers.items()}
    # if authorization['Authorization'] != headers['Authorization']:
    #     code = 401; code_discription = 'Unauthorized: The client must authenticate itself to get the requested response.'
    # if authorization['Authorization'] == headers['Authorization']:
    #     code = 200; code_discription = 'OK: The request was successful.'
    if (endpoint == 'get_payment_plan') | (endpoint == 'get_debts'):
        if 'contact_id' not in json_body:
            code = 400; code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
        try:
            contact_id = int(json_body['contact_id'])
        except:
            code = 400; code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
    if (endpoint == 'get_contact') & ('lookup_field' not in json_body) & ('lookup_value' not in json_body):
        code = 400; code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
    if (endpoint == 'get_contact') & ('lookup_field' in json_body) & ('lookup_value' in json_body):
        lookup_field = json_body['lookup_field']
        lookup_value = json_body['lookup_value']
        field_confirmed = False
        for field in ['contact_id', 'ssn', 'hash_value']:
            if (field == lookup_field) & (lookup_value != '*') & (lookup_value != '_') & (lookup_value != '%'):
                field_confirmed = True
        if field_confirmed == False:
            code = 400; code_discription = 'Bad Request: The server cannot process the request due to client error (e.g., malformed request).'
    print(endpoint)
    if(authenticate(headers['Authorization'],contact_id, not(endpoint == 'get_contact'))==401):
        code = 401; code_discription = 'Unauthorized: The client must authenticate itself to get the requested response.'
    else:
        code = 200; code_discription = 'OK: The request was successful.'
    return headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value

app = Flask(__name__)

@app.route('/get_contact', methods = ['POST'])
def get_contact():
    headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value = request_check(request.headers, request.get_json(), 'get_contact')
    # auth(headers['Authorization'], contact_id, True)
    if code != 200: return jsonify({"Response": code_discription}), code
    df = snow_data_pull(f"SELECT * FROM KORE_AI.DATA.TBL_CONTACT_ID_LIST WHERE {lookup_field} = '{lookup_value}'", 'ENCS')
    json_data = df.to_json(orient='records')
    return json_data, code

@app.route('/get_payment_plan', methods = ['POST'])
def get_payment_plan():
    headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value = request_check(request.headers, request.get_json(), 'get_payment_plan')
    # auth(headers['Authorization'], contact_id, True)
    if code != 200: return jsonify({"Response": code_discription}), code
    df = snow_data_pull(f'SELECT * FROM KORE_AI.DATA.TBL_PAYMENT_PLAN2 WHERE CONTACT_ID = {contact_id}', 'ENCS')
    json_data = df.to_json(orient='records')
    return json_data, code

@app.route('/get_debts', methods = ['POST'])
def get_debts():
    headers, json_body, code, code_discription, contact_id, lookup_field, lookup_value = request_check(request.headers, request.get_json(), 'get_debts')
    # auth(headers['Authorization'], contact_id, True)
    if code != 200: return jsonify({"Response": code_discription}), code
    df = snow_data_pull(f'SELECT * FROM KORE_AI.DATA.TBL_DEBTS WHERE CONTACT_ID = {contact_id}', 'ENCS')
    json_data = df.to_json(orient='records')
    return json_data, code

if __name__ == '__main__':
    #app.run(debug = True)
    #https://flaskserverglgappapis-oxypy426xa-uc.a.run.app/get_contact
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))