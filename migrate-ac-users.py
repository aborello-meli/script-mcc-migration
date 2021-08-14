import pymysql
import requests
import csv
import sys
import time
import concurrent.futures
import re

TOKEN = "15cc82e60de9d6765e8f0c95ab0ac4df7df114336e4963dc3e86cdb48728cdc7"
BASE_URL = "http://master_regulations-activity-code.furyapps.io"

## A db user and password needed to access regulations-activitycode mysql

DB_USER = None
DB_PASSWORD = None

### User structures

class NeedTokenRefresh(Exception):
    pass

def ccc_body(row):
    userid = row["user_id"]
    code = row["code"]
    codetype = row["type"]
    body = {
        "user_id": userid,
        "code": code,
        "type": codetype,
        "primary": False,
        "tags": [],
    }
    return body


### SQL

def get_connection():
    if not DB_USER or not DB_PASSWORD:
        print()
        print("SET THE DB_USER AND DB_PASSWORD IN THE SCRIPT")
        print()
        sys.exit(1)
    connection = pymysql.connect(host='proxysql.slave.meliseginf.com',
                                port=6612,
                                user=DB_USER,
                                password=DB_PASSWORD,
                                database='activcode',
                                cursorclass=pymysql.cursors.DictCursor)
    return connection

def select_ac_users(limit, offset):
    connection = get_connection()
    results = []
    with connection.cursor() as cursor:
        sql = f"SELECT * from user limit {limit} offset {offset}"
        cursor.execute(sql)
        connection.commit()
        results = cursor.fetchall()
    return results

def select_users_with_codes(limit, offset):
    connection = get_connection()
    results = []
    with connection.cursor() as cursor:
        sql = f"select distinct user.user_id from user inner join user_code on user_code.user_id=user.user_id limit {limit} offset {offset}"
        cursor.execute(sql)
        connection.commit()
        results = cursor.fetchall()
    return results

def select_user_codes(userid):
    connection = get_connection()
    results = []
    with connection.cursor() as cursor:
        sql = f"select oc.code, oc.site, user_code.user_id, user_code.type from user_code inner join original_code oc on user_code.code_id = oc.id where user_code.user_id={userid}"
        cursor.execute(sql)
        connection.commit()
        results = cursor.fetchall()
    return results

### Requests

def get_user(user_id):
    headers = {
        "x-auth-token": TOKEN,
        "X-Client-Id": "3016981581429034",
    }
    base_url = BASE_URL

    url = f"{base_url}/users/{user_id}/mcc/all"
    response = requests.get(url, headers=headers)
    return response

def create_user(user_id):
    headers = {
        "x-auth-token": TOKEN,
        "X-Client-Id": "3016981581429034",
    }
    base_url = BASE_URL

    url = f"{base_url}/createuser/{user_id}"
    response = requests.get(url, headers=headers)
    return response

def create_user_parallel(users):
    responses = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            res = [executor.submit(create_user, user) for user in users]
            concurrent.futures.wait(res)
            for fut in res:
                responses.append(fut.result())
        return responses
    except Exception as e:
        print("CREATION")
        print(e)
        return None

def get_user_parallel(users):
    responses = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            res = [executor.submit(get_user, user) for user in users]
            concurrent.futures.wait(res)
            for fut in res:
                responses.append(fut.result())
        return responses
    except Exception as e:
        print("GET")
        print(e)
        return None

def post_new_ccc(ccc):
    headers = {
        "x-auth-token": TOKEN,
        "X-Client-Id": "3016981581429034",
    }
    url = f"{BASE_URL}/users/ccc"
    response = requests.post(url, headers=headers, json=ccc)
    return response

# Script

def save_users(users):
    savefile = "missing-users"
    with open(savefile, "a") as userfile:
        for user in users:
            if user:
                userfile.write(f"{user}\n")

def save_created_users(users):
    savefile = "created-users"
    with open(savefile, "a") as userfile:
        for user in users:
            if user:
                userfile.write(f"{user}\n")

def save_last_offset(offset):
    path = "offset"
    with open(path, 'w') as savefile:
        savefile.write(f"{offset}\n")

def load_last_offset():
    path = "offset"
    with open(path, 'r') as savefile:
        return int(savefile.readline().strip())


number_match = re.compile("\D+(\d+)\D*")

def extract_first_number(text_with_number):
    match = number_match.match(text_with_number)
    try:
        number = match.group(1)
        return number
    except IndexError:
        # no number in text
        return None



def users_to_add(db_results):
    # {'user_id': -1, 'date_created': datetime.datetime(2017, 6, 15, 17, 26, 3), 'last_updated': datetime.datetime(2017, 6, 15, 17, 26, 3), 'site': 'MLA'}
    to_add = []
    uids = [x["user_id"] for x in db_results]

    retries = 0
    max_retries = 5
    results = None
    while retries < max_retries:
        results = get_user_parallel(uids)
        if results:
            break
        else:
            retries = retries + 1

    if not results:
        print("MAX RETRIES ON USERS TO ADD")
        sys.exit(1)

    for response in results:
        if response.status_code == 404:
            uid = extract_first_number(response.url)
            to_add.append(uid)
        if response.status_code == 401:
            raise NeedTokenRefresh()
    return to_add

def create_users(users):
    # {'user_id': -1, 'date_created': datetime.datetime(2017, 6, 15, 17, 26, 3), 'last_updated': datetime.datetime(2017, 6, 15, 17, 26, 3), 'site': 'MLA'}
    succesful = []
    failed = []
    retries = 0
    max_retries = 5
    results = None
    while retries < max_retries:
        results = create_user_parallel(users)
        if results:
            break
        else:
            retries = retries + 1

    if not results:
        print("MAX RETRIES ON CREATE USERS")
        sys.exit(1)

    for response in results:
        if response.status_code == 401:
            raise NeedTokenRefresh()
        if response.status_code == 200:
            uid = extract_first_number(response.url)
            succesful.append(uid)
        if response.status_code != 200:
            uid = extract_first_number(response.url)
            failed.append(uid)

    return succesful, failed

def create_ccc_entries_for_user(uid):
    db_data = select_user_codes(uid)
    ccc_bodies = [ccc_body(x) for x in db_data]
    responses = []
    for ccc in ccc_bodies:
        r = post_new_ccc(ccc)
        if r.status_code == 401:
            raise NeedTokenRefresh()
        responses.append(r)
    return responses

    

# get users from ac v2 db
# check if they are in ac v3
# if they are not, get their usercodes for ac v2
# then, fill them en ac v3
def fill_missing_users_to_ac():
    try:
        print()
        print(time.strftime("%H:%M:%S"))
        offset = load_last_offset()
        print(offset)
        print()
        db_result = select_users_with_codes(1000, offset)
        uids_to_add = users_to_add(db_result)
        if uids_to_add:
            success, failed = create_users(uids_to_add)
            save_created_users(success)
            save_users(failed)
            print(f"users creaetd: {len(success)}")
        print(f"users found: {len(uids_to_add)}")
    except NeedTokenRefresh:
        print("REFRESH THE TOKEN")
        sys.exit(1)
    save_last_offset(offset + 1000)

def run():
    while True:
        start = time.time()
        fill_missing_users_to_ac()
        end = time.time()
        if end - start < 1:
            time.sleep(1)

if __name__ == "__main__":
    run()
