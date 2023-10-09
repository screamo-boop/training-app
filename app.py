import uuid
import logging
import platform
import random
from flask import Flask, request, jsonify
from os import getenv
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from bson.json_util import dumps
from json import loads
from datetime import datetime, timedelta

app = Flask(__name__)

# Getting neccessary information from env variables

CONNECTION_STRING = getenv(
    'APP_DB_CONNECTION_URL', '')
DBNAME = getenv('APP_DB_NAME', 'OilRefineApplication')
APP_PORT = getenv('APP_PORT', '8086')
FORMAT = f"%(asctime)s - [%(levelname)s] - %(message)s"
global ready
ready = 1
global healthy
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
memory_consumer = []

# That function randomly generating stuff into db


def Add_Converted_Material_To_DB():
    converted_type = ['Ai-92', 'Ai-95', 'Ai-98', 'Ai-100', 'PremiumLuxary666',
                      'Aviation Cerosine', 'Technical Cerosine', 'Lightning Cerosine']
    transaction_id = str(uuid.uuid1())
    try:
        petrol.insert_one({'transaction_id': transaction_id, 'type': random.choice(
            converted_type), 'quantity': random.randint(5, 100)})
    except Exception as e:
        logging.error(f'MongoDB Error - {e}')
        return 'Error! DB Connection error', 500

    logging.info(f'Item created! Transaction ID - {transaction_id}')

def back_to_live():
    global ready
    global healthy
    ready = 1
    healthy = 1

# Set schedule for generate dummy requests
sched = BackgroundScheduler(daemon=True)
sched.add_job(Add_Converted_Material_To_DB, 'interval', minutes=1)
sched.add_job(back_to_live, 'interval', minutes=1)
sched.start()


@app.route("/")
def main_page():
    return f'''Welcome to <h1>CrudeRefineApp</h1> API <br>
      DB Name: {DBNAME} <br> 
      Port of application: {APP_PORT} <br> 
      Hostname: {platform.uname()[1]} <br>
      System: {platform.platform()}'''

# Healthcheck an readiness check, readiness check can be manually changed


@app.route("/health")
def health_check():
    if healthy == 1:
        return "Healthy"
    else:
        return 'Unhealthy', 502

@app.route("/health_set", methods=['POST'])
def health_set():
    raw_data = request.data
    try:
        global healthy
        healthy = int(raw_data)      
    except Exception as e:
        logging.error(e)
        pass
    return 'ok', 200


@app.route("/load_memory")
def load_memory():
    x = bytearray(1024*1024*100)
    memory_consumer.append(x)
    return "100mb increased"


@app.route("/readiness")
def readiness_check():
    if ready == 1:
        return 'im ok', 200
    else:
        return 'something went wrong', 502


@app.route("/readiness_set", methods=['POST'])
def readiness_set():
    raw_data = request.data
    try:
        global ready
        ready = int(raw_data)      
    except Exception as e:
        logging.error(e)
        pass
    return 'ok', 200

# Showing stats of added materials to database


@app.route("/statistics")
def statistics():
    formatted_str = "<tr><th>Material Type</th><th>Quantity</th></tr>"
    try:
        stats_from_db = list(petrol.aggregate(
            [{"$group": {"_id": "$type", "sumof": {"$sum": "$quantity"}}}]))
    except Exception as e:
        logging.error(f'MongoDB Error - {e}')
        return 'Error! DB Connection error', 500

    for position in stats_from_db:
        formatted_str += f'<tr>'
        for key, val in position.items():
            formatted_str += f'<th>{val}</th>'
        formatted_str += f'</tr>'

    return f'<table> {formatted_str} </table>' + ''' <style> table, th, td { border:1px solid black; } </style>''', 201


@app.route("/statistics_json")
def statistics_json():

    try:
        stats_from_db = list(petrol.aggregate(
            [{"$group": {"_id": "$type", "sumof": {"$sum": "$quantity"}}}]))
    except Exception as e:
        logging.error(f'MongoDB Error - {e}')
        return 'Error! DB Connection error', 500

    return jsonify(loads(dumps(stats_from_db)))


@app.route('/petrol', methods=['GET', 'POST'])
def petrol():
    data = request.get_json()
    match request.method:
        case 'GET':
            if 'transaction-id' in data:
                cursor = petrol.find(
                    {'transaction_id': data['transaction-id']})
                return jsonify(loads(dumps(cursor)))
            else:
                return '', 400
        case 'POST':
            if 'type' and 'quantity' in data:
                transaction_id = str(uuid.uuid1())
                try:
                    petrol.insert_one(
                        {'transaction_id': transaction_id, 'type': data['type'], 'quantity': int(data['quantity'])})
                except Exception as e:
                    logging.error(f'MongoDB Error - {e}')
                    return 'Error! DB Connection error', 500

                logging.info(
                    f'Item created! Transaction ID - {transaction_id}')
                return jsonify({"status": "Ok", "transaction-id": transaction_id}), 201
            else:
                return '', 400
        case _:
            return "Something went wrong", 500


if __name__ == "__main__":
    try:
        client = MongoClient(CONNECTION_STRING)
        check_connection = client.admin.command('ismaster')
        logging.info("MongoDB connected!")
    except Exception as e:
        logging.fatal(f'MongoDB error - {e}')
        exit()

    db = client[DBNAME]
    petrol = db['conversion']
    healthy = 1
    logging.info("Application started")
    app.run(host=getenv('APP_ADDRESS', '0.0.0.0'), port=APP_PORT)
