"""
This module builds a Flask API Server that serves the interactive Web UI and exposes API endpoints
where you can Report Seismic Activity & fetch the entire preserved log history from the database.
"""

import json
import psycopg2
import re
from flask import Flask, jsonify, request, make_response, send_from_directory
from confluent_kafka import Producer
import socket
import os

#* Config

API_producer = Producer(
    {"bootstrap.servers": "kafka:9092",
    "client.id": socket.gethostname(),
    "log_level": 0}
)

API = Flask(__name__, static_folder='./Web/build')

#* GET Controllers

def fetch_from_DB(table):
    """Fetch Logs from PostgresDB
    Args:
        table (str): Table name to query
    Returns:
        list: Returns a list of Seismic Records
    """
    try:
        conn = psycopg2.connect(
            host = os.getenv("DB_HOST"),
            database = os.getenv("DB_NAME"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
        )
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        data = cur.fetchall()
        if (not data):
            raise Exception
        return [{'magnitude':row[0], 'region': row[1], 'time': row[2], 'co_ordinates': row[3]} for row in data]
    except:
        return []

#* POST Controllers

def parseData(data):
    """Parse Seismic Event Data to adhere to the following format:
    {
        "magnitude":"float",
        "region":"string",
        "time":"ISO-8601 string. Min: YYYY, Max: YYYY-MM-DD(T)hh:mm:ss.ssssss(Zone)",
        "co_ordinates":"array of floats"
    }
    and create a Kafka-JDBC Record Format with Schema.
    Args:
        data (dict): A Seismic Data dict to be parsed
    Returns:
        bool or dict: Returns False if data is invalid and returns a formatted Kafka Record dict if valid
    """
    try:
        magnitude = float(data['magnitude'])
        region = data['region']
        time = data["time"]
        timeExpr = r"\d{4}(-\d{2}(-\d{2}(T\d{2}(:\d{2}(:\d{2}(\.\d{1,6})?)?)?(Z|[+-]\d{2}:\d{2})?)?)?)?"
        if (not re.fullmatch(timeExpr,time)):
            raise Exception("Invalid Time Format")
        co_ordinates = "[" + ', '.join(map(str,list(data['co_ordinates']))) + "]"
        return {'schema':{
            "type":'struct', 'optional': False, 'version':1, 'fields':[
                {"field":"magnitude","type":"float"},
                {"field":"region","type":"string"},
                {"field": "time", "type": "string"},
                {"field":"co_ordinates","type":"string"}
            ]
        },'payload': {'magnitude': magnitude, 'region': region, 'time': time, 'co_ordinates': co_ordinates}}
    except:
        return False

def publish_event(event):
    """Publish a Seismic Record to the respective Kafka Topic based on its Magnitude
    Args:
        event (dict): Parsed Seismic Record
    Returns:
        Response: JSON HTTP Response
    """
    if event:
        if event['payload']['magnitude']>=3.5:
            API_producer.produce('severe_seismic_events', value = json.dumps(event).encode('utf-8'))
        else:
            API_producer.produce('minor_seismic_events', value = json.dumps(event).encode('utf-8'))
        return jsonify({"message": "Event Published Successfully","event":event['payload']})
    else:
        return make_response(jsonify({"message":"Incorrect Data Format! Try Again!",
                                    "format": {
                                        "magnitude":"float",
                                        "region":"string",
                                        "time":"ISO-8601 string. Min: YYYY, Max: YYYY-MM-DD(T)hh:mm:ss.ssssss(Zone)",
                                        "co_ordinates":"array of floats"
                                    }}),422)

#* The Endpoints

# Web UI
@API.route('/', defaults={'path': 'index.html'}, methods=['GET'])
@API.route('/<path:path>', methods=['GET'])
def serve(path):
    if API.static_folder is not None:
        return send_from_directory(API.static_folder, path)
    else:
        return make_response(jsonify({"message": "Web UI not available at the moment!"}), 503)

# Public APIs
@API.route("/seismic_events", methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def events():
    if request.method == "GET":  # Retrieve all Seismic Logs
        data = fetch_from_DB("severe_seismic_events") + fetch_from_DB("minor_seismic_events")
        if data:
            return jsonify(data)
        return make_response(jsonify({'message':"No Records Found!"}),503)
    elif request.method == "POST":  # Publish Seismic Event
        return publish_event(parseData(request.get_json()))
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET or POST method on this Endpoint"}),405)

@API.route("/seismic_events/severe",methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def severe():
    if request.method == "GET":  # Retrieve Severe Seismic Logs
        data = fetch_from_DB("severe_seismic_events")
        if data:
            return jsonify(data)
        return make_response(jsonify({'message':"No Records Found!"}),503)
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET method on this Endpoint"}),405)

@API.route("/seismic_events/minor",methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def minor():
    if request.method == "GET":  # Retrieve Minor Seismic Logs
        data = fetch_from_DB("minor_seismic_events")
        if data:
            return jsonify(data)
        return make_response(jsonify({'message':"No Records Found!"}),503)
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET method on this Endpoint"}),405)

# Error Handlers
@API.errorhandler(404)
def invalid_endpoint(e):
    return make_response(jsonify({"message":"Not an Endpoint", "Available Endpoints":["GET -> / (Web UI)","GET, POST -> /seismic_events","GET -> /seismic_events/severe","GET -> /seismic_events/minor"]}),404)

@API.errorhandler(405)
def method_not_allowed(e):
    return make_response(jsonify({"message":"Method not Allowed", "Available Endpoints":["GET -> / (Web UI)","GET, POST -> /seismic_events","GET -> /seismic_events/severe","GET -> /seismic_events/minor"]}),405)

#* Serve
if __name__ == "__main__":
    API.run(host='0.0.0.0', port=5000)
