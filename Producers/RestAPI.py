"""
This module builds a flask API endpoint where you can Report Seismic Activity,
or fetch the entire preserved log history from the database
"""

from __future__ import unicode_literals
import json
import psycopg2
import re
from flask import Flask, jsonify, request, make_response, send_from_directory
from confluent_kafka import Producer
import socket
import os

API_producer = Producer(
    {"bootstrap.servers": "kafka:9092",
    "client.id": socket.gethostname(),
    "log_level": 0}
)

API = Flask(__name__, static_folder='./Web/build')

# GET

def fetch_from_DB(table):
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

# POST

def parseData(data):
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

def activity_logs():
    return jsonify({"message": "GET request received, returning activity logs"})


def publish_event(event):
    if event:
        if event['payload']['magnitude']>=3.5:
            API_producer.produce('severe_seismic_events', value = json.dumps(event).encode('utf-8'))
        else:
            API_producer.produce('minor_seismic_events', value = json.dumps(event).encode('utf-8'))
        return jsonify({"message": "Event Published Successfully","event":event['payload']})
    else:
        return make_response(jsonify({"message":"Incorrect Data Format! Try Again!",
                                      "format":{
                                          "magnitude":"float",
                                          "region":"string",
                                          "time":"ISO-8601 string. Min: YYYY, Max: YYYY-MM-DD(T)hh:mm:ss.ssssss(Zone)",
                                          "co_ordinates":"array of floats"
                                      }}),422)

# The Endpoints

@API.route("/", methods=['GET'])
def web():
    if API.static_folder is not None:
        return send_from_directory(API.static_folder, 'index.html')
    else:
        return make_response(jsonify({"message": "Web UI not available at the moment!"}), 503)
@API.route('/assets/<path:filename>', methods=['GET'])
def serve_static(filename):
    if API.static_folder is not None:
        return send_from_directory(API.static_folder + '/assets', filename)
    else:
        return make_response(jsonify({"message": "Web UI not available at the moment!"}), 503)

@API.route("/seismic_events", methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def events():
    if request.method == "GET":
        data = fetch_from_DB("severe_seismic_events") + fetch_from_DB("minor_seismic_events")
        if data:
            return jsonify(data)
        return make_response(jsonify({'message':"No Records Found!"}),503)
    elif request.method == "POST":
        return publish_event(parseData(request.get_json()))
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET or POST method on this Endpoint"}),405)

@API.route("/seismic_events/severe",methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def severe():
    if request.method == "GET":
        data = fetch_from_DB("severe_seismic_events")
        if data:
            return jsonify(data)
        return make_response(jsonify({'message':"No Records Found!"}),503)
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET method on this Endpoint"}),405)

@API.route("/seismic_events/minor",methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def minor():
    if request.method == "GET":
        data = fetch_from_DB("minor_seismic_events")
        if data:
            return jsonify(data)
        return make_response(jsonify({'message':"No Records Found!"}),503)
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET method on this Endpoint"}),405)

@API.errorhandler(404)
def invalid_endpoint(e):
    return make_response(jsonify({"message":"Not an Endpoint", "Available Endpoints":["GET -> / (Web UI)","GET, POST -> /seismic_events","GET -> /seismic_events/severe","GET -> /seismic_events/minor"]}),404)

@API.errorhandler(405)
def method_not_allowed(e):
    return make_response(jsonify({"message":"Method not Allowed", "Available Endpoints":["GET -> / (Web UI)","GET, POST -> /seismic_events","GET -> /seismic_events/severe","GET -> /seismic_events/minor"]}),405)

if __name__ == "__main__":
    API.run(host='producers', port=5000)
