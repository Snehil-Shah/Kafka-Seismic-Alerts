"""
This module builds a flask API endpoint where you can Report Seismic Activity,
or fetch the entire preserved log history from the database
"""

from __future__ import unicode_literals
import json
import psycopg2
from dateutil.parser.isoparser import isoparse
from flask import Flask, jsonify, request, make_response
from confluent_kafka import Producer
import socket

API_producer = Producer(
    {"bootstrap.servers": "kafka:9092",
    "client.id": socket.gethostname(),
    "log_level": 0}
)

API = Flask(__name__)

# GET

def fetch_from_DB(table_1,table_2=None):
    try:
        conn = psycopg2.connect(
            host="db",
            database="Logs",
            user="postgres",
            password="password")
        cur = conn.cursor()
        if table_2:
            cur.execute(f"SELECT * FROM {table_1} UNION SELECT * FROM {table_2}")
        else:
            cur.execute(f"SELECT * FROM {table_1}")
        return jsonify([{'magnitude':row[0], 'region': row[1], 'time': row[2], 'co_ordinates': row[3]} for row in cur.fetchall()])
    except:
        return make_response(jsonify({'message':"Service Unavailable at the Moment!"}),503)

# POST

def parseData(data):
    try:
        magnitude = float(data['magnitude'])
        region = data['region']
        time = data["time"]
        isoparse(time)
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
                                          "time":"ISO-8601 string",
                                          "co_ordinates":"array of floats"
                                      }}),422)

# The Endpoints

@API.route("/seismic_events", methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def events():
    if request.method == "GET":
        return fetch_from_DB("severe_seismic_events","minor_seismic_events")
    elif request.method == "POST":
        return publish_event(parseData(request.get_json()))
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET or POST method on this Endpoint"}),405)

@API.route("/seismic_events/severe",methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def severe():
    if request.method == "GET":
        return fetch_from_DB("severe_seismic_events")
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET method on this Endpoint"}),405)

@API.route("/seismic_events/minor",methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def minor():
    if request.method == "GET":
        return fetch_from_DB("minor_seismic_events")
    else:
        return make_response(jsonify({"message": "Invalid HTTP Access. Use GET method on this Endpoint"}),405)

@API.errorhandler(404)
def invalid_endpoint(e):
    return make_response(jsonify({"message":"Not an Endpoint", "Available Endpoints":["GET, POST -> /seismic_events","GET -> /seismic_events/severe","GET -> /seismic_events/minor"]}),404)

if __name__ == "__main__":
    API.run(host='producers', port=5000)
