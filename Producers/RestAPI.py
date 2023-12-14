"""
This module builds a flask API endpoint where you can Report Seismic Activity,
or fetch the entire preserved log history from the database
"""

from __future__ import unicode_literals
import json
from os import name
from dateutil.parser.isoparser import isoparse
from flask import Flask, jsonify, request, make_response
from confluent_kafka import Producer
import socket

API_producer = Producer(
    {"bootstrap.servers": "kafka:9092",
    "client.id": socket.gethostname()}
)

API = Flask(__name__)

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
        return jsonify({"message": "Event Published Successfully","event":event})
    else:
        return make_response(jsonify({"message":"Incorrect Data Format! Try Again!"}),422)

# The Endpoint
@API.route("/activity", methods=["GET", "POST"])
def process():
    if request.method == "GET":
        return activity_logs()
    elif request.method == "POST":
        return publish_event(parseData(request.get_json()))
    return make_response(jsonify({"message": "Invalid HTTP Access"}),405)

if __name__ == "__main__":
    API.run(host='producers', port=5000)
