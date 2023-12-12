"""
This module builds a flask API endpoint where you can Report Seismic Activity,
or fetch the entire preserved log history from the database
"""

import json
from os import name
from flask import Flask, jsonify, request
from confluent_kafka import Producer
import socket

API_producer = Producer(
    {"bootstrap.servers": "kafka:9092",
    "client.id": socket.gethostname()}
)

API = Flask(__name__)

def parseData(event):
    try:
        magnitude = int(event['magnitude'])
        region = event['region']
        time = event['time']
        co_ordinates = list(event['co_ordinates'])
        return {'magnitude': magnitude, 'region': region, 'time': time, 'co_ordinates': co_ordinates}
    except:
        return False

def activity_logs():
    return jsonify({"message": "GET request received, returning activity logs"})


def publish_event(data):
    event = parseData(data)
    if event:
        if event['magnitude']>=3.5:
            API_producer.produce('severe_seismic_events', json.dumps(event).encode('utf-8'))
        else:
            API_producer.produce('minor_seismic_events', json.dumps(event).encode('utf-8'))
        return jsonify({"message": "Event Published Successfully","event":event})
    else:
        return jsonify({"message":"Incorrect Data Format! Try Again!"})


@API.route("/activity", methods=["GET", "POST"])
def process():
    if request.method == "GET":
        return activity_logs()
    elif request.method == "POST":
        return publish_event(request.get_json())
    return jsonify({"message": "405: Invalid HTTP Access"})


if __name__ == "__main__":
    API.run(host='producer', port=5000)
