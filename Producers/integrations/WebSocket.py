"""
This module establishes a WebSocket connection to the Seismic Portal (©EMSC) ,
and publishes realtime seismic event data received over the connection to respective Kafka Topics.
"""

import logging
import json
import sys

from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado import gen
from confluent_kafka import Producer
import socket

#* Config

webSocket_uri = "wss://www.seismicportal.eu/standing_order/websocket"

WS_producer = Producer(
    {"bootstrap.servers": "kafka:9092", "client.id": socket.gethostname(), "log_level": 0}
)

#* Controllers

def parseData(data):
    """Parse Seismic Event Data to adhere to Kafka-JDBC Record Format with Schema
    Args:
        data (dict): A Seismic Data dict from the Websocket to be parsed
    Returns:
        bool or dict: Returns False if data is invalid and returns a formatted Kafka Record dict if valid
    """
    try:
        magnitude = float(data["data"]["properties"]["mag"])
        region = data["data"]["properties"]["flynn_region"]
        time = data["data"]["properties"]["time"]
        co_ordinates = "[" + ', '.join(map(str,list(data["data"]["geometry"]["coordinates"]))) + "]"
        return {
            "schema": {
                "type": "struct",
                "optional": False,
                "version": 1,
                "fields": [
                    {"field": "magnitude", "type": "float"},
                    {"field": "region", "type": "string"},
                    {"field": "time", "type": "string"},
                    {"field": "co_ordinates", "type": "string"}
                ],
            },
            "payload": {
                "magnitude": magnitude,
                "region": region,
                "time": time,
                "co_ordinates": co_ordinates,
            },
        }
    except:
        return False

def publish_event(event):
    """Publish a Seismic Record to the respective Kafka Topic based on its Magnitude
    Args:
        event (dict): Parsed Seismic Record
    Returns:
        None
    """
    if event:
        if event['payload']['magnitude']>=3.5:
            WS_producer.produce(
                "severe_seismic_events", json.dumps(event).encode("utf-8")
            )
        else:
            WS_producer.produce(
                "minor_seismic_events", json.dumps(event).encode("utf-8")
            )
        logging.info(f"WebSocket - - [{event['payload']['time']}] MESSAGE Received OK!")
    else:
        logging.warning("WebSocket - - Corrupt Data Received!")

#* WebSocket

@gen.coroutine
def listen(ws):  # Read Messages from WebSocket
    while True:
        msg = yield ws.read_message()
        if msg is None:
            logging.info("close")
            ws = None
            break
        publish_event(parseData(json.loads(msg)))

@gen.coroutine
def launch_client():  # Connect to WebSocket
    try:
        logging.info("Open WebSocket connection to seismicportal.eu")
        ws = yield websocket_connect(webSocket_uri, ping_interval=15)
    except Exception:
        logging.exception("WebSocket Connection Error")
    else:
        logging.info("WebSocket Connection Successful!\nWaiting for messages...\n")
        listen(ws)

#* Initialize
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    io_loop = IOLoop.instance()
    launch_client()
    try:
        io_loop.start()
    except KeyboardInterrupt:
        logging.info("Close WebSocket")
        io_loop.stop()
