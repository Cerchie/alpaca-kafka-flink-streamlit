#https://pypi.org/project/websocket_client/
from confluent_kafka import Producer
from configparser import ConfigParser
from dotenv import load_dotenv

import os
import rel
import socket
import websocket

load_dotenv()

FINNHUB_KEY = os.environ['FINNHUB_KEY']

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])


producer = Producer(client_config)

def on_select(stockname):

    def on_message(ws, message):
        producer.produce(stockname, message)

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("### closed ###")

    def on_open(ws):
        ws.send(f'{{"type":"subscribe","symbol":"{stockname}"}}')
  


    websocket.enableTrace(True)

    ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={FINNHUB_KEY}',
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()

    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()