#https://pypi.org/project/websocket_client/
import pickle
from confluent_kafka import Producer
from configparser import ConfigParser


import config
import json


from alpaca.data.live import StockDataStream

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

producer = Producer(client_config)

wss_client = StockDataStream(config.ALPACA_KEY, config.ALPACA_SECRET)

def on_select(stockname):

# async handler
    async def quote_data_handler(data):
        # quote data will arrive here
        print(data)
        in_string = str(data)

        producer.produce(stockname, in_string)

    wss_client.subscribe_quotes(quote_data_handler, stockname)

    wss_client.run()