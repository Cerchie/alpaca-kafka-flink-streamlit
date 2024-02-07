#https://pypi.org/project/websocket_client/
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from configparser import ConfigParser


import config
import srconfig
import json


from alpaca.data.live import StockDataStream

schema_str = """{
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "bid_timestamp": {
      "description": "The string type is used for strings of text.",
      "type": "string"
    },
    "price": {
      "description": "The integer type is used for integral numbers.",
      "type": "integer"
    },
    "symbol": {
      "description": "The string type is used for strings of text.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}"""

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])


producer = Producer(client_config)

wss_client = StockDataStream(config.ALPACA_KEY, config.ALPACA_SECRET)

schema_registry_client = SchemaRegistryClient(srconfig.sr_config)


def on_select(stockname):


    async def quote_data_handler(data):
        # quote data will arrive here
        print(data)

        def delivery_report(err, event):
            if err is not None:
                print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
            else:
                print(f'reading for {event.key().decode("utf8")} produced to {event.topic()}')

        def serialize_custom_data(custom_data):
                return json.dumps({
            'price': int(data.bid_price),
            'symbol': data.symbol,
            'bid_timestamp': data.timestamp,       
        },default=str).encode('utf-8')


        json_serializer = JSONSerializer(schema_str, schema_registry_client, data_to_dict)
        
        producer.produce(stockname, serialize_custom_data(data).encode('utf8'))

        producer.flush()

    wss_client.subscribe_quotes(quote_data_handler, stockname)

    wss_client.run()