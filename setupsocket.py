# https://pypi.org/project/websocket_client/
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from configparser import ConfigParser
import asyncio

import config
import srconfig
import json


from alpaca.data.live import StockDataStream


def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'reading for {event.key().decode("utf8")} produced to {event.topic()}')


def serialize_custom_data(custom_data, ctx):
    return {
        "bid_timestamp": str(custom_data.timestamp),
        "price": int(custom_data.bid_price),
        "symbol": custom_data.symbol,
    }


async def quote_data_handler(data, producer, stockname, json_serializer):

    config_parser = ConfigParser(interpolation=None)
    config_file = open("config.properties", "r")
    config_parser.read_file(config_file)
    client_config = dict(config_parser["kafka_client"])

    producer = Producer(client_config)

    schema_registry_client = SchemaRegistryClient(srconfig.sr_config)

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
      "description": "JSON number type.",
      "type": "number"
    },
    "symbol": {
      "description": "The string type is used for strings of text.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}"""

    json_serializer = JSONSerializer(
        schema_str, schema_registry_client, serialize_custom_data
    )
    # quote data will arrive here
    print(data)

    await producer.produce(
        topic=stockname,
        key=stockname,
        value=json_serializer(
            data, SerializationContext(stockname, MessageField.VALUE)
        ),
        on_delivery=delivery_report,
    )

    producer.flush()


async def on_select(stockname):
    print("onselect called")

    wss_client = StockDataStream(config.ALPACA_KEY, config.ALPACA_SECRET)

    wss_client.subscribe_quotes(quote_data_handler, stockname)

    wss_client.run()
