import sys
import time

import pandas as pd
import streamlit as st
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

consumer = Consumer(client_config)



# We create the placeholder once
placeholder = st.empty()

running = True



try:
        consumer.subscribe(["AAPL"])

        while True:
            msg = consumer.poll()

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))

            df = pd.DataFrame({"my_col": [(msg.value().decode('utf-8'))]})
            # It is important to exit the context of the placeholder in each step of the loop
            # placeholder object should have the same methods for displaying data as st
            placeholder.dataframe(df)
            time.sleep(2)
finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False