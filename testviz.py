import asyncio
import sys
import time

import pandas as pd
import streamlit as st
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException
from setupsocket import on_select
import numpy as np


config_parser = ConfigParser(interpolation=None)

config_file = open("config.properties", "r")
config_parser.read_file(config_file)
client_config = dict(config_parser["kafka_client"])

consumer = Consumer(client_config)

option = st.selectbox(
    "Which stock would you like to see data for?",
    ("AAPL", "BABA"),
    index=None,
)


if isinstance(option, str):

    st.write("You selected:", option)

    # We create the placeholder once
    placeholder = st.empty()

    data = []

    on_select(option)

    while True:
        try:

            consumer.subscribe(["tumble_interval"])

            msg = consumer.poll()
            if msg is None:
                pass

            elif msg.error():
                print("Consumer error: {}".format(msg.error()))

            print("Received message: {}".format(msg.value().decode("utf-8")))

            with placeholder:
                print(data)
                data.append(msg.value().decode("utf-8"))
                st.write(pd.DataFrame(data))

            st.bar_chart(data)

            # It is important to exit the context of the placeholder in each step of the loop
            # placeholder object should have the same methods for displaying data as st
            # placeholder.dataframe(df)

            # Close down consumer to commit final offsets.

        except KeyboardInterrupt:
            print("Canceled by user.")
            consumer.close()

else:
    chart_data = pd.DataFrame(np.random.randn(3), columns=["No data yet"])
    st.bar_chart(chart_data)
