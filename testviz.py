import asyncio
import datetime
import json
import math
import sys
import time

import pandas as pd
import streamlit as st
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
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

data = []


def get_time_offset():
    """Returns the POSIX epoch representation (in milliseconds) of the datetime 5 minutes prior to being called"""
    delta = datetime.timedelta(hours=5)
    now = datetime.datetime.now(
        datetime.timezone.utc
    )  # TZ-aware object to simplify POSIX epoch conversion
    prior = now - delta
    return math.floor(
        prior.timestamp() * 1000
    )  # convert seconds to milliseconds for Consumer.offsets_for_times()


def reset_offsets(consumer, partitions):
    """Resets the offsets of the provided partitions to the first offsets found corresponding to timestamps greater than or equal to 5 minutes ago."""
    time_offset = get_time_offset()
    search_partitions = [
        TopicPartition(p.topic, p.partition, time_offset) for p in partitions
    ]  # new TPs with offset= time_offset
    time_offset_partitions = consumer.offsets_for_times(
        search_partitions
    )  # find TPs with timestamp of earliest offset >= time_offset
    consumer.assign(
        time_offset_partitions
    )  # (re-)set consumer partition assignments and start consuming


async def consume_and_write(placeholder):
    if isinstance(option, str):

        st.write("You selected:", option)

        # We create the placeholder once
        placeholder = st.empty()

        # asyncio not implemented, consumer only works if producer is not triggered. need to get them on separate threads
        await on_select(option)

        for i in range(5):
            try:
                consumer.subscribe(["tumble_interval"], on_assign=reset_offsets)

                msg = await consumer.poll()

                if msg is None:
                    pass

                elif msg.error():
                    print("Consumer error: {}".format(msg.error()))

                # print("Received message: {}".format(msg.value()))

                with placeholder:
                    data_string_with_bytes_mess = "{}".format(msg.value())

                    data_string_without_bytes_mess = (
                        data_string_with_bytes_mess.replace(
                            data_string_with_bytes_mess[0:22], ""
                        )
                    )

                    data_string_without_bytes_mess = data_string_without_bytes_mess[:-1]
                    dict_to_append = json.loads(data_string_without_bytes_mess)

                    st.write()
                    data.append(dict_to_append["price"])

                # It is important to exit the context of the placeholder in each step of the loop
                # placeholder object should have the same methods for displaying data as st
                # placeholder.dataframe(df)

                # Close down consumer to commit final offsets.

            except KeyboardInterrupt:
                print("Canceled by user.")
                consumer.close()

        st.bar_chart(data)

    # https://stackoverflow.com/questions/76056824/how-to-consume-the-last-5-minutes-data-in-kafka-using-confluent-kakfa-python-pac


async def main():
    placeholder = st.empty()
    await consume_and_write(placeholder)


asyncio.run(main())
