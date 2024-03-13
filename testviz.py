import asyncio
import datetime
import json
import math
import streamlit as st
from configparser import ConfigParser
from confluent_kafka import Consumer, TopicPartition
from setupsocket import on_select

config_parser = ConfigParser(interpolation=None)

config_file = open("config.properties", "r")
config_parser.read_file(config_file)
client_config = dict(config_parser["kafka_client"])
print(f"client_config:{client_config}")

consumer = Consumer(client_config)

option = st.selectbox(
    "Which stock would you like to see data for?",
    ("AAPL", "BABA", "SIE", "SPY"),
    index=None,
)


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
    """Resets the offsets of the provided partitions to the first offsets found corresponding to timestamps greater
    than or equal to 5 minutes ago."""
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


async def main():
    if isinstance(option, str):
        st.write("You selected:", option)

        # We create the placeholder once
        placeholder = st.empty()

        await asyncio.gather(
            on_select(option),
            display_quotes(placeholder))


async def display_quotes(component):
    price_history = []
    print("Subscribing to topic")
    topic_name = "AAPL"
    consumer.subscribe([topic_name], on_assign=reset_offsets)

    while True:
        try:
            print("Polling topic")
            msg = consumer.poll(5)

            print("Pausing")
            await asyncio.sleep(0.5)

            print("Received message: {}".format(msg))
            if msg is None:
                continue

            elif msg.error():
                print("Consumer error: {}".format(msg.error()))

            # print("Received message: {}".format(msg.value()))

            with component:
                data_string_with_bytes_mess = "{}".format(msg.value())
                data_string_without_bytes_mess = data_string_with_bytes_mess.replace(
                    data_string_with_bytes_mess[0:22], ""
                )
                data_string_without_bytes_mess = data_string_without_bytes_mess[:-1]
                quote_dict = json.loads(data_string_without_bytes_mess)
                last_price = quote_dict["price"]
                price_history.append(last_price)

                # uncomment this if you prefer to see the price history.
                data = price_history

                # but I think it's easier to just see the price fluctuate in place
                # data = [last_price]
                component.bar_chart(data)



        except KeyboardInterrupt:
            print("Canceled by user.")
            consumer.close()


# https://stackoverflow.com/questions/76056824/how-to-consume-the-last-5-minutes-data-in-kafka-using-confluent-kakfa-python-pac
asyncio.run(main())
