import asyncio
import datetime
import json
import math
import streamlit as st
from confluent_kafka import Consumer, TopicPartition
from setupsocket import on_select


config_dict = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "sasl.mechanisms": "PLAIN",
    "security.protocol": "SASL_SSL",
    "auto.offset.reset": "earliest",
    "session.timeout.ms": "45000",
    "sasl.username": st.secrets["SASL_USERNAME"],
    "sasl.password": st.secrets["SASL_PASSWORD"],
}
# https://stackoverflow.com/questions/38032932/attaching-kafaconsumer-assigned-to-a-specific-partition
consumer = Consumer(config_dict)


st.title("Stock Price Averages")
st.write(
    "Each bar represents an average price from the last five seconds of sales. The chart may not show up if trading is closed for the day or otherwise not happening."
)

option = st.selectbox(
    "Which stock would you like to see the price avg for?",
    ("AAPL", "BABA"),
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


# https://stackoverflow.com/questions/76056824/how-to-consume-the-last-5-minutes-data-in-kafka-using-confluent-kakfa-python-pac


async def main():
    if isinstance(option, str):

        await asyncio.gather(on_select(option), display_quotes(placeholder))


async def display_quotes(component):
    component.empty()
    price_history = []
    print("Subscribing to topic")
    topic_name = option
    consumer.assign([TopicPartition(f"tumble_interval_{topic_name}", 3)])
    print(f"tumble_interval_{topic_name}")
    consumer.subscribe([f"tumble_interval_{topic_name}"])

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

            with component:
                data_string_with_bytes_mess = "{}".format(msg.value())
                data_string_without_bytes_mess = data_string_with_bytes_mess.replace(
                    data_string_with_bytes_mess[0:22], ""
                )
                data_string_without_bytes_mess = data_string_without_bytes_mess[:-1]
                quote_dict = json.loads(data_string_without_bytes_mess)

                last_price = quote_dict["price"]

                price_history.append(f"${last_price}")

                # uncomment this if you prefer to see the price history.
                # data = price_history

                # but I think it's easier to just see the price fluctuate in place
                data = {"price_avg": price_history}
                print("data coming in:", data)
                component.bar_chart(data, color=["#fb8500"], y=None)

        except KeyboardInterrupt:
            print("Canceled by user.")
            consumer.close()

        # We create the placeholder once


placeholder = st.empty()


st.subheader(
    "What's going on behind the scenes of this chart?",
    divider="rainbow",
)
st.image(
    "./graph.png",
    caption="chart graphing relationship of different nodes in the data pipeline",
)
st.markdown(
    "First, data is piped from the [Alpaca API](https://docs.alpaca.markets/docs/getting-started) websocket into a Kafka topic located in Confluent Cloud. Next, the data is processed in [Confluent Cloud’s](https://confluent.cloud/) Flink SQL workspace with a query like this."
)
st.code(
    """INSERT INTO tumble_interval
SELECT symbol, DATE_FORMAT(window_start,'yyyy-MM-dd hh:mm:ss.SSS'), DATE_FORMAT(window_end,'yyyy-MM-dd hh:mm:ss.SSS'), AVG(price)
FROM TABLE(
        TUMBLE(TABLE AAPL, DESCRIPTOR($rowtime), INTERVAL '5' SECONDS))
GROUP BY
    symbol,
    window_start,
    window_end;
""",
    language="python",
)
st.markdown(
    "Then, the data is consumed from a Kafka topic backing the FlinkSQL table in Confluent Cloud, and visualized using Streamlit."
)
st.markdown(
    "For more background on this project and to run it for yourself, visit the [GitHub repository](https://github.com/Cerchie/alpaca-kafka-flink-streamlit/tree/main)."
)

asyncio.run(main())
