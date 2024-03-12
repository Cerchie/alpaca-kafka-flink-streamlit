`git clone https://github.com/Cerchie/finnhub.git` && cd finnhub

pip3 install confluent_kafka

pip3 install alpaca-py

pip3 install streamlit

[sign up for alpaca](https://alpaca.markets/) to get a key


for configuration, you will need:

config.properties

```
[kafka_client]
bootstrap.servers=
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=
sasl.password=

group.id="stocks_consumer_group"

# Best practice for higher availability in librdkafka clients prior to 1.7
session.timeout.ms=45000
```
config.py
```
ALPACA_KEY = 
ALPACA_SECRET = 
FINNHUB_KEY =
CC_KEY =
CC_SECRET = 
```

srconfig.py
```
sr_config = {
    "url": ,
    "basic.auth.user.info": ,
}

```

In Confluent Cloud, set up a new environment with topic named 'AAPL' and this value schema:

```
{
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
}
```

You'll also need API keys for your schema registry and topic.

Now, run streamlit testviz.py to produce some messages to that topic. 


Then, set up a Flink compute pool _in the same region_ as your cluster. Run these commands in the UI:

```sql
CREATE TABLE tumble_interval
(`symbol` STRING, `window_start` STRING,`window_end` STRING,`price` DOUBLE, PRIMARY KEY (`symbol`) NOT ENFORCED)
    WITH ('value.format' = 'json-registry');
```

```sql
INSERT INTO tumble_interval
SELECT symbol, DATE_FORMAT(window_start,'yyyy-MM-dd hh:mm:ss.SSS'), DATE_FORMAT(window_end,'yyyy-MM-dd hh:mm:ss.SSS'), AVG(price)
FROM TABLE(
        TUMBLE(TABLE AAPL, DESCRIPTOR($rowtime), INTERVAL '5' SECONDS))
GROUP BY
    symbol,
    window_start,
    window_end;

```

Now your app should be able to consume from 'tumble_interval' 
