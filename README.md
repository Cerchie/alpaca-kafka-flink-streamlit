# How to use FlinkSQL with Kafka, Streamlit, and the Alpaca API

Pssst. This app is currently deployed at https://alpaca-kafka-flink-app.streamlit.app/

Learn how to use these 4 technologies together by running this demo yourself! 

This project produces stock trade events from the [Alpaca API markets](https://app.alpaca.markets) websocket to an [Apache Kafka](https://kafka.apache.org/) topic located in [Confluent Cloud](https://www.confluent.io/lp/confluent-cloud). From there, it uses [FlinkSQL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/overview/) in Confluent Cloud to generate 5 sec averages of stock prices over a tumbling window. Then, this app consumes the averages from a backup Kafka topic and displays them using [Streamlit](https://streamlit.io/). 

<img width="718" alt="graph of the 4 technologies" src="https://github.com/Cerchie/alpaca-kafka-flink-streamlit/assets/54046179/7600d717-69bc-46c5-8679-d8d65b9ce810">


## Step 1: Get set up in Confluent Cloud

Sign up for [Confluent Cloud](https://www.confluent.io/confluent-cloud). Follow the in-product tutorial to create an environment called `stocks_environment`. Skip any prompting or options for stream governance.

Then, follow [these instructions](https://docs.confluent.io/cloud/current/get-started/index.html#section-1-create-a-cluster-and-add-a-topic) to create a cluster. When you're prompted to select a provider and location, choose AWS's `us-east-2`. 

[Create a Confluent Cloud API key](https://docs.confluent.io/cloud/current/access-management/authenticate/api-keys/api-keys.html#cloud-cloud-api-keys) and save it. 

[Create 2 topics](https://docs.confluent.io/cloud/current/get-started/index.html#section-1-create-a-cluster-and-add-a-topic) with one partition each named `AAPL` and `BABA`. 

[Create an API key](https://docs.confluent.io/cloud/current/get-started/schema-registry.html#create-an-api-key-for-ccloud-sr) for Schema Registry. 

For each topic, [set a JSON schema](https://docs.confluent.io/cloud/current/sr/schemas-manage.html#create-a-topic-schema):

```json
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

There are two urls you'll need later. Your bootstrap server url can be found under Cluster Overview -> Cluster Settings, and your Schema Registry URL is on the right under the Stream Governance API widget, named 'Endpoint'. 

[Create a Flink compute pool](https://docs.confluent.io/cloud/current/flink/operate-and-deploy/create-compute-pool.html#create-a-af-compute-pool-in-ccloud-console). Name it `stocks_compute_pool`.

[Run these two](https://docs.confluent.io/cloud/current/flink/get-started/quick-start-cloud-console.html#step-2-run-sql-statements) `CREATE table` statements in your FlinkSQL workspace:

```sql
CREATE TABLE tumble_interval_AAPL
(`symbol` STRING, `window_start` STRING,`window_end` STRING,`price` DOUBLE, PRIMARY KEY (`symbol`) NOT ENFORCED)
    WITH ('value.format' = 'json-registry');
```

and

```sql
CREATE TABLE tumble_interval_BABA
(`symbol` STRING, `window_start` STRING,`window_end` STRING,`price` DOUBLE, PRIMARY KEY (`symbol`) NOT ENFORCED)
    WITH ('value.format' = 'json-registry');
```

You'll need to run these two statements as well:


```sql
INSERT INTO tumble_interval_AAPL
SELECT symbol, DATE_FORMAT(window_start,'yyyy-MM-dd hh:mm:ss.SSS'), DATE_FORMAT(window_end,'yyyy-MM-dd hh:mm:ss.SSS'), AVG(price)
FROM TABLE(
        TUMBLE(TABLE AAPL, DESCRIPTOR($rowtime), INTERVAL '5' SECONDS))
GROUP BY
    symbol,
    window_start,
    window_end;

```

and

```sql
INSERT INTO tumble_interval_BABA
SELECT symbol, DATE_FORMAT(window_start,'yyyy-MM-dd hh:mm:ss.SSS'), DATE_FORMAT(window_end,'yyyy-MM-dd hh:mm:ss.SSS'), AVG(price)
FROM TABLE(
        TUMBLE(TABLE AAPL, DESCRIPTOR($rowtime), INTERVAL '5' SECONDS))
GROUP BY
    symbol,
    window_start,
    window_end;

```

## Step 2: Your Alpaca credentials

You'll need to sign up for [sign up for alpaca](https://alpaca.markets/) to get a key. 

Navigate to https://app.alpaca.market.

_You do not have to give a tax ID or fill out the other account info to get your paper trading key._

Generate a key using the widget you'll find on the right of the screen on the home page. This is the key/secret you'll be adding to the app. 

## Step 3: Get started running the app

`git clone https://github.com/Cerchie/finnhub.git && cd finnhub`

then

`pip install -r requirements.txt` 

Now, create a file in the root directory named `.streamlit/secrets.toml` (that initial `.` is part of the convention.)

In it, you'll need:

```
ALPACA_KEY = "your_alpaca_key"
ALPACA_SECRET = "your_alpaca_secret"
SASL_USERNAME = "your_confluent_cloud_api_key"
SASL_PASSWORD = "your_confluent_cloud_api_secret"
SR_URL =  "your_confluent_cloud
BASIC_AUTH_USER_INFO = "your_confluent_cloud_schema_registry_key:your_confluent_cloud_schema_registry_secret"
BOOTSTRAP_URL = "your_confluent_cloud_bootstrap_url"
```
Note that the `:` is necessary for `BASIC_AUTH_USER_INFO`. 

You'll need a [Streamlit account](https://streamlit.io/) as well for the secrets to be in the environment. 

Now, run `streamlit run alpacaviz.py` in your root dir in order to run the app. 

To deploy on Streamlit yourself, follow the [instructions here](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app) and make sure to [include the secrets](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management) in your settings. 
