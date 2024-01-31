from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

import config

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.get_config().set("pipeline.jars", "file:////Users/lcerchie/Downloads/flink-sql-connector-kafka-3.0.2-1.18.jar")
print("set env")

# org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule

# 2. create source Table
table_env.execute_sql(f"""
    CREATE TABLE tumble_interval (
        window_start INT,
        window_end INT,
        price INT
    ) WITH (
   'connector' = 'kafka',
  'properties.security.protocol' = 'SASL_PLAINTEXT',
  'properties.sasl.mechanism' = 'PLAIN',
'topic' = 'tumble_interval', 
  'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.CC_KEY}" password="{config.CC_SECRET}";',
   'properties.bootstrap.servers' = 'psrc-1wydj.us-east-2.aws.confluent.cloud:9092',
    'properties.group.id' = 'stocks_consumer',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'        
    )
""")
print("connect to kafka")
# 3. create sink Table
table_env.execute_sql("""
    CREATE TABLE print (
        window_start INT,
        window_end INT,
        price INT
    ) WITH (
        'connector' = 'print'
    )
""")

source_table = table_env.sql_query("SELECT * FROM tumble_interval")
print(source_table)

result_table = source_table.select(col("price"), col("window_start"),col("window_end"))
print(result_table)

table_env.sql_query("SELECT * FROM tumble_interval") \
    .execute_insert("print").wait()

# table_env.execute_sql("INSERT INTO print SELECT * FROM tumble_interval").wait()