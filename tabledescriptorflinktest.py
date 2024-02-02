from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.common.typeinfo import Types
from pyflink.table import DataTypes
from pyflink.datastream.formats.json import (
    JsonRowSerializationSchema,
    JsonRowDeserializationSchema,
)

from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    TableDescriptor,
    Schema,
    DataTypes,
    FormatDescriptor,
)

import config

import json


table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
table_env.get_config().set("parallelism.default", "1")


table_env.get_config().set(
    "pipeline.jars",
    "file:////Users/lcerchie/Downloads/flink-sql-connector-kafka-3.0.2-1.18.jar",
)

table_env._add_jars_to_j_env_config(
    "file:////Users/lcerchie/Downloads/flink-sql-connector-kafka-3.0.2-1.18.jar"
)


table_env.create_temporary_table(
    "tumble_interval",
    TableDescriptor.for_connector("kafka")
    .schema(
        Schema.new_builder()
        .column("window_start", DataTypes.BIGINT())
        .column("window_end", DataTypes.BIGINT())
        .column("price", DataTypes.DOUBLE())
        .build()
    )
    .option("properties.security.protocol", "SASL_SSL")
    .option("properties.sasl.mechanism", "PLAIN")
    .option("topic", "tumble_interval")
    .option(
        "properties.sasl.jaas.config",
        f'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.CC_KEY}" password="{config.CC_SECRET}";',
    )
    .option(
        "properties.bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
    )
    .option("properties.group.id", "stocks_consumer")
    .option("scan.startup.mode", "earliest-offset")
    .option("value.format", "json")
    .build(),
)

table = table_env.execute_sql("SELECT * FROM tumble_interval")

table.execute().print()
