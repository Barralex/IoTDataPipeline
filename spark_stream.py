import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType
import datetime
from kafka import KafkaProducer
import json
from cassandra.query import BatchStatement

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.temperature_readings (
        device_key TEXT PRIMARY KEY,
        temperature FLOAT,
        timestamp TIMESTAMP);
    """)

    print("temperature_readings table created successfully!")


def send_temperature_to_kafka(producer, topic, message):
    try:
        producer.send(topic, value=message)
        producer.flush()
        logging.info(f"Message sent to Kafka topic {topic}")
    except Exception as e:
        logging.error(f"Could not send message to Kafka topic {topic} due to {e}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'iot_temperature_readings') \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("device_key", StringType(), True),
        StructField("temperature", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select(
            "data.*")
    print(sel)

    return sel


def process_batch(df, epoch_id, session, producer):
    if not df.rdd.isEmpty():
        df = df.withColumn("temperature", col("temperature").cast("float"))
        avg_temp_df = df.groupBy("device_key").agg(avg("temperature").alias("avg_temperature"))

        batch = BatchStatement()

        results = []

        for row in avg_temp_df.collect():
            device_id = row["device_key"]
            avg_temp = row["avg_temperature"]
            print(f"Device ID: {device_id}, Average Temperature: {avg_temp}")

            insert_query = session.prepare("""
                INSERT INTO spark_streams.temperature_readings (device_key, temperature, timestamp)
                VALUES (?, ?, ?)
            """)
            batch.add(insert_query, (device_id, float(avg_temp), datetime.datetime.now()))

            results.append({"device_key": device_id, "avg_temperature": avg_temp})

        if batch:
            session.execute(batch)

        if results:
            send_temperature_to_kafka(producer, "average_temperature_results", results)


def create_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        logging.info("Kafka producer created successfully")
    except Exception as e:
        logging.error(f"Could not create Kafka producer due to {e}")
    return producer


if __name__ == "__main__":

    spark_conn = create_spark_connection()
    session = create_cassandra_connection()
    producer = create_kafka_producer()

    if spark_conn is not None and producer is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream
                               .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, session, producer))
                               .outputMode("update")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .start())

            streaming_query.awaitTermination()
            session.shutdown()

