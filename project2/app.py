import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

keyspace = 'npetrovic_p2_keyspace'


def build_database(cassandra_session):
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % keyspace)

    cassandra_session.set_keyspace(keyspace)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS statistics (
            time timestamp ,
            duration_min float,
            duration_max float,
            duration_avg float,
            num_of_rides float,
            PRIMARY KEY (time)
        )
        """)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS popular_stations (
            time timestamp ,
            start_station_name1 text,
            num_of_rides1 float,
            start_station_name2 text,
            num_of_rides2 float,
            start_station_name3 text,
            num_of_rides3 float,
            PRIMARY KEY (time)
        )
        """)


def write_to_cassandra(df, epoch, station_id):
    print("Epoch " + str(epoch))
    zad1 = df
    zad2 = df
    zad1 = zad1.filter(
        zad1.start_station_id == station_id) \
        .agg(
        min(col("duration").cast('int')).alias('duration_min'), max(col("duration").cast('int')).alias('duration_max'),
        mean(col("duration").cast('int')).alias('duration_avg'),
        count("start_station_id").alias('num_of_rides')
    ).collect()

    zad2 = zad2.groupBy(
        zad2.start_station_name) \
        .agg(count('start_station_name').alias('num_of_rides')).sort(desc('num_of_rides')).take(3)

    if zad1[0]['duration_avg']:
        davg = zad1[0]['duration_avg']
        dmax = zad1[0]['duration_max']
        dmin = zad1[0]['duration_min']
        rides = zad1[0]['num_of_rides']

        cassandra_session.execute(f"""
                            INSERT INTO npetrovic_p2_keyspace.statistics(time, duration_avg, duration_max, duration_min, num_of_rides)
                            VALUES (toTimeStamp(now()), {davg}, {dmax}, {dmin}, {rides})
                            """)

    stations = ['Unknown', 'Unknown', 'Unknown']
    num_rides = [-1, -1, -1]
    for i, row in enumerate(zad2):
        stations[i] = row['start_station_name']
        num_rides[i] = row['num_of_rides']

    cassandra_session.execute(f"""
                    INSERT INTO npetrovic_p2_keyspace.popular_stations(time, start_station_name1, num_of_rides1, start_station_name2, num_of_rides2, start_station_name3, num_of_rides3)
                    VALUES (toTimeStamp(now()), '{stations[0]}', {num_rides[0]}, '{stations[1]}', {num_rides[1]}, '{stations[2]}', {num_rides[2]})
                    """)


if __name__ == '__main__':
    load_dotenv()
    kafka_url = str(os.getenv('KAFKA_URL'))
    kafka_topic = str(os.getenv('KAFKA_TOPIC'))
    start_station_id = str(os.getenv('START_STATION_ID'))
    cassandra_host = str(os.getenv('CASSANDRA_HOST'))
    process_time = str(os.getenv('PROCESS_TIME'))

    cassandra_cluster = Cluster([cassandra_host], port=9042)
    cassandra_session = cassandra_cluster.connect()
    build_database(cassandra_session)
    print('Connected to Cassandra!')

    dataSchema = StructType() \
        .add("started_at", "timestamp") \
        .add("ended_at", "timestamp") \
        .add("duration", "string") \
        .add("start_station_id", "string") \
        .add("start_station_name", "string") \
        .add("start_station_description", "string") \
        .add("start_station_latitude", "decimal") \
        .add("start_station_longitude", "decimal") \
        .add("end_station_id", "string") \
        .add("end_station_name", "string") \
        .add("end_station_description", "string") \
        .add("end_station_latitude", "decimal") \
        .add("end_station_longitude", "decimal")

    # spark = (
    #     SparkSession.builder.appName("BigDataP2Nemanja")
    #         .config("spark.jars.packages",
    #                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0")
    #         .getOrCreate()
    # )
    spark = (
        SparkSession.builder.appName("BigDataP2Nemanja")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    print("Jedan")
    sampleDataframe = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_url)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
    ).selectExpr("CAST(value as STRING)", "timestamp").select(
        from_json(col("value"), dataSchema).alias("sample"), "timestamp"
    ).select("sample.*")

    print("Dva")

    sampleDataframe.writeStream \
        .option("spark.cassandra.connection.host", cassandra_host+':'+str(9042)) \
        .foreachBatch(lambda df, epoch_id: write_to_cassandra(df, epoch_id, start_station_id)) \
        .outputMode("update") \
        .trigger(processingTime=process_time) \
        .start().awaitTermination()
