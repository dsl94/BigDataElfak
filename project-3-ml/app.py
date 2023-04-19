import time
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv
from influxdb import InfluxDBClient
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer

load_dotenv()

dbhost = os.getenv('INFLUXDB_HOST', 'localhost')
dbport = int(os.getenv('INFLUXDB_PORT'))
dbuser = os.getenv('INFLUXDB_USERNAME')
dbpassword = os.getenv('INFLUXDB_PASSWORD')
dbname = os.getenv('INFLUXDB_DATABASE')
topic = os.getenv('KAFKA_TOPIC')
kafka = os.getenv('KAFKA_URL')

def influxDBconnect():
    return InfluxDBClient(dbhost, dbport, dbuser, dbpassword, dbname)


def influxDBwrite(count, predictions, accuracy):
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    measurementData = [
        {
            "measurement": topic,
            "time": timestamp,
            "fields": {
                "number of rows": count,
                "predictions": count,
                "correct predictions": accuracy
            }
        }
    ]
    print(measurementData)
    influxDBConnection.write_points(measurementData, time_precision='ms')
    print("Count " + str(count))
    print("Accuracy " + str(accuracy))

def analyze(df, epoch, model):
    print("Epoch " + str(epoch))
    columns = ['start_station_id', 'end_station_id']

    for column in columns:
        df = df.withColumn(column, F.col(column).cast(FloatType()))

    vectorAssembler = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid('skip')

    assembled = vectorAssembler.transform(df)

    stringIndexer = StringIndexer().setInputCol('duration').setOutputCol('label')
    indexedDataFrame = stringIndexer.fit(assembled).transform(assembled)

    prediction = model.transform(indexedDataFrame)

    prediction.select('prediction', 'label')
    # prediction.show(truncate=False)

    predictionsMade = prediction.count()

    correctNumber = float(prediction.filter(prediction['label'] == prediction['prediction']).count())

    influxDBwrite(df.count(), predictionsMade, correctNumber)


if __name__ == '__main__':
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

    MODEL = os.getenv('MODEL')
    influxDBConnection = influxDBconnect()

    # spark = (
    #     SparkSession.builder.appName("BigDataP2Nemanja")
    #         .config("spark.jars.packages",
    #                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0")
    #         .getOrCreate()
    # )
    spark = (
        SparkSession.builder.appName("BigDataP3-ClassificationNemanja")
            .getOrCreate()
    )

    model = PipelineModel.load(MODEL)
    spark.sparkContext.setLogLevel("INFO")
    sampleDataframe = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
    ).selectExpr("CAST(value as STRING)", "timestamp").select(
        from_json(col("value"), dataSchema).alias("sample"), "timestamp"
    ).select("sample.*")


    sampleDataframe.writeStream \
        .foreachBatch(lambda df, epoch_id: analyze(df, epoch_id, model)) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start().awaitTermination()
