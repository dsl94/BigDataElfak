import os
from dotenv import load_dotenv
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, mean, stddev, dayofweek
from pyspark.sql.types import StructType

if __name__ == '__main__':
    load_dotenv()
    START_ID = float(os.getenv('START_ID'))
    START_DATE = str(os.getenv('START_DATE'))
    END_DATE = str(os.getenv('END_DATE'))
    FILE_DATA = os.getenv('FILE_DATA')
    HDFS_DATA = os.getenv('HDFS_DATA')

    lines = list()
    # spark = SparkSession.builder.master('local[*]').appName('BigDataP1Nemanja').getOrCreate()
    spark = SparkSession.builder.appName('BigDataP1Nemanja').getOrCreate()
    dataSchema = StructType() \
        .add("started_at", "timestamp") \
        .add("ended_at", "timestamp") \
        .add("duration", "integer") \
        .add("start_station_id", "integer") \
        .add("start_station_name", "string") \
        .add("start_station_description", "string") \
        .add("start_station_latitude", "decimal") \
        .add("start_station_longitude", "decimal") \
        .add("end_station_id", "integer") \
        .add("end_station_name", "string") \
        .add("end_station_description", "string") \
        .add("end_station_latitude", "decimal") \
        .add("end_station_longitude", "decimal")

    # dataFrame = spark.read.csv(FILE_DATA, schema=dataSchema)
    dataFrame = spark.read.csv(HDFS_DATA, schema=dataSchema)

    from_date = datetime.datetime.strptime(START_DATE, "%Y-%m-%dT%H:%M:%S")
    to_date = datetime.datetime.strptime(END_DATE, "%Y-%m-%dT%H:%M:%S")

    query1_count = dataFrame.filter(dataFrame.start_station_id == START_ID).filter((dataFrame.started_at >= from_date) &
                                                                (dataFrame.started_at <= to_date)).count()
    lines.append("ZADATAK 1")
    query1 = "Number of rides from given station in given time period is: " + str(query1_count)
    lines.append(query1)

    query2_dataFrame = dataFrame.filter(dataFrame.start_station_id == START_ID).filter((dataFrame.started_at >= from_date) &
                                                            (dataFrame.started_at <= to_date)).groupBy(dataFrame.end_station_name).count()

    query2 = "5 most frequent routes from given start station in given period:\n"
    query2_result = query2_dataFrame.sort(query2_dataFrame['count'].desc()).collect()
    for res in query2_result:
        lines.append(res['end_station_name'] + " " + str(res['count']))

    lines.append("ZADATAK 2")
    query3 = "Statistics group by start station in period od one week"
    lines.append(query3)
    query3_result = dataFrame.groupBy("start_station_name", dayofweek("started_at")).agg(
        min("duration"), max("duration"), stddev("duration"), mean("duration")
    ).collect()

    for res in query3_result:
        line = res['start_station_name'] + " on day of the week " + str(res['dayofweek(started_at)'])
        line = line + " min duration " + str(res['min(duration)'])
        line = line + " max duration " + str(res['max(duration)'])
        line = line + " average duration " + str(res['avg(duration)'])
        line = line + " standard deviation of duration " + str(res['stddev_samp(duration)'])
        lines.append(line)

    with open("output.txt", "w") as fileOutput:
        for line in lines:
            fileOutput.write(line)
            fileOutput.write("\n")

