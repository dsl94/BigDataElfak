from kafka import KafkaProducer
import csv
import json
import time
import os
from dotenv import load_dotenv


if __name__ == '__main__':
    msgProducer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                value_serializer=lambda x: x.encode('utf-8'), api_version=(0,11,5))

    with open('oslo-bikes.csv') as csvFile:
        data = csv.DictReader(csvFile)
        for row in data:
            msgProducer.send('t2', json.dumps(row))
            msgProducer.flush()

            print('Message: ' + json.dumps(row))
            time.sleep(2)

    print('Kafka message producer done!')

