from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import os

D = os.listdir(r'C:\Users\duong\PycharmProjects\DoAn\data')

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    product_name_list = ["VIRAT_S_00001","VIRAT_S_00002","VIRAT_S_00003",
                         "VIRAT_S_00004","VIRAT_S_00005", "VIRAT_S_00006"]
    order_card_list = ["No1", "No2", "No3", "No4", "No5", "No6"]

    frame_list = D


    message_list = []
    message = None
    for i in range(1000):
        i = i + 1
        message = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        message["order_id"] = i
        message["order_product_name"] = random.choice(product_name_list)
        message["order_card_type"] = random.choice(order_card_list)
        message["order_amount"] = round(random.uniform(5.5, 555.5), 2)
        message["order_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        message["order_frame"] = random.choice(frame_list)

        #
        print("Message: ", message)

        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)

    # print(message_list)

    print("Kafka Producer Application Completed. ")