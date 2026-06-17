import json
import time
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://localhost:27017")
TOPIC           = "detection_results"
GROUP_ID        = "storage-group"

def wait_for_kafka(bootstrap, retries=10, delay=5):
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=bootstrap,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            print("[Storage] Kafka connected!")
            return consumer
        except NoBrokersAvailable:
            print(f"[Storage] Kafka chua san sang ({i+1}/{retries}), thu lai...")
            time.sleep(delay)
    raise RuntimeError("Khong the ket noi Kafka")

def main():
    consumer = wait_for_kafka(KAFKA_BOOTSTRAP)

    client     = MongoClient(MONGO_URI)
    db         = client["people_counter"]
    collection = db["detection_results"]

    # Index de query nhanh theo video va thoi gian
    collection.create_index([("video_name", 1), ("frame_id", 1)])
    collection.create_index("timestamp")

    print("[Storage] Bat dau luu ket qua vao MongoDB...")

    for msg in consumer:
        data = msg.value

        # Khong luu annotated_frame de tiet kiem dung luong
        data.pop("annotated_frame", None)

        collection.insert_one(data)
        print(f"[Storage] Da luu frame {data['frame_id']} "
              f"({data['people_count']} nguoi) tu {data['video_name']}")

if __name__ == "__main__":
    main()
