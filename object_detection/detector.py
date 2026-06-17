import json
import base64
import time
import os
import numpy as np
import cv2
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from ultralytics import YOLO

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC      = "raw_frames"
OUTPUT_TOPIC     = "detection_results"
GROUP_ID         = "detection-group"
CONFIDENCE_THRES = 0.5

def wait_for_kafka(bootstrap, retries=10, delay=5):
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=bootstrap,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                max_partition_fetch_bytes=10_485_760,
            )
            producer = KafkaProducer(bootstrap_servers=bootstrap)
            print("[Detector] Kafka connected!")
            return consumer, producer
        except NoBrokersAvailable:
            print(f"[Detector] Kafka chua san sang ({i+1}/{retries}), thu lai...")
            time.sleep(delay)
    raise RuntimeError("Khong the ket noi Kafka")

def decode_frame(b64_str):
    img_bytes = base64.b64decode(b64_str)
    arr       = np.frombuffer(img_bytes, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)

def draw_boxes(frame, boxes):
    annotated = frame.copy()
    for x1, y1, x2, y2 in boxes:
        cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 255, 0), 2)
    cv2.putText(annotated, f"Count: {len(boxes)}", (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
    _, buf = cv2.imencode(".jpg", annotated)
    return base64.b64encode(buf).decode("utf-8")

def main():
    consumer, producer = wait_for_kafka(KAFKA_BOOTSTRAP)

    model = YOLO("yolov8n.pt")
    print("[Detector] Model YOLOv8 da san sang.")

    for msg in consumer:
        data  = msg.value
        frame = decode_frame(data["frame_data"])

        results = model(frame, classes=[0], conf=CONFIDENCE_THRES, verbose=False)

        boxes = []
        for r in results:
            for box in r.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                boxes.append([x1, y1, x2, y2])

        result_msg = {
            "video_name":      data["video_name"],
            "frame_id":        data["frame_id"],
            "timestamp":       datetime.now().isoformat(),
            "people_count":    len(boxes),
            "bounding_boxes":  boxes,
            "annotated_frame": draw_boxes(frame, boxes),
        }

        producer.send(OUTPUT_TOPIC, json.dumps(result_msg).encode("utf-8"))
        print(f"[Detector] Frame {data['frame_id']} -> {len(boxes)} nguoi | boxes: {boxes}")

    producer.flush()

if __name__ == "__main__":
    main()
