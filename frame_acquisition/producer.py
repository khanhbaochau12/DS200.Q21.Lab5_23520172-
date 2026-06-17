import cv2
import base64
import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
VIDEO_PATH      = os.getenv("VIDEO_PATH", "video1.mp4")
TOPIC           = "raw_frames"
FPS_LIMIT       = 5  # gửi tối đa 5 frame/giây để tránh quá tải

def wait_for_kafka(bootstrap, retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                compression_type="gzip",       # nén frame trước khi gửi
                max_request_size=10_485_760,   # tối đa 10MB/message
                acks="all",                    # đảm bảo broker đã nhận
            )
            print("[Producer] Kafka connected!")
            return producer
        except NoBrokersAvailable:
            print(f"[Producer] Kafka chưa sẵn sàng, thử lại sau {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Không thể kết nối Kafka sau nhiều lần thử")

def main():
    producer = wait_for_kafka(KAFKA_BOOTSTRAP)

    cap = cv2.VideoCapture(VIDEO_PATH)
    if not cap.isOpened():
        raise FileNotFoundError(f"Không thể mở video: {VIDEO_PATH}")

    video_name = os.path.basename(VIDEO_PATH)
    frame_id   = 0
    interval   = 1.0 / FPS_LIMIT

    print(f"[Producer] Bắt đầu đọc video: {video_name}")

    while True:
        ret, frame = cap.read()
        if not ret:
            print("[Producer] Đã đọc hết video.")
            break

        _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        frame_b64 = base64.b64encode(buffer).decode("utf-8")

        message = {
            "video_name": video_name,
            "frame_id":   frame_id,
            "timestamp":  datetime.now().isoformat(),
            "width":      frame.shape[1],
            "height":     frame.shape[0],
            "frame_data": frame_b64,
        }

        producer.send(TOPIC, json.dumps(message).encode("utf-8"))
        print(f"[Producer] Đã gửi frame {frame_id}")

        frame_id += 1
        time.sleep(interval)

    producer.flush()
    cap.release()
    print(f"[Producer] Hoàn thành. Tổng frame đã gửi: {frame_id}")

if __name__ == "__main__":
    main()
