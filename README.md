# People Counting System

Hệ thống đếm số người trong camera sử dụng Kafka + YOLOv8 + MongoDB.

## Kiến trúc

```
[Video/Camera]
     │
     ▼
┌──────────────────┐      Kafka topic        ┌────────────────────┐
│ frame_acquisition│  ──► "raw_frames"  ──►  │ object_detection   │
│  (producer.py)   │                         │  (detector.py)     │
└──────────────────┘                         └────────────────────┘
                                                       │
                                            Kafka topic│
                                        "detection_results"
                                                       │
                                                       ▼
                                             ┌─────────────────┐
                                             │    storage       │
                                             │  (consumer.py)  │
                                             │   → MongoDB     │
                                             └─────────────────┘
```

**Big Data context:** Apache Kafka làm distributed message broker, cho phép scale nhiều instance xử lý song song, tách biệt hoàn toàn 3 tầng.

## Cấu trúc thư mục

```
.
├── docker-compose.yml
├── .env                    # credentials (không commit)
├── .env.example            # mẫu để tham khảo
├── .gitignore
├── frame_acquisition/
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── object_detection/
│   ├── detector.py
│   ├── Dockerfile
│   └── requirements.txt
├── storage/
│   ├── consumer.py
│   ├── mongo_init.js
│   ├── Dockerfile
│   └── requirements.txt
└── videos/
    └── video1.mp4          # đặt video vào đây
```

## Cách chạy

**1. Chuẩn bị**
```bash
cp .env.example .env
# Sửa MONGO_USER và MONGO_PASSWORD trong .env
```

**2. Đặt video vào thư mục videos/**
```bash
cp /path/to/your/video.mp4 videos/video1.mp4
```

**3. Khởi động toàn bộ hệ thống**
```bash
docker compose up --build
```

**4. Xem kết quả trong MongoDB**
```bash
docker exec -it mongodb mongosh \
  -u admin -p password123 \
  --authenticationDatabase admin \
  people_counter \
  --eval "db.detection_results.find({}, {annotated_frame:0}).limit(5).pretty()"
```

## Kết quả lưu trong MongoDB

Mỗi document trong collection `detection_results`:

```json
{
  "video_name": "video1.mp4",
  "frame_id": 42,
  "timestamp": "2026-06-17T10:00:00.123",
  "people_count": 3,
  "bounding_boxes": [[120, 80, 200, 300], [340, 90, 420, 310], [500, 100, 580, 320]]
}
```

## Biến môi trường

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Địa chỉ Kafka broker |
| `VIDEO_PATH` | `video1.mp4` | Đường dẫn video đầu vào |
| `MONGO_URI` | `mongodb://localhost:27017` | Connection string MongoDB |
| `MONGO_USER` | — | Username MongoDB (trong .env) |
| `MONGO_PASSWORD` | — | Password MongoDB (trong .env) |
