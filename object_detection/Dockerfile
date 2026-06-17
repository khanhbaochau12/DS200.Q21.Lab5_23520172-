FROM python:3.10-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libgl1 libglib2.0-0 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir \
    torch==2.1.0+cpu torchvision==0.16.0+cpu \
    --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    ultralytics==8.2.0 \
    opencv-python-headless==4.9.0.80 \
    numpy==1.26.4

COPY detector.py .
CMD ["python", "detector.py"]
