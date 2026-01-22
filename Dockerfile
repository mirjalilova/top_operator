FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

RUN apt-get update && \
    apt-get install -y python3.10 python3.10-distutils python3.10-venv python3-pip \
    libavformat-dev libavcodec-dev libavdevice-dev \
    libavfilter-dev libavutil-dev libswscale-dev libswresample-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3.10 /usr/bin/python3

WORKDIR /app
COPY requirements.txt .

RUN pip3 install --upgrade pip

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
