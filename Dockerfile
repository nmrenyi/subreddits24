FROM python:3.11-slim

WORKDIR /

COPY preprocess_data.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "preprocess_data.py"]
