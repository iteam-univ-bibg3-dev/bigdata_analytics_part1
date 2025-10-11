FROM python:3.11

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all producer scripts
COPY . /app

CMD ["python3", "nature_consumer.py"]
