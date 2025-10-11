FROM python:3.11

RUN pip install kafka-python

COPY init_kafka.py /scripts/init_kafka.py



CMD ["python3", "/scripts/init_kafka.py"]
