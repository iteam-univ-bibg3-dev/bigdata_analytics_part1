FROM python:3.10-slim

# Install dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all producer scripts
COPY . /app

# Entry script will decide which producer to run
CMD ["python3", "nature_producer.py"]
