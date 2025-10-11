# 📌 Use official lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy your main code
COPY main.py .

# Install dependencies in one layer (faster + cleaner)
RUN pip install --no-cache-dir fastapi "elasticsearch>=8.0.0,<9.0.0" uvicorn[standard] python-multipart requests


# Expose Fastmain port
EXPOSE 8000

# Launch Fastmain server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
