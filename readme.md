🏥 Real-Time Healthcare Monitoring System
A comprehensive real-time patient monitoring system that combines medical data processing with big data technologies. This system simulates patient vital signs, processes them in real-time using Apache Spark and machine learning, and provides interactive dashboards for healthcare professionals.

📋 Table of Contents
Overview

Architecture

Technologies Used

Project Structure

Quick Start

Detailed Setup Guide

Usage

API Endpoints

Troubleshooting

Future Enhancements

🌟 Overview
This project demonstrates a real-time healthcare monitoring pipeline that:

Simulates patient vital signs (heart rate, blood pressure, oxygen saturation, etc.)

Processes data in real-time using Apache Kafka and Spark Streaming

Applies ML models to predict patient risk levels

Stores data in Elasticsearch for efficient querying

Visualizes data through Kibana and Streamlit dashboards

Generates alerts for critical medical conditions

🏗️ Architecture



🛠️ Technologies Used
Technology	Purpose	Version
Apache Kafka	Real-time data streaming	3.4+
Apache Spark	Stream processing & ML	3.5.0
Elasticsearch	Data storage & search	8.9.0
Kibana	Data visualization	8.9.0
Streamlit	Interactive dashboard	1.28.0
Docker	Containerization	20.10+
Python	Data simulation & ML	3.8+
Scikit-learn	Machine learning	1.3.0
📁 Project Structure
text
healthcare-monitoring/
├── scripts/
│   ├── patient_simulator.py
│   ├── kafka_to_elasticsearch.py
│   ├── spark_streaming_ml.py
│   ├── spark_simplified.py
│   ├── analyzed_data_consumer.py
│   ├── train_ml_model.py
│   ├── streamlit_dashboard.py
│   └── check_elasticsearch.py
├── models/
│   └── patient_risk_model.pkl
├── config/
├── data/
└── docker-compose.yml
🚀 Quick Start
Prerequisites
Docker and Docker Compose

Python 3.8+

8GB RAM minimum

1. Clone and Setup
bash
git clone <your-repo-url>
cd healthcare-monitoring
1. Start Core Services
bash
docker-compose up -d
1. Install Python Dependencies
bash
pip install -r requirements.txt
1. Train ML Model
bash
python scripts/train_ml_model.py
1. Start the Data Pipeline
Terminal 1 - Data Simulation:

bash
python scripts/patient_simulator.py
Terminal 2 - Basic Data Pipeline:

bash
python scripts/kafka_to_elasticsearch.py
Terminal 3 - Spark Streaming:

bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 scripts/spark_simplified.py
Terminal 4 - Dashboard:

bash
streamlit run scripts/streamlit_dashboard.py
📊 Features
Real-time Patient Monitoring
Continuous vital signs simulation

Real-time data processing pipeline

ML-powered risk prediction

Critical condition alerting

Interactive Dashboards
Kibana: Clinical overview and analytics

Streamlit: Interactive patient monitoring

Real-time risk scoring and visualization

Machine Learning
Pretrained Random Forest model for risk prediction

Real-time inference on streaming data

Rule-based and ML-based risk assessment

🎯 Key Components
Data Simulation
Generates realistic patient vital signs

Includes abnormal values for alert testing

Configurable patient count and frequency

Stream Processing
Apache Kafka for data ingestion

Spark Streaming for real-time processing

Elasticsearch for storage and search

Alert System
Detects critical medical conditions

Real-time alert generation

Multi-level risk assessment

🔧 Configuration
Kafka Topics
raw-vitals: Raw patient data

analyzed-vitals: Spark-processed data

patient-alerts: Critical condition alerts

Elasticsearch Indices
patient-vitals: Raw patient measurements

patient-alerts: Generated alerts

spark-analyzed-data: ML analysis results

🚨 Alert Conditions
Heart Rate: < 50 or > 120 bpm

Oxygen Saturation: < 90%

Blood Pressure:

Systolic: < 90 or > 180 mmHg

Diastolic: < 60 or > 120 mmHg

📈 Risk Levels
Level	Description	Color
0	Normal	Green
1	Moderate	Yellow
2	High	Orange
3	Critical	Red
🌐 Access Points
Kibana Dashboard: http://localhost:5601

Streamlit Dashboard: http://localhost:8501

Spark Master: http://localhost:8080

Elasticsearch: http://localhost:9200

🐛 Troubleshooting
Common Issues
Memory Constraints

bash
# Reduce resource limits in docker-compose.yml
deploy:
  resources:
    limits:
      memory: 512M
Kafka Connection Issues

bash
# Check if Kafka is running
docker-compose ps kafka

# Create topics manually
docker-compose exec kafka kafka-topics --create --topic raw-vitals --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Elasticsearch Index Errors

bash
# Check cluster health
curl http://localhost:9200/_cluster/health
🚀 Future Enhancements
Online ML model retraining

HDFS integration for long-term storage

Advanced anomaly detection

Predictive analytics

Mobile alert notifications

📝 License
This project is for educational purposes. Ensure compliance with healthcare regulations when using with real patient data.

🤝 Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

⭐ If you find this project useful, please give it a star!

Built with ❤️ for Healthcare Innovation