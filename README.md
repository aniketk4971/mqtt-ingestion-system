# High-Frequency MQTT Data Ingestion System

## Project Overview

This project implements a high-frequency data ingestion pipeline using MQTT, Django, and MongoDB. The system is designed to efficiently handle continuous incoming sensor data using a queue-based architecture with optimized batch insertion into MongoDB.

The main objective of the project is to demonstrate scalable backend system design concepts such as:

* Producer-Consumer architecture
* MQTT-based asynchronous communication
* Batch processing optimization
* Concurrent message handling
* Reliable logging and monitoring
* Graceful shutdown handling
* Data loss minimization under heavy load

---

# System Architecture

## Data Flow

```
Publisher Service
       ↓
Mosquitto MQTT Broker
       ↓
Django MQTT Consumer
       ↓
Thread-Safe In-Memory Buffer
       ↓
Batch Insert Worker Thread
       ↓
MongoDB
```

---

# Technologies Used

| Component            | Technology            |
| -------------------- | --------------------- |
| Programming Language | Python                |
| Backend Framework    | Django                |
| Messaging Protocol   | MQTT                  |
| MQTT Broker          | Mosquitto             |
| MQTT Client          | Paho MQTT             |
| Database             | MongoDB               |
| Containerization     | Docker                |
| Logging              | Python Logging Module |

---

# Key Features

## 1. MQTT Publisher Service

A Python-based publisher continuously generates and publishes sensor data to the MQTT topic.

### Features

* Continuous high-frequency publishing
* MQTT QoS 1 support
* Publish failure tracking
* Real-time logging
* Automatic connection monitoring

---

## 2. Django MQTT Consumer

The Django consumer subscribes to the MQTT topic and continuously receives incoming sensor data.

### Features

* Asynchronous MQTT message consumption
* Thread-safe buffering
* High-frequency data handling
* Real-time monitoring
* Separate database insertion thread

---

## 3. Batch Processing Optimization

Instead of inserting each message individually into MongoDB, the system performs bulk insert operations using MongoDB `insert_many()`.

### Optimization Techniques

* Size-based batch flushing
* Time-based batch flushing
* MongoDB bulk insert optimization
* `ordered=False` for improved insertion throughput

### Batch Flush Conditions

The system flushes buffered data when:

* Buffer size reaches 1000 messages
* OR 10 seconds have elapsed

This ensures:

* Better throughput during heavy traffic
* Reduced database round trips
* No stale messages during low traffic

---

# Reliability Features

## MQTT QoS 1

The system uses MQTT QoS 1 to provide at-least-once delivery semantics.

This improves reliability by ensuring that messages are acknowledged and retried if necessary.

---

## Thread-Safe Buffering

A shared in-memory buffer is protected using Python threading locks to prevent race conditions between:

* MQTT consumer thread
* MongoDB batch insertion thread

---

## Graceful Shutdown Handling

The system flushes all remaining buffered messages before shutdown.

This minimizes message loss during:

* Application termination
* Keyboard interrupt
* Docker stop events

---

## Logging and Monitoring

The system maintains separate log files for:

| Log File      | Purpose                         |
| ------------- | ------------------------------- |
| publisher.log | Publisher activity and failures |
| consumer.log  | MQTT consumer events            |
| mongodb.log   | MongoDB insertion activity      |

Logs are written both to:

* Terminal output
* Persistent log files

---

# Concurrency Model

The system uses a multi-threaded architecture.

## Thread 1 — MQTT Consumer Thread

Responsibilities:

* Receive MQTT messages
* Parse payloads
* Push messages into the shared buffer

## Thread 2 — Batch Insert Worker Thread

Responsibilities:

* Monitor buffer state
* Create batches
* Insert batches into MongoDB

This separation prevents database operations from blocking high-frequency message ingestion.

---

# MongoDB Data Structure

Each sensor message is stored as a MongoDB document.

Example:

```json
{
  "device_id": 12,
  "temperature": 34,
  "timestamp": 1747698921.12
}
```

---

# Data Loss Analysis

The system tracks:

* Published message count
* Received message count
* Inserted message count
* Failed message count

These metrics help analyze:

* MQTT delivery loss
* MongoDB insertion failures
* Processing lag
* Overall reliability

## Observations

During testing, the system demonstrated:

* Stable high-frequency ingestion
* Successful batch processing
* Minimal observable data loss
* Reliable graceful shutdown behavior

Small differences between received and inserted counts represent temporary buffer lag rather than actual packet loss.

---

# Performance Optimization

## Why Batch Inserts?

Inserting records one-by-one creates:

* Excessive database round trips
* Increased network overhead
* Reduced throughput

Using MongoDB `insert_many()` significantly improves performance by:

* Reducing database calls
* Reducing network overhead
* Improving insertion throughput

---

# Backpressure Handling

The system architecture supports backpressure handling using:

* Buffered ingestion
* Batch processing
* Separate insertion thread

This prevents MongoDB insertion operations from blocking MQTT consumption.

---

# Project Structure

```text
├── backend/
│   ├── ingestion/
│   │   ├── management/
│   │   │   └── commands/
│   │   │       └── mqtt_consumer.py
│   │   ├── mongo_service.py
│   │   └── ...
│   ├── manage.py
│   └── ...
│
├── publisher/
│   └── publisher.py
│
├── .env.example
├── requirements.txt
├── .gitignore
├── docker-compose.yml
│
└── README.md
```

Log files are generated dynamically during runtime and are excluded from version control using `.gitignore`.

---

# Setup Instructions

## 1. Clone Repository

```
git clone https://github.com/aniketk4971/mqtt-ingestion-system
cd mqtt-ingestion-system
```

---

## 2. Create Virtual Environment

```
python -m venv venv
```

### Activate Virtual Environment

#### Windows

```
venv\Scripts\activate
```

#### Linux / MacOS

```
source venv/bin/activate
```

---

## 3. Install Dependencies

```
pip install -r requirements.txt
```

---

## 4. Environment Variables

Create a `.env` file in the project root using the values from `.env.example`.

Example:

```env
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_TOPIC=sensor/data
MQTT_QOS=1

MONGO_URI=mongodb://localhost:27017
MONGO_DB=mqtt_system
MONGO_COLLECTION=sensor_data

BATCH_SIZE=1000
BATCH_TIMEOUT=10

MAX_BUFFER_SIZE=5000
```

The project uses environment variables for configurable and production-ready setup.

---

## 5. Start Docker Containers

```
docker compose up -d
```

This starts:

* Mosquitto MQTT Broker
* MongoDB

---

## 6. Start Django Consumer

```
cd backend
python manage.py mqtt_consumer
```

---

## 7. Start Publisher Service

Open a new terminal:

```
cd publisher
python publisher.py
```

---

# Verifying MongoDB Data

Open Mongo shell:

```
docker exec -it mongodb mongosh
```

Select database:

```
use mqtt_system
```

Check document count:

```
db.sensor_data.countDocuments()
```

---

# Sample Logs

## Publisher Log

```
2026-05-19 23:33:10,769 - Published: 200
2026-05-19 23:33:12,394 - Published: 300
```

---

## Consumer Log

```
2026-05-19 23:35:13,611 - Received: 500
2026-05-19 23:35:15,279 - Received: 600
```

---

## MongoDB Log

```
2026-05-19 23:36:16,296 - Inserted Batch | Batch Size: 618 | Total Inserted: 4331
2026-05-19 23:36:26,378 - Inserted Batch | Batch Size: 609 | Total Inserted: 4940
```

---

# Future Improvements

Potential future enhancements include:

* Kafka integration
* WebSocket live monitoring dashboard
* Persistent disk-backed queues
* Dockerized Django deployment

---

# Conclusion

This project demonstrates the implementation of a scalable and reliable high-frequency data ingestion pipeline using MQTT, Django, and MongoDB.

The system successfully showcases:

* Queue-based asynchronous communication
* Batch processing optimization
* Concurrent backend architecture
* Reliability engineering concepts
* Graceful shutdown handling
* Logging and monitoring
* MongoDB bulk insertion optimization

The architecture can be extended further for real-world event streaming and telemetry systems.
