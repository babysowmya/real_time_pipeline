# Real-Time User Activity Streaming Pipeline

## 📌 Project Overview

This project is a **real-time data streaming pipeline** that processes user activity events.

It uses:

* Apache Kafka – for event streaming
* Apache Spark (Structured Streaming) – for real-time processing
* PostgreSQL – for storing aggregated results
* Docker & Docker Compose – for container orchestration

The pipeline reads user activity events from Kafka, performs window-based aggregation, and stores the results in PostgreSQL.

---

## 🏗 Architecture

```
Kafka (user_activity topic)
        ↓
Spark Structured Streaming
        ↓
Window Aggregation (1-minute window)
        ↓
PostgreSQL (page_view_counts table)
```

---

## 📂 Project Structure

```
real_time_pipeline/
│
├── docker-compose.yml
├── .env
├── init-db.sql
│
└── spark/
    ├── Dockerfile
    └── app/
        └── streaming_app.py
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone <your-repo-url>
cd real_time_pipeline
```

---

### 2️⃣ Create `.env` File

Create a `.env` file in the root directory:

```
DB_USER=user
DB_PASSWORD=password
DB_NAME=stream_data
```

---

### 3️⃣ Start All Services

```bash
docker-compose up -d --build
```

This will start:

* Zookeeper
* Kafka
* PostgreSQL
* Spark Streaming Application

---

## 📥 Produce Sample Data to Kafka

Run the following command:

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic user_activity
```

Then enter JSON messages:

```json
{"event_time":"2026-02-25T22:10:00","user_id":"u1","page_url":"/home","event_type":"page_view"}
```

Press `Ctrl + C` to exit.

---

## 📊 Check Data in PostgreSQL

Connect to database:

```bash
docker exec -it db psql -U user -d stream_data
```

Run:

```sql
SELECT * FROM page_view_counts ORDER BY window_start;
```

You should see aggregated results like:

```
window_start       | window_end         | page_url | view_count
---------------------------------------------------------------
2026-02-25 22:10   | 2026-02-25 22:11   | /home    | 1
```

---

## 🧠 How It Works

1. User activity events are pushed into Kafka.
2. Spark reads the Kafka stream in real time.
3. Events are grouped into 1-minute time windows.
4. Page view counts are calculated per page.
5. Aggregated results are written into PostgreSQL.

---

## 🛑 Stop the Pipeline

```bash
docker-compose down
```

To remove volumes:

```bash
docker-compose down -v
```

## 🏆 Outcome

This project demonstrates a complete **real-time data engineering pipeline** using Kafka, Spark Structured Streaming, and PostgreSQL inside Docker.

## Author 
Sunkara Sowmya

