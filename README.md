# 🔐 Real-Time Cybersecurity Login Monitoring

A real-time cybersecurity monitoring system for detecting suspicious login activities using **Apache Kafka, Apache Spark Structured Streaming, HDFS, Hive, and HBase**.

## 📌 Project Overview

Modern applications continuously generate authentication events such as successful logins, failed logins, MFA failures, password resets, and account locks. This project builds a streaming pipeline that ingests these events, processes them in near real time, detects suspicious patterns, and stores security data for both real-time monitoring and historical analytics.

### Objectives

- Stream authentication events through Apache Kafka
- Process events using Spark Structured Streaming
- Detect brute-force login attempts
- Detect repeated MFA failures
- Detect suspicious changes in login location
- Handle duplicate events using event IDs
- Use event-time windows and watermarks
- Store raw, processed, and alert data in HDFS
- Analyze structured data using Hive
- Store real-time security alerts in HBase
- Demonstrate Kafka consumer groups and offsets
- Demonstrate Kafka replication, leader election, and broker failure recovery

---

## 🏗️ System Architecture

```text
                    ┌──────────────────────┐
                    │ Scala Event Generator│
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Kafka Producer    │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │ Apache Kafka                   │
              │ Topic: cybersecurity-logins    │
              │ Partitions: 3                  │
              └───────────────┬────────────────┘
                              │
                              ▼
              ┌────────────────────────────────┐
              │ Spark Structured Streaming     │
              │                                │
              │ • Parse events                 │
              │ • Clean/filter                 │
              │ • Deduplicate                  │
              │ • Event-time processing        │
              │ • Watermarking                 │
              │ • Window-based detection       │
              └───────────────┬────────────────┘
                              │
                 ┌────────────┼─────────────┐
                 ▼            ▼             ▼
          ┌────────────┐ ┌───────────┐ ┌───────────┐
          │    HDFS    │ │   Hive    │ │   HBase   │
          │            │ │           │ │           │
          │ Raw        │ │ Analytics │ │ Real-time │
          │ Processed  │ │ Tables    │ │ Alerts    │
          │ Alerts     │ │ Partitions│ │           │
          └────────────┘ └───────────┘ └───────────┘
```

---

## 🛠️ Technologies Used

| Technology | Version | Purpose |
|---|---:|---|
| Scala | 2.12.18 | Event generation and Spark application |
| Apache Kafka | 4.x image | Real-time event streaming |
| Apache Spark | 3.5.3 | Structured streaming and detection |
| Hadoop HDFS | 3.3.6 | Distributed storage |
| Apache Hive | 3.1.2 | SQL analytics and partitioning |
| Apache HBase | 2.5.8 | Real-time alert storage |
| Apache YARN | 3.3.6 | Resource management |
| Apache ZooKeeper | 3.x | HBase coordination |
| Docker | 29.6.2 | Kafka deployment |
| SBT | 2.0.7 | Scala build |
| Ubuntu / WSL2 | 22.04.5 | Development environment |

---

## 📂 Repository Structure

The repository is organized into source code, sample data, and verified execution evidence.

```text
real-time-cybersecurity-login-monitoring/
│
├── .gitignore
├── README.md
│
├── data/
│   ├── login_events.json
│   ├── processed_login_events.json
│   └── security_alerts.json
│
├── producer/
│   ├── build.sbt
│   ├── project/
│   │   └── build.properties
│   └── src/
│       └── main/
│           └── scala/
│               └── LoginEventProducer.scala
│
├── spark/
│   ├── build.sbt
│   ├── project/
│   │   └── build.properties
│   └── src/
│       └── main/
│           └── scala/
│               └── CybersecurityLoginStreaming.scala
│
└── outputs/
    ├── kafka/
    │   ├── topic-description.txt
    │   ├── consumer-group-offsets.txt
    │   ├── replication-before-failure.txt
    │   ├── replication-after-broker-failure.txt
    │   └── replication-after-recovery.txt
    │
    ├── spark/
    │   ├── brute-force-alert.txt
    │   ├── mfa-alert.txt
    │   ├── location-change-alert.txt
    │   ├── duplicate-event-test.txt
    │   └── checkpoint-offsets.txt
    │
    ├── hdfs/
    │   ├── hdfs-report.txt
    │   ├── hdfs-directories.txt
    │   └── hdfs-data-listing.txt
    │
    ├── hive/
    │   ├── database-and-tables.txt
    │   ├── partitions.txt
    │   └── partition-query-results.txt
    │
    └── hbase/
        ├── table-schema.txt
        └── security-alerts-scan.txt
```

### Directory purpose

| Directory | Purpose |
|---|---|
| `data/` | Sample raw, processed, and security-alert JSON data |
| `producer/` | Scala Kafka event generator |
| `spark/` | Spark Structured Streaming application and security rules |
| `outputs/kafka/` | Kafka topic, offset, replication, failure, and recovery evidence |
| `outputs/spark/` | Spark detection, deduplication, and checkpoint evidence |
| `outputs/hdfs/` | HDFS capacity, directory, and data-listing evidence |
| `outputs/hive/` | Hive database, partition, and query evidence |
| `outputs/hbase/` | HBase schema and security-alert scan evidence |

Runtime Spark checkpoints, SBT build directories, logs, temporary files, and the local Hive Derby metastore are intentionally excluded from Git.

---

## 📋 Verified Execution Evidence

The `outputs/` directory contains the execution evidence collected while testing the project.

### Kafka
- 3-partition `cybersecurity-logins` topic description
- Consumer-group offsets and lag
- 3-broker replication test with replication factor 3
- ISR state before broker failure
- ISR and leader state after broker failure
- ISR state after broker recovery

### Spark Structured Streaming
- Brute-force detection: 3 failed logins within 1 minute
- MFA attack detection: 2 MFA failures within 1 minute
- Suspicious location-change detection
- Duplicate-event removal using `event_id`
- Streaming checkpoint evidence

### HDFS
- NameNode/DataNode capacity report
- `/cybersecurity/raw`
- `/cybersecurity/processed`
- `/cybersecurity/alerts`
- Stored raw, processed, and alert files

### Hive
- `cybersecurity` database
- External processed-login-events table
- Dynamic partitioning by `event_type`
- Verified partitions: `LOGIN_FAILED`, `LOGIN_SUCCESS`, and `MFA_FAILED`
- Aggregation results: 3 failed, 2 successful, and 2 MFA-failed events

### HBase
- Enabled `security_alerts` table
- `details` column family
- 3 verified security-alert rows
- Brute-force, suspicious-location, and MFA alert records

These files provide evidence for the major project demonstrations without committing runtime state such as Spark checkpoints or the local Hive metastore.


---

## 📡 Login Event Format

Authentication events are represented as JSON.

Example:

```json
{
  "event_id": "login001",
  "user_id": "user101",
  "location": "Hyderabad",
  "event_type": "LOGIN_FAILED",
  "timestamp": "2026-09-18T02:50:00",
  "status": "SUSPICIOUS"
}
```

### Supported event types

```text
LOGIN_SUCCESS
LOGIN_FAILED
PASSWORD_RESET
MFA_FAILED
ACCOUNT_LOCKED
```

### Main fields

| Field | Description |
|---|---|
| `event_id` | Unique identifier used for deduplication |
| `user_id` | User associated with the event |
| `location` | Login location |
| `event_type` | Authentication event type |
| `timestamp` | Event-time timestamp |
| `status` | Event status |

---

## 🚨 Security Detection Rules

### 1. Brute-Force Detection

A possible brute-force attack is detected when a user has **3 or more LOGIN_FAILED events within a 1-minute window**.

Example:

```text
user101 → Hyderabad → LOGIN_FAILED
user101 → Hyderabad → LOGIN_FAILED
user101 → Hyderabad → LOGIN_FAILED
```

Generated alert:

```text
Alert Type    : BRUTE_FORCE_SUSPECTED
Severity      : HIGH
Attempt Count : 3
```

### 2. MFA Attack Detection

A possible MFA attack is detected when a user has **2 or more MFA_FAILED events within a 1-minute window**.

Generated alert:

```text
Alert Type : MFA_ATTACK_SUSPECTED
Severity   : HIGH
```

### 3. Suspicious Location Change

A suspicious location change is detected when a failed login is followed by a successful login from a different location within the monitored window.

Example:

```text
user101 → Hyderabad → LOGIN_FAILED
user101 → USA       → LOGIN_SUCCESS
```

Generated alert:

```text
Alert Type : SUSPICIOUS_LOCATION_CHANGE
Severity   : MEDIUM
```

---

## ⏱️ Spark Streaming Features

### Event-Time Processing

The Spark application uses the event timestamp for time-based analysis.

### Windowing

Security rules use a **1-minute event-time window**.

### Watermarking

A **30-second watermark** is configured to handle late-arriving events and finalize completed event-time windows.

### Deduplication

Events are deduplicated using `event_id`. If the same event is received more than once, Spark treats it as a single logical event.

### Checkpointing

Spark Structured Streaming uses a checkpoint directory to maintain streaming progress and state across batches.

The checkpoint directory is intentionally excluded from Git.

---

## 📨 Kafka

### Main Kafka Topic

```text
cybersecurity-logins
```

Main project configuration:

```text
Partitions           : 3
Replication Factor   : 1
Min ISR               : 1
```

### Consumer Group

```text
cybersecurity-monitor-group
```

Consumer-group testing demonstrated:

- Partition offsets
- Current offsets
- Log-end offsets
- Consumer lag
- Message consumption

---

## 🔁 Kafka Replication & Fault Tolerance

A separate three-broker Kafka KRaft environment was created for replication testing.

Configuration:

```text
Brokers              : 3
Replication Factor   : 3
Min ISR               : 2
```

### Failure test

One broker was stopped during testing.

Observed behavior:

1. The failed broker left the ISR.
2. ISR reduced from 3 brokers to 2.
3. A new leader was elected for the affected partition where required.
4. Messages remained available while the configured minimum ISR was maintained.
5. The stopped broker was restarted.
6. The recovered broker rejoined the ISR.

This demonstrates Kafka replication and broker failure recovery.

---

## 🗄️ HDFS Storage

The following HDFS directories are used:

```text
/cybersecurity/raw
/cybersecurity/processed
/cybersecurity/alerts
```

### Storage flow

```text
Login Events
     │
     ├──► /cybersecurity/raw
     │
     ├──► /cybersecurity/processed
     │
     └──► /cybersecurity/alerts
```

The HDFS environment was validated with a live DataNode and NameNode, and the cybersecurity directories were created successfully.

---

## 🐝 Hive Analytics

Hive database:

```text
cybersecurity
```

The project includes an external JSON-based login-events table and a partitioned managed table.

### Partitioning

The login events are partitioned by:

```text
event_type
```

Partitions demonstrated:

```text
event_type=LOGIN_FAILED
event_type=LOGIN_SUCCESS
event_type=MFA_FAILED
```

Dynamic partition loading was tested successfully, followed by partition-specific queries.

---

## 🧱 HBase Real-Time Alerts

HBase table:

```text
security_alerts
```

Column family:

```text
details
```

Example alert data stored in HBase includes:

| Field | Example |
|---|---|
| User ID | user101 |
| Alert Type | BRUTE_FORCE_SUSPECTED |
| Severity | HIGH |
| Attempt Count | 3 |
| Reason | 3 or more failed login attempts within 1 minute |

HBase is used for real-time alert-oriented storage, while Hive is used for structured analytical queries.

---

## 🧪 Testing & Validation

### Duplicate Event Test

A duplicate event with the same `event_id` was submitted. Spark deduplication prevented it from being processed as a second logical event.

### Brute-Force Test

Three failed login events for `final_user` were submitted within one minute.

Observed alert:

```text
BRUTE_FORCE_SUSPECTED
Severity      : HIGH
Attempt Count : 3
```

### MFA Test

Two MFA failure events were submitted within one minute.

Observed alert:

```text
MFA_ATTACK_SUSPECTED
Severity : HIGH
```

### Location Change Test

A failed login from Hyderabad followed by a successful login from the USA was tested.

Observed alert:

```text
SUSPICIOUS_LOCATION_CHANGE
Severity : MEDIUM
```

### End-to-End Streaming Test

The final controlled test confirmed the complete path:

```text
Login Event
     ↓
Kafka
     ↓
Spark Structured Streaming
     ↓
Event-Time Window
     ↓
Watermark Advancement
     ↓
Brute-Force Detection
     ↓
Security Alert
```

The Spark output confirmed a `BRUTE_FORCE_SUSPECTED` alert for `final_user` with an attempt count of 3.

---

## ▶️ Running the Project

### 1. Start Kafka

Check that the Kafka container is running:

```bash
docker ps
```

The main Kafka container used during testing is:

```text
banking-kafka
```

### 2. Run the Scala Producer

```bash
cd producer
sbt run
```

The producer sends authentication events to the `cybersecurity-logins` topic.

### 3. Run Spark Streaming

In another terminal:

```bash
cd spark
sbt compile
sbt run
```

### 4. Send a Test Event

```bash
echo '{"event_id":"test001","user_id":"user101","location":"Hyderabad","event_type":"LOGIN_FAILED","timestamp":"2026-09-18T02:50:00","status":"SUSPICIOUS"}' | docker exec -i banking-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cybersecurity-logins
```

### 5. Consume Kafka Events

```bash
docker exec -it banking-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cybersecurity-logins --group cybersecurity-monitor-group
```

---

## 📊 Example Alert Output

```text
Batch: 15

window_start       : 2026-09-18 02:50:00
window_end         : 2026-09-18 02:51:00
user_id            : final_user
attempt_count      : 3
alert_type         : BRUTE_FORCE_SUSPECTED
severity            : HIGH
reason              : 3 or more failed login attempts within 1 minute
```

---

## 🔄 Complete Data Flow

```text
                Authentication Event
                         │
                         ▼
                  Scala Producer
                         │
                         ▼
                       Kafka
                         │
                         ▼
           Spark Structured Streaming
                         │
             ┌───────────┼───────────┐
             │           │           │
          Parse      Deduplicate   Validate
             │           │           │
             └───────────┼───────────┘
                         ▼
                Event-Time Window
                         │
                         ▼
                    Watermark
                         │
                         ▼
                 Security Rules
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
             HDFS       Hive       HBase
              │          │          │
             Raw      Analytics   Alerts
          Processed    Queries
           Alerts     Partitions
```

---

## 📈 Key Project Demonstrations

The project demonstrates the following data-engineering and streaming concepts:

- Kafka topics and partitions
- Kafka consumer groups
- Kafka offsets and lag
- Kafka replication
- Kafka ISR
- Kafka leader election
- Broker failure recovery
- Spark Structured Streaming
- Event-time processing
- Window-based detection
- Watermarking
- Stateful streaming
- Event deduplication
- HDFS storage
- Hive external and managed tables
- Hive dynamic partitioning
- HBase column families and alert storage
- YARN-based Hadoop environment
- Docker-based Kafka deployment
- Scala and SBT project structure

---

## 🎯 Project Outcome

This project provides an end-to-end demonstration of a real-time cybersecurity data pipeline:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Security Detection
  ↓
HDFS + Hive + HBase
```

The implemented tests demonstrate duplicate-event handling, brute-force detection, MFA attack detection, suspicious location-change detection, Kafka consumer-group offset tracking, and Kafka broker failure recovery.

---

## 👨‍💻 Author

**Marthati Vignesh**

Real-Time Cybersecurity Login Monitoring  
Data Engineering / Cybersecurity Streaming Project
